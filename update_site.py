#!/usr/bin/env python3
"""
Refresh index.html (the live Allen County shipment progress site) with
current data from archive.org.

Run manually with:

    python3 update_site.py

Normally run automatically by .github/workflows/update.yml on a schedule.

It pulls every SEARCHABLE archive.org item in collection:allen_county, groups
by shiptracking code, and additionally recovers "stub" items that archive.org
excludes from search entirely (metadata field noindex:true -- used for items
that are received/reserved but not yet fully processed, sitting at
repub_state -1/-2/etc). Those stubs are invisible to any search query, so this
script finds them a different way: for shiptracking codes whose searchable
identifiers follow a detectable "prefix + sequential number" pattern (e.g.
merwinfam04, merwinfam05, ...), it directly probes archive.org/metadata/<id>
for the full number range (including gaps and a run past the highest known
number) to recover the true total.

A shipment with NO searchable items at all -- a brand new shipment that
hasn't had anything indexed yet -- cannot be pattern-detected from nothing.
allen_county_shipments.json (the manifest) is how those get declared: give a
shipment an identifier_prefix (and, ideally, an expected_items count from the
packing list) and this script probes for it directly from day one, showing it
on the dashboard as "received, awaiting digitization" instead of leaving it
invisible until archive.org has something to show. The manifest also carries
each shipment's friendly display name (it replaces the old, separate
allen_county_shipment_names.json / allen_county_stub_seeds.json files).

A shipment's displayed "total" is max(expected_items, observed_items) when a
manifest count is declared -- never below what's actually been observed, and
never above 100% complete once observed items reach it. If observed exceeds
expected, that's surfaced as a manifest_warnings entry rather than silently
corrected forever; the manifest is meant to be updated when that happens.

A shipment counts as "active" if it has had a completion (repub_state -> 19)
or a newly-added stub item in the last 90 days, AND is not yet fully complete
(completed < total) -- OR it is a manifest-declared shipment that has had no
activity at all yet (so it doesn't just vanish while genuinely waiting to be
scanned). A shipment that reaches completed == total is recorded once into
allen_county_completed_history.json (permanent) and drops out of Active; the
site shows only the COMPLETED_DISPLAY_COUNT most recently completed
shipments, not the whole history. The run in which a shipment's final item
finishes still shows it in Active one last time, flagged "finalizing", before
it settles into history-only on the next run.

Everything else in the HTML (layout, styling) is left untouched.
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

SITE_PATH = "index.html"
MANIFEST_PATH = "allen_county_shipments.json"  # authoritative per-shipment declarations: name, identifier prefix, expected item count, received date -- see the file itself for field docs
ENUM_CACHE_PATH = "allen_county_enum_cache.json"  # persists settled stub-discovery numbers across runs -- see README.md "Enumeration cache"
COMPLETED_HISTORY_PATH = "allen_county_completed_history.json"  # persists every shipment once it hits completed==total, so the "recently completed" list survives shipments aging out of the active window
COMPLETED_DISPLAY_COUNT = 7  # only the N most recently completed shipments are ever shown on the site

# Guard rails for the write step (see sanity_check below).
MAX_SHRINK_PCT = 40                  # refuse to publish if OBSERVED items drop more than this vs the current file
MAX_DISPLAYED_SHRINK_PCT = 70        # much looser check on the manifest-inflated displayed total -- catches a gross manifest typo without tripping on a legitimate recount
MAX_UNRESOLVED_PROBES = 10           # refuse to publish if more than this many identifier probes failed (flat floor)
MAX_UNRESOLVED_PROBE_RATE = 0.05     # ...or more than this fraction of all probes made this run, whichever is larger
MAX_DECLARED_RANGE = 2000            # refuse to probe further than this past a manifest-declared number_start, even if expected_items claims more -- typo guard
STALE_DECLARED_DAYS = 120            # a declared shipment with zero items this long after its `received` date gets flagged (still shown -- not blocked)
DEFAULT_NUMBER_WIDTH = 2             # zero-padding width to assume for a declared shipment with nothing to detect it from and no number_width override
SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
METADATA_URL = "https://archive.org/metadata/"
QUERY = "collection:allen_county"
FIELDS = "identifier,shiptracking,repub_state,republisher_date,publicdate,imagecount"
ACTIVE_WINDOW_DAYS = 90

ID_PATTERN = re.compile(r"^([a-zA-Z]+?)(\d+)$")


def as_int(value):
    """imagecount (and a few other fields) come back as an int from the
    scrape API but as a string from the metadata API -- summing a mix of
    both with a plain sum() raises TypeError. Coerce leniently; anything
    missing or unparseable counts as 0 pages, not a crash."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


# ---------- Bulk fetch of the searchable index ----------

def fetch_scrape_page(params, retries=4):
    url = SCRAPE_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "allen-county-site-update/1.0"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"  (page fetch failed: {e} -- retrying, attempt {attempt + 2}/{retries})", flush=True)
            time.sleep(2 * (attempt + 1))


def fetch_all_items():
    """Full-collection pull, only used with --full-scan. See fetch_discovery_items
    and fetch_items_for_codes for the default, much faster path."""
    print("Fetching EVERY searchable item from archive.org (collection:allen_county) -- full scan, the slow path...")
    items = []
    cursor = None
    page = 0
    total = None
    while True:
        page += 1
        params = {"q": QUERY, "count": "10000", "fields": FIELDS}
        if cursor:
            params["cursor"] = cursor
        data = fetch_scrape_page(params)
        # Only the FIRST page reports a trustworthy total: on cursor pages
        # archive.org has been observed returning the whole-archive count
        # instead of this query's. Pin the first value, and never assume it
        # is present at all -- f"{None:,}" raises TypeError.
        if total is None:
            total = data.get("total")
        page_items = data.get("items", [])
        items.extend(page_items)
        total_str = f"{total:,}" if isinstance(total, int) else "?"
        print(f"  page {page}: {len(items):,} / {total_str}", flush=True)
        cursor = data.get("cursor")
        if not cursor or not page_items:
            break
        time.sleep(0.2)
    return items


def fetch_discovery_items(now):
    """
    Phase A of the default path: one cheap query for items whose
    republisher_date OR publicdate falls within ACTIVE_WINDOW_DAYS --
    exactly the two signals "active" is computed from elsewhere in this
    script. Returns the set of distinct group codes seen. See the module
    docstring for why this can't hide a shipment that would otherwise
    qualify -- and why manifest-declared codes are unioned in separately,
    since a brand-new shipment has no activity to be discovered by yet.
    """
    cutoff = now - timedelta(days=ACTIVE_WINDOW_DAYS)
    q = (f"{QUERY} AND (republisher_date:[{cutoff.strftime('%Y%m%d%H%M%S')} TO 99991231235959] "
         f"OR publicdate:[{cutoff.strftime('%Y-%m-%d')} TO 2099-12-31])")
    print(f"Discovering shiptracking codes with activity in the last {ACTIVE_WINDOW_DAYS} days...")
    items = []
    cursor = None
    page = 0
    while True:
        page += 1
        params = {"q": q, "count": "10000", "fields": "identifier,shiptracking"}
        if cursor:
            params["cursor"] = cursor
        data = fetch_scrape_page(params)
        page_items = data.get("items", [])
        items.extend(page_items)
        print(f"  discovery page {page}: {len(items):,} items", flush=True)
        cursor = data.get("cursor")
        if not cursor or not page_items:
            break
        time.sleep(0.2)
    codes = {it["shiptracking"] for it in items if it.get("shiptracking")}
    print(f"  found {len(codes)} recently-active shiptracking code(s)")
    return codes


def fetch_items_for_codes(codes):
    """
    Phase B of the default path: for each candidate code, pull its COMPLETE
    indexed item set with its own targeted query -- not date-restricted --
    so pattern-detection and totals are exactly as accurate as scanning the
    whole collection would produce. Codes are fetched concurrently since
    each query is independent and small.
    """
    if not codes:
        return []
    print(f"Fetching complete item history for {len(codes)} shiptracking code(s)...")

    def fetch_one(code):
        results = []
        cursor = None
        while True:
            params = {"q": f"{QUERY} AND shiptracking:{code}", "count": "1000", "fields": FIELDS}
            if cursor:
                params["cursor"] = cursor
            data = fetch_scrape_page(params)
            page_items = data.get("items", [])
            results.extend(page_items)
            cursor = data.get("cursor")
            if not cursor or not page_items:
                break
        return results

    all_items = []
    done = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_one, code): code for code in sorted(codes)}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                all_items.extend(fut.result())
            except Exception as e:
                print(f"  WARNING: could not fetch items for {code}: {e} -- this shipment may be missing this run.")
            done += 1
            if done % 10 == 0 or done == len(codes):
                print(f"  fetched {done}/{len(codes)} codes ({len(all_items):,} items so far)", flush=True)
    return all_items


def parse_republisher_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def parse_publicdate(value):
    """
    The two archive.org APIs disagree on how they format publicdate:

        search/scrape API : "2025-09-15T15:29:47Z"   (ISO-8601)
        metadata API      : "2025-09-15 15:29:47"    (space-separated)

    Handling only the second silently returned None for EVERY search result,
    which left last_added permanently dead for indexed-only groups and made
    recency depend entirely on republisher_date. Accept both.
    """
    if not isinstance(value, str) or not value:
        return None
    s = value.strip().replace("T", " ").rstrip("Z").strip()
    for fmt, width in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(s[:width], fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


# ---------- Stub discovery (noindex items invisible to search) ----------

def fetch_metadata(identifier, retries=3, timeout=15):
    url = METADATA_URL + urllib.parse.quote(identifier)
    for _ in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "allen-county-site-update/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data if data else None
        except Exception:
            continue
    return "FAIL"


def detect_padding_width(numbers_and_strings):
    smallest = min(numbers_and_strings, key=lambda t: t[0])
    return len(smallest[1])


def format_candidate_id(prefix, n, width):
    if n < 10 ** width:
        return prefix + str(n).zfill(width)
    return prefix + str(n)


def discover_width(prefix, lo, default=DEFAULT_NUMBER_WIDTH):
    """
    Probe a handful of candidate zero-padding widths at position `lo` to
    guess how a brand-new declared shipment's identifiers are numbered, when
    no items exist yet to infer it from. Falls back to `default` if none
    hit -- probing will simply come up empty until a real item appears (or
    number_width is set explicitly in the manifest).
    """
    for width in (2, 3, 4, 1):
        ident = format_candidate_id(prefix, lo, width)
        data = fetch_metadata(ident)
        if data and data != "FAIL":
            return width
    return default


def _is_settled(entry):
    """
    True for a number that will never need re-probing: a confirmed-absent
    slot (None), a real item that has finished digitization (repub_state
    "19", which doesn't regress), or a slot confirmed to belong to a
    DIFFERENT shipment ({"foreign": ...} -- see enumerate_full_shipment). A
    found-but-still-in-progress item is deliberately NOT settled -- it must
    keep being re-probed until it actually finishes, or its progress would
    freeze in the cache.
    """
    return (
        entry is None
        or (isinstance(entry, dict) and entry.get("repub_state") == "19")
        or (isinstance(entry, dict) and "foreign" in entry)
    )


def enumerate_full_shipment(prefix, known_numbers, cached_resolved=None, batch_size=15,
                            max_extra_batches=8, lo=1, min_hi=0, width=None,
                            expect_code=None, suppress_absent_upto=0):
    """
    known_numbers: {number: {"identifier", "repub_state", "publicdate", "imagecount"}}
        from THIS run's fresh search index. Empty for a manifest-declared
        shipment that hasn't had anything scanned yet.
    cached_resolved: {number: entry_or_None_or_foreign} of numbers already
        conclusively settled as of a PRIOR run (see _is_settled) -- skipped
        on reprobe.
    lo: first number to probe (manifest number_start, default 1).
    min_hi: the ceiling to probe up to even with nothing observed --
        number_start + expected_items - 1 for a declared shipment, 0
        otherwise (meaning "just whatever's been observed").
    width: zero-padding width, if already known (from the manifest).
        Detected from known_numbers when unset and something's observed, or
        discovered by probing when unset and nothing is.
    expect_code: when set, a probed identifier whose own `shiptracking`
        metadata field disagrees is treated as belonging to a DIFFERENT
        shipment (a colliding prefix guess) rather than as this one's --
        see the `foreign` return value. Only meaningful for a manifest-
        declared prefix; an auto-detected prefix was derived FROM this
        code's own items, so it can't collide with anything.
    suppress_absent_upto: numbers <= this value are NOT cached as confirmed-
        absent even when a probe finds nothing there. Used for the
        still-being-filled part of a declared range: an empty slot today
        may be scanned tomorrow, and caching it as permanently absent would
        freeze the shipment at whatever it happened to have on day one.

    Returns (found, settled, cache_hits, unresolved, foreign, total_probes):
        found        -- {number: entry} for every real item of THIS shipment
                         now known.
        settled      -- {number: entry_or_None_or_foreign}, the subset worth
                         caching for next run (see _is_settled).
        cache_hits   -- how many numbers were resolved from the cache instead
                         of a network probe, for the summary print.
        unresolved   -- probes that FAILED (network/throttling) -- not the
                         same as a number being confirmed absent.
        foreign      -- probes that hit a REAL item under a DIFFERENT
                         shiptracking code -- signals a colliding prefix guess.
        total_probes -- how many /metadata/ requests this call made, so the
                         unresolved-probe guard can scale with it.

    Walking past the highest known number to look for brand-new hidden
    items always runs here, regardless of the cache -- that check is the
    one thing this whole pipeline exists to guarantee.
    """
    cached_resolved = cached_resolved or {}
    numbers = sorted(known_numbers.keys())

    if numbers:
        observed_width = detect_padding_width(
            [(n, known_numbers[n]["identifier"][len(prefix):]) for n in numbers]
        )
        if width is None:
            width = observed_width
        hi = max(numbers[-1], min_hi)
    else:
        if width is None:
            width = discover_width(prefix, lo)
        hi = max(min_hi, lo - 1)

    found = dict(known_numbers)
    settled = {n: e for n, e in cached_resolved.items() if _is_settled(e) and n <= hi}
    for n, e in settled.items():
        if isinstance(e, dict) and "foreign" not in e and n not in found:
            found[n] = e

    to_probe = [n for n in range(lo, hi + 1) if n not in found and n not in settled]
    # Numbers resolved from the cache instead of a fresh probe, for the summary print.
    cache_hits = sum(1 for n in range(lo, hi + 1) if n not in known_numbers and n in settled)

    unresolved = 0
    foreign = 0
    total_probes = len(to_probe)

    def probe(n):
        """
        Returns (n, data, ok). ok=False means the fetch FAILED, which is NOT
        the same as the item not existing -- a missing identifier returns
        HTTP 200 with an empty body (-> None, ok=True); only a network error
        or throttled request yields "FAIL". Collapsing those two silently
        shrinks the total. `data` is a dict tagged {"foreign": <code>} when
        expect_code is set and the probed item belongs to someone else.
        """
        ident = format_candidate_id(prefix, n, width)
        data = fetch_metadata(ident)
        if data == "FAIL":
            return (n, None, False)
        if data is None:
            return (n, None, True)
        md = data.get("metadata", {})
        if expect_code is not None:
            actual = md.get("shiptracking")
            if actual and str(actual).upper() != expect_code.upper():
                return (n, {"foreign": actual}, True)
        return (n, {
            "identifier": ident,
            "repub_state": md.get("repub_state"),
            "publicdate": md.get("publicdate"),
            "imagecount": md.get("imagecount"),
        }, True)

    def handle(n, data, ok):
        nonlocal unresolved, foreign
        if not ok:
            unresolved += 1
            return False
        if data is None:
            if n > suppress_absent_upto:
                settled[n] = None
            return False
        if "foreign" in data:
            foreign += 1
            settled[n] = data
            return False
        found[n] = data
        if _is_settled(data):
            settled[n] = data
        return True

    if to_probe:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, n) for n in to_probe]
            for fut in as_completed(futures):
                n, data, ok = fut.result()
                handle(n, data, ok)

    # Extend past the highest known number in parallel batches, stop once a
    # whole batch turns up nothing of THIS shipment's. This is NEVER skipped
    # by the cache -- it's how a genuinely new hidden item beyond the known
    # ceiling gets caught.
    n = hi + 1
    for _ in range(max_extra_batches):
        batch = list(range(n, n + batch_size))
        total_probes += len(batch)
        hits = 0
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, b) for b in batch]
            for fut in as_completed(futures):
                bn, data, ok = fut.result()
                if handle(bn, data, ok):
                    hits += 1
        n += batch_size
        if hits == 0:
            break

    return found, settled, cache_hits, unresolved, foreign, total_probes


def find_enumerable_candidates(indexed_by_code, declared_codes=()):
    """
    Detect shiptracking codes whose identifiers show a clean prefix+number
    pattern. Skips codes already declared in the manifest -- those are
    enumerated directly (see the manifest-declared loop in
    build_shipments_data), trusting the operator's prefix instead of
    re-deriving one from a heuristic.
    """
    candidates = {}
    for code, entries in indexed_by_code.items():
        if code in declared_codes:
            continue
        matches = [(e, ID_PATTERN.match(e["identifier"])) for e in entries]
        good = [(e, m) for e, m in matches if m]
        if len(good) < 1 or len(good) < 0.9 * len(entries):
            continue
        prefixes = set(m.group(1).lower() for _, m in good)
        if len(prefixes) != 1:
            continue
        prefix = next(iter(prefixes))
        numbers = {}
        for e, m in good:
            numbers[int(m.group(2))] = {
                "identifier": e["identifier"],
                "repub_state": e.get("repub_state"),
                "publicdate": e.get("publicdate"),
                "imagecount": e.get("imagecount"),
            }
        lo, hi = min(numbers), max(numbers)
        span = hi - lo + 1
        ratio = span / len(numbers)
        if lo <= 5 and ratio <= 8:
            candidates[code] = (prefix, numbers)
        else:
            print(f"  NOTE: {code} looks like it might follow the pattern '{prefix}<number>' "
                  f"(seen {prefix}{lo:02d}..{prefix}{hi:02d}, {len(numbers)} item(s)) but doesn't "
                  f"meet the auto-detection safety thresholds. Add \"identifier_prefix\": \"{prefix}\" "
                  f"to its entry in {MANIFEST_PATH} to enable full stub discovery for it.")
    return candidates


def _load_json_map(path, value_type, label):
    """
    Load a {key: value} JSON map, skipping "_"-prefixed keys.

    The shipped example files carry their documentation in "_comment"/
    "_example" keys. Without this skip, copying an example file as-is
    crashes on the first run with "TypeError: string indices must be
    integers", and "_example" would otherwise be enumerated as if it were a
    real shipment.
    """
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"  WARNING: {path} is not valid JSON ({e}) -- ignoring it.")
        return {}
    if not isinstance(raw, dict):
        print(f"  WARNING: {path} should contain a JSON object -- ignoring it.")
        return {}
    out = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        if not isinstance(v, value_type):
            print(f"  WARNING: ignoring {label} entry '{k}' -- unexpected format.")
            continue
        out[k] = v
    return out


def load_manifest():
    """
    {code: {"name", "identifier_prefix", "number_width", "number_start",
    "expected_items", "received", "closed"}} -- see allen_county_shipments.json
    for the field meanings. Every field but the key is optional. An invalid
    individual field is dropped with a warning; the rest of the entry
    survives (this is what lets a plain {"name": "..."} entry behave exactly
    like the old names-only file).
    """
    raw = _load_json_map(MANIFEST_PATH, dict, "shipment")
    manifest = {}
    for code, entry in raw.items():
        clean = {"closed": bool(entry.get("closed", False))}

        name = entry.get("name")
        if name is not None:
            if isinstance(name, str):
                clean["name"] = name
            else:
                print(f"  WARNING: {MANIFEST_PATH}: '{code}'.name is not a string -- ignoring it.")

        prefix = entry.get("identifier_prefix")
        if prefix is not None:
            if isinstance(prefix, str) and re.match(r"^[a-zA-Z][a-zA-Z0-9_.-]*$", prefix):
                clean["identifier_prefix"] = prefix
            else:
                print(f"  WARNING: {MANIFEST_PATH}: '{code}'.identifier_prefix is invalid -- ignoring it.")

        width = entry.get("number_width")
        if width is not None:
            if isinstance(width, int) and width > 0:
                clean["number_width"] = width
            else:
                print(f"  WARNING: {MANIFEST_PATH}: '{code}'.number_width is invalid -- ignoring it.")

        start = entry.get("number_start", 1)
        if isinstance(start, int) and start > 0:
            clean["number_start"] = start
        else:
            print(f"  WARNING: {MANIFEST_PATH}: '{code}'.number_start is invalid -- defaulting to 1.")
            clean["number_start"] = 1

        expected = entry.get("expected_items")
        if expected is not None:
            if isinstance(expected, int) and expected > 0:
                clean["expected_items"] = expected
            else:
                print(f"  WARNING: {MANIFEST_PATH}: '{code}'.expected_items is invalid -- ignoring it.")

        received = entry.get("received")
        if received is not None:
            if isinstance(received, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", received):
                clean["received"] = received
            else:
                print(f"  WARNING: {MANIFEST_PATH}: '{code}'.received is not a YYYY-MM-DD date -- ignoring it.")

        if "expected_items" in clean and "received" not in clean:
            print(f"  WARNING: {MANIFEST_PATH}: '{code}' has expected_items but no received date -- "
                  f"add one so a brand-new shipment with zero items still has a reason to stay visible.")

        manifest[code] = clean
    return manifest


def load_enum_cache():
    """{code: {"n": entry_or_None_or_foreign, ..., "_meta": {"prefix","width"}}}
    of conclusively settled numbers as of the last successful run. The
    "_meta" key (absent on cache written before the manifest existed) is
    handled by cached_numbers_for() in build_shipments_data, not here."""
    try:
        with open(ENUM_CACHE_PATH, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"  WARNING: {ENUM_CACHE_PATH} is not valid JSON ({e}) -- starting with an empty cache.")
        return {}
    return raw if isinstance(raw, dict) else {}


def save_enum_cache(cache):
    with open(ENUM_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, separators=(",", ":"))


def load_completed_history():
    """{code: {"name","total","completed","discovery","pages_completed","completed_date"}}
    for every shipment ever seen at completed==total. Kept indefinitely (it's small);
    only the COMPLETED_DISPLAY_COUNT most recent are ever shown on the site."""
    try:
        with open(COMPLETED_HISTORY_PATH, encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"  WARNING: {COMPLETED_HISTORY_PATH} is not valid JSON ({e}) -- starting with an empty history.")
        return {}
    return raw if isinstance(raw, dict) else {}


def save_completed_history(history):
    with open(COMPLETED_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, separators=(",", ":"))


# ---------- Per-code aggregation shared by auto-detected and declared codes ----------

def process_enumerated_code(indexed_by_code, code, prefix, numbers, cached,
                             lo, min_hi, width, expect_code, manifest_expected):
    """
    Runs enumerate_full_shipment for one code and turns the result into a
    display-ready group dict, whether the code was auto-pattern-detected or
    manifest-declared -- the two differ only in what they pass for
    lo/min_hi/width/expect_code/manifest_expected.
    """
    entries = indexed_by_code.get(code, [])
    indexed_count = len(entries)
    # While a declared shipment's box isn't fully accounted for yet, don't
    # cache an empty slot as permanently absent -- it may be scanned
    # tomorrow. See enumerate_full_shipment's suppress_absent_upto docs.
    suppress_absent_upto = min_hi if (manifest_expected and len(numbers) < manifest_expected) else 0

    full, settled, cache_hits, unresolved, foreign, total_probes = enumerate_full_shipment(
        prefix, numbers, cached_resolved=cached, lo=lo, min_hi=min_hi, width=width,
        expect_code=expect_code, suppress_absent_upto=suppress_absent_upto,
    )

    observed = max(len(full), indexed_count)
    if manifest_expected:
        total = max(manifest_expected, observed)
        total_source = "manifest" if manifest_expected >= observed else "manifest-exceeded"
    elif observed > 0:
        total = observed
        total_source = "enumerated"
    else:
        total = 0
        total_source = "unknown"

    # "completed" is deliberately NOT sum(1 for v in full.values() if repub_state == 19).
    # `full` includes items recovered by direct metadata probing, which can be complete
    # (repub_state 19) before archive.org's search index has caught up with them -- a lag
    # of a few days is normal. A partner clicking the shiptracking:<code> search link on
    # the dashboard would then see fewer items than "completed" claimed, with no way to
    # know why. Restricting to identifiers that are BOTH recognized by the enumeration (in
    # `full`) AND actually present in this run's live search results (`entries`) keeps
    # "completed" equal to what that link shows right now. It also keeps completed <= total
    # -- but only together with the max() above, which guarantees total is never smaller
    # than what's actually been observed. Do not "simplify" completed to count every
    # repub_state 19 in `full` without that guarantee alongside it.
    full_identifiers = {v["identifier"] for v in full.values() if isinstance(v, dict) and "identifier" in v}
    completed = sum(
        1 for e in entries
        if e.get("repub_state") == "19" and e.get("identifier") in full_identifiers
    )
    pages_completed = sum(
        as_int(e.get("imagecount")) for e in entries
        if e.get("repub_state") == "19" and e.get("identifier") in full_identifiers
    )
    pages_scanned = sum(as_int(v.get("imagecount")) for v in full.values() if isinstance(v, dict))

    # Finished in metadata but not yet visible in search -- operator-only signal,
    # deliberately not folded into `completed` (see the comment above).
    full_done_identifiers = {
        v["identifier"] for v in full.values()
        if isinstance(v, dict) and v.get("repub_state") == "19"
    }
    indexed_identifiers = {e["identifier"] for e in entries}
    completed_unindexed = len(full_done_identifiers - indexed_identifiers)

    last_republish = None
    last_added = None
    for v in full.values():
        if not isinstance(v, dict):
            continue
        pd = parse_publicdate(v.get("publicdate"))
        if pd and (last_added is None or pd > last_added):
            last_added = pd
    # republisher_date is only present in the bulk index fields, not the per-item probe results
    for it in entries:
        rd = parse_republisher_date(it.get("republisher_date"))
        if rd and (last_republish is None or rd > last_republish):
            last_republish = rd

    assert completed <= total, f"{code}: completed ({completed}) exceeds total ({total})"

    group = {
        "total": total,
        "completed": completed,
        "completed_unindexed": completed_unindexed,
        "pages_completed": pages_completed,
        "pages_scanned": pages_scanned,
        "last_republish": last_republish,
        "last_added": last_added,
        "discovery": "enumerated",
        "unresolved": unresolved,
        "foreign": foreign,
        "total_probes": total_probes,
        "total_source": total_source,
        "total_confirmed": total_source not in ("indexed-only", "unknown"),
        "expected_items": manifest_expected,
        "observed_items": observed,
        "overage": max(0, observed - manifest_expected) if manifest_expected else 0,
    }
    return group, {str(n): e for n, e in settled.items()}, cache_hits


# ---------- Aggregation ----------

def build_shipments_data(items, manifest=None, prev_shipments=None):
    if manifest is None:
        manifest = load_manifest()
    prev_by_code = {s["code"]: s for s in (prev_shipments or [])}

    indexed_by_code = defaultdict(list)
    for it in items:
        code = it.get("shiptracking")
        if code:
            indexed_by_code[code].append(it)

    declared_codes = {code for code, m in manifest.items() if m.get("identifier_prefix")}
    candidates = find_enumerable_candidates(indexed_by_code, declared_codes=declared_codes)

    print(f"Detected {len(candidates)} auto-pattern-matched shiptracking code(s), "
          f"{len(declared_codes)} manifest-declared.")

    enum_cache = load_enum_cache()
    new_enum_cache = {}

    def cached_numbers_for(code, prefix, width):
        """
        Numbers cache is invalidated for a code whose declared prefix or
        width changed since it was written -- otherwise a corrected typo in
        the manifest would have its old prefix's confirmed-absent slots
        poison the new range forever.
        """
        raw = enum_cache.get(code, {})
        meta = raw.get("_meta") if isinstance(raw, dict) else None
        if meta and (meta.get("prefix") != prefix or (width is not None and meta.get("width") not in (None, width))):
            print(f"  {code}: identifier_prefix/number_width changed -- discarding its stale cache.")
            return {}
        return {int(k): v for k, v in raw.items() if k != "_meta"}

    groups = {}
    total_unresolved = 0
    total_cache_hits = 0
    manifest_warnings = []
    hard_block = False

    # Auto-pattern-matched codes: unchanged behavior from before the manifest existed.
    for code, (prefix, numbers) in candidates.items():
        cached = cached_numbers_for(code, prefix, None)
        cache_note = f" ({len(cached)} settled in cache)" if cached else ""
        print(f"  enumerating {code} (prefix={prefix}, auto-detected){cache_note}...", flush=True)
        g, settled_str, cache_hits = process_enumerated_code(
            indexed_by_code, code, prefix, numbers, cached,
            lo=1, min_hi=0, width=None, expect_code=None, manifest_expected=None,
        )
        new_enum_cache[code] = {"_meta": {"prefix": prefix, "width": None}, **settled_str}
        total_cache_hits += cache_hits
        total_unresolved += g["unresolved"]
        if g["unresolved"]:
            print(f"    WARNING: {g['unresolved']} identifier probe(s) for {code} could not be "
                  f"resolved (network error or throttling) -- this row may undercount.", flush=True)
        groups[code] = g

    # Manifest-declared codes: processed directly and UNCONDITIONALLY, whether or
    # not anything has been scanned for them yet. This is the path that makes a
    # brand-new shipment show up the day it's received, not the day archive.org
    # finishes its first item.
    for code, m in manifest.items():
        if code not in declared_codes:
            continue
        prefix = m["identifier_prefix"]
        lo = m.get("number_start", 1)
        expected = m.get("expected_items")
        width = m.get("number_width")

        probe_span = expected
        if expected and expected > MAX_DECLARED_RANGE:
            manifest_warnings.append({
                "code": code, "kind": "range_clamped",
                "detail": f"expected_items={expected} exceeds the {MAX_DECLARED_RANGE}-number probe "
                          f"limit -- only probing the first {MAX_DECLARED_RANGE}. Check for a typo.",
            })
            probe_span = MAX_DECLARED_RANGE
        min_hi = (lo + probe_span - 1) if probe_span else 0

        numbers = {}
        for e in indexed_by_code.get(code, []):
            match = ID_PATTERN.match(e["identifier"])
            if match and match.group(1).lower() == prefix.lower():
                numbers[int(match.group(2))] = {
                    "identifier": e["identifier"],
                    "repub_state": e.get("repub_state"),
                    "publicdate": e.get("publicdate"),
                    "imagecount": e.get("imagecount"),
                }

        cached = cached_numbers_for(code, prefix, width)
        cache_note = f" ({len(cached)} settled in cache)" if cached else ""
        print(f"  enumerating {code} (prefix={prefix}, declared, expected={expected}){cache_note}...", flush=True)

        g, settled_str, cache_hits = process_enumerated_code(
            indexed_by_code, code, prefix, numbers, cached,
            lo=lo, min_hi=min_hi, width=width, expect_code=code, manifest_expected=expected,
        )
        new_enum_cache[code] = {"_meta": {"prefix": prefix, "width": width}, **settled_str}
        total_cache_hits += cache_hits
        total_unresolved += g["unresolved"]
        if g["unresolved"]:
            print(f"    WARNING: {g['unresolved']} identifier probe(s) for {code} could not be "
                  f"resolved (network error or throttling) -- this row may undercount.", flush=True)
        if g["foreign"]:
            manifest_warnings.append({
                "code": code, "kind": "foreign",
                "detail": f"{g['foreign']} probed identifier(s) under prefix '{prefix}' belong to a "
                          f"DIFFERENT shiptracking code -- this prefix is almost certainly wrong.",
            })
            hard_block = True
        if expected and g["observed_items"] > expected:
            manifest_warnings.append({
                "code": code, "kind": "manifest_exceeded",
                "detail": f"expected_items is {expected} but {g['observed_items']} item(s) actually "
                          f"exist -- update {MANIFEST_PATH}.",
            })
        groups[code] = g

    if total_cache_hits:
        print(f"  (cache avoided re-probing {total_cache_hits} already-settled number(s) this run)")

    # Everything else: indexed-only counts (search-based, may undercount hidden stubs).
    for code, entries in indexed_by_code.items():
        if code in groups:
            continue
        total = len(entries)
        completed = sum(1 for e in entries if e.get("repub_state") == "19")
        pages_completed = sum(as_int(e.get("imagecount")) for e in entries if e.get("repub_state") == "19")
        pages_scanned = sum(as_int(e.get("imagecount")) for e in entries)
        last_republish = None
        last_added = None
        for e in entries:
            rd = parse_republisher_date(e.get("republisher_date"))
            if rd and (last_republish is None or rd > last_republish):
                last_republish = rd
            pd = parse_publicdate(e.get("publicdate"))
            if pd and (last_added is None or pd > last_added):
                last_added = pd
        groups[code] = {
            "total": total,
            "completed": completed,
            "completed_unindexed": 0,
            "pages_completed": pages_completed,
            "pages_scanned": pages_scanned,
            "last_republish": last_republish,
            "last_added": last_added,
            "discovery": "indexed-only",
            "unresolved": 0,
            "foreign": 0,
            "total_probes": 0,
            "total_source": "indexed-only",
            "total_confirmed": False,
            "expected_items": None,
            "observed_items": total,
            "overage": 0,
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_WINDOW_DAYS)
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DECLARED_DAYS)

    # A shipment appearing here has had activity in the last ACTIVE_WINDOW_DAYS.
    # This used to be the entire "active" list, which meant a shipment that
    # finished long ago could still clutter the dashboard for up to 90 days
    # after completing, while one that finished 91+ days ago just vanished with
    # no record. Instead: genuinely in-progress shipments (completed < total)
    # are shown here as before ("active", unchanged); a shipment that is fully
    # done is recorded once into COMPLETED_HISTORY_PATH (permanent, keyed by
    # code, never re-added) and only the COMPLETED_DISPLAY_COUNT most recent
    # such completions are ever shown, regardless of the activity window.
    completed_history = load_completed_history()

    # A name recorded into history at completion time is never touched again by
    # the code below (it only ever writes a NEW entry once) -- so a name added
    # or corrected in the manifest after a shipment already completed would
    # otherwise never reach the display. Refresh every existing entry's name
    # from the current manifest on every run; harmless no-op when nothing changed.
    for code, h in completed_history.items():
        h["name"] = (manifest.get(code) or {}).get("name") or h.get("name")

    # One-time migration shim (a no-op on every later run): a shipment that was
    # already completed==total in the PREVIOUS snapshot but has since aged out
    # of this run's activity window entirely won't appear in `groups` at all --
    # without this, it would be silently lost from completed_history forever
    # the moment this feature was turned on, rather than just missing the "last
    # N" display cutoff like an intentionally-aged shipment. Only applies to
    # codes not already tracked; every code is tracked permanently once seen.
    for code, prev in prev_by_code.items():
        if code in completed_history:
            continue
        if prev.get("total") and prev["completed"] >= prev["total"]:
            completed_history[code] = {
                "name": prev.get("name"),
                "total": prev["total"],
                "completed": prev["completed"],
                "discovery": prev.get("discovery"),
                "pages_completed": prev.get("pages_completed"),
                "completed_date": prev.get("last_activity") or date.today().strftime("%Y-%m-%d"),
            }

    active = []
    just_finalized_codes = []
    for code, g in groups.items():
        m = manifest.get(code, {})
        last_dates = [d for d in (g["last_republish"], g["last_added"]) if d]
        recent = bool(last_dates) and max(last_dates) >= cutoff

        # A manifest-declared shipment that has never had ANY activity at all
        # (no items yet, so no dates to be recent about) still needs to show
        # up -- this is the exact case that was invisible before the manifest
        # existed. It stays visible unconditionally while un-closed; a
        # STALE_DECLARED_DAYS-old one gets a warning, not a removal.
        is_declared_no_activity = bool(m.get("identifier_prefix")) and not m.get("closed") and not last_dates

        if not (recent or is_declared_no_activity):
            continue

        if is_declared_no_activity:
            received_dt = None
            if m.get("received"):
                try:
                    received_dt = datetime.strptime(m["received"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
            if received_dt is None:
                manifest_warnings.append({
                    "code": code, "kind": "no_received_date",
                    "detail": "has identifier_prefix but no 'received' date -- add one so staleness can be tracked.",
                })
            elif received_dt < stale_cutoff:
                manifest_warnings.append({
                    "code": code, "kind": "no_items_yet",
                    "detail": f"received {m['received']} but still has no items after "
                              f"{STALE_DECLARED_DAYS} days -- identifier_prefix is probably wrong.",
                })

        last_activity = max(last_dates).strftime("%Y-%m-%d") if last_dates else None
        is_complete = g["total"] > 0 and g["completed"] >= g["total"]

        row = {
            "code": code,
            "name": m.get("name"),
            "total": g["total"],
            "completed": g["completed"],
            "completed_unindexed": g["completed_unindexed"],
            "pages_completed": g["pages_completed"],
            "pages_scanned": g["pages_scanned"],
            "discovery": g["discovery"],
            "total_source": g["total_source"],
            "total_confirmed": g["total_confirmed"],
            "expected_items": g["expected_items"],
            "observed_items": g["observed_items"],
            "overage": g["overage"],
            "unresolved": g["unresolved"],
            "last_activity": last_activity,
        }

        if not is_complete:
            active.append(row)
            continue

        if code in completed_history:
            # Already recorded as complete in a prior run -- it has already had
            # its one appearance in Active with the finalizing badge, and now
            # lives only in the completed-history list. Nothing to do here.
            continue

        # First time this code is seen at completed==total: record it permanently.
        completed_history[code] = {
            "name": m.get("name"),
            "total": g["total"],
            "completed": g["completed"],
            "discovery": g["discovery"],
            "pages_completed": g["pages_completed"],
            "completed_date": last_activity or date.today().strftime("%Y-%m-%d"),
        }

        prev = prev_by_code.get(code)
        was_in_progress_before = prev is not None and prev.get("total") and prev["completed"] < prev["total"]
        if was_in_progress_before:
            # This run is the one where the final item finished -- show it one
            # last time in Active, flagged, before it settles into history-only.
            just_finalized_codes.append(code)
            active.append({**row, "finalizing": True})
        # else: a code we've never tracked before, already complete the first
        # time we see it -- backfilled into history above with no finalizing
        # badge, since we can't claim it "just" finished.

    active.sort(key=lambda s: (s["completed"] / s["total"] if s["total"] else 0))

    completed_display = sorted(
        completed_history.items(), key=lambda kv: kv[1].get("completed_date") or "", reverse=True
    )[:COMPLETED_DISPLAY_COUNT]
    completed_shipments = [
        {
            "code": code,
            "name": h.get("name"),
            "total": h["total"],
            "completed": h["completed"],
            "discovery": h.get("discovery"),
            "pages_completed": h.get("pages_completed"),
            "completed_date": h.get("completed_date"),
        }
        for code, h in completed_display
    ]

    # Totals cover what's actually rendered (active + the visible completed
    # rows) so the KPI row measures the same universe a viewer sees, not the
    # entire unbounded completed_history.
    total_items = sum(s["total"] for s in active) + sum(s["total"] for s in completed_shipments)
    total_completed = sum(s["completed"] for s in active) + sum(s["completed"] for s in completed_shipments)
    total_pages_completed = (
        sum(s.get("pages_completed") or 0 for s in active)
        + sum(s.get("pages_completed") or 0 for s in completed_shipments)
    )
    # Sum of what was actually OBSERVED this run, across every code processed --
    # independent of any manifest inflation. This is what the outage/emptiness
    # guard in sanity_check() keys off, so a manifest can never blind it.
    total_observed_items = sum(g["observed_items"] for g in groups.values())
    total_probes = sum(g.get("total_probes", 0) for g in groups.values())

    if just_finalized_codes:
        print(f"  {len(just_finalized_codes)} shipment(s) just reached 100% complete: {', '.join(just_finalized_codes)}")
    if manifest_warnings:
        print(f"  {len(manifest_warnings)} manifest warning(s):")
        for w in manifest_warnings:
            print(f"    {w['code']}: {w['detail']}")

    return {
        "generated_note": "Snapshot of archive.org metadata for collection:allen_county, grouped by shiptracking, including stub items recovered via direct identifier discovery or manifest declaration where possible",
        "active_window_days": ACTIVE_WINDOW_DAYS,
        "shipment_count": len(active),
        "total_items": total_items,
        "total_completed": total_completed,
        "total_pages_completed": total_pages_completed,
        "total_observed_items": total_observed_items,
        "total_probes": total_probes,
        "unresolved_probes": total_unresolved,
        "manifest_warnings": manifest_warnings,
        "shipments": active,
        "completed_shipments": completed_shipments,
    }, new_enum_cache, completed_history, hard_block


def inject(html, data, snapshot_date):
    data_json = json.dumps(data, separators=(",", ":"))

    html, n1 = re.subn(
        r"^const SHIPMENTS = .*;$",
        "const SHIPMENTS = " + data_json.replace("\\", "\\\\") + ";",
        html, count=1, flags=re.M,
    )
    html, n2 = re.subn(
        r'^const SNAPSHOT_DATE = ".*";$',
        f'const SNAPSHOT_DATE = "{snapshot_date}";',
        html, count=1, flags=re.M,
    )

    if not (n1 and n2):
        raise RuntimeError(
            f"Could not find expected markers in {SITE_PATH} "
            f"(SHIPMENTS matched {n1}, SNAPSHOT_DATE matched {n2}). "
            "The file may have been edited in a way that moved/renamed these lines."
        )
    return html


def previous_data(html):
    """The data currently in the file we are about to overwrite."""
    m = re.search(r"^const SHIPMENTS = (.*);$", html, re.M)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def sanity_check(html, data, force=False):
    """
    Refuse to overwrite a working dashboard with implausible data.

    The required_keys check only ever verified that keys EXIST. An archive.org
    outage, a throttled run, or a typo'd collection query all produce a
    perfectly well-formed result with every key present and zeroes in it --
    which the scheduled Action would then commit and push to the partner's
    live URL with nobody in the loop. This keys off total_observed_items
    rather than the manifest-inflated total_items so a declared shipment
    can never blind this guard.
    """
    problems = []

    if not data["shipment_count"] or not data.get("total_observed_items"):
        problems.append(
            f"result is empty (shipments={data['shipment_count']}, observed items="
            f"{data.get('total_observed_items')}) -- archive.org may be unreachable, or the "
            "collection query may be wrong"
        )

    unresolved_limit = max(MAX_UNRESOLVED_PROBES, int(MAX_UNRESOLVED_PROBE_RATE * data.get("total_probes", 0)))
    if data.get("unresolved_probes", 0) > unresolved_limit:
        problems.append(
            f"{data['unresolved_probes']} identifier probes could not be resolved "
            f"(limit {unresolved_limit} of {data.get('total_probes', 0)} probes this run) -- "
            "totals would undercount"
        )

    prev = previous_data(html)
    if prev:
        prev_observed = prev.get("total_observed_items", prev.get("total_items"))
        if prev_observed:
            drop = 100.0 * (prev_observed - data["total_observed_items"]) / prev_observed
            if drop > MAX_SHRINK_PCT:
                problems.append(
                    f"observed items fell {drop:.0f}% ({prev_observed:,} -> {data['total_observed_items']:,}), "
                    f"more than the {MAX_SHRINK_PCT}% limit"
                )
        if prev.get("total_items"):
            total_drop = 100.0 * (prev["total_items"] - data["total_items"]) / prev["total_items"]
            if total_drop > MAX_DISPLAYED_SHRINK_PCT:
                problems.append(
                    f"displayed total items fell {total_drop:.0f}% ({prev['total_items']:,} -> "
                    f"{data['total_items']:,}) -- check for a manifest typo (expected_items set too "
                    "low, or a shipment marked closed by mistake)"
                )

    if not problems:
        return True

    print()
    print("REFUSING TO WRITE -- the new data does not look plausible:")
    for p in problems:
        print(f"  - {p}")
    if force:
        print()
        print("  (--force given: writing it anyway)")
        return True
    print()
    print("Nothing was changed. If this is real -- e.g. several finished shipments")
    print("aged out of the active window at once -- re-run with --force to publish it.")
    return False


def main():
    try:
        with open(SITE_PATH, encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        print(f"Could not find {SITE_PATH} in the current directory.")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    manifest = load_manifest()
    declared_codes = {code for code, m in manifest.items() if m.get("identifier_prefix")}

    if "--full-scan" in sys.argv:
        print("--full-scan given: pulling the ENTIRE collection (the slow, exhaustive audit path).")
        items = fetch_all_items()
    else:
        discovered_codes = fetch_discovery_items(now)
        target_codes = discovered_codes | declared_codes
        items = fetch_items_for_codes(target_codes)

    if not items and not declared_codes:
        print("archive.org returned no items at all -- refusing to write. Nothing was changed.")
        sys.exit(1)

    prev = previous_data(html)
    data, new_enum_cache, new_completed_history, hard_block = build_shipments_data(
        items, manifest=manifest, prev_shipments=(prev or {}).get("shipments", [])
    )

    if hard_block:
        print()
        print("REFUSING TO WRITE -- a manifest-declared identifier_prefix collided with a "
              "DIFFERENT shipment's real items. This cannot be overridden with --force; fix "
              f"the prefix in {MANIFEST_PATH} first. See the manifest warning(s) above.")
        sys.exit(1)

    snapshot_date = date.today().strftime("%B %-d, %Y")

    required_keys = {
        "active_window_days", "shipment_count", "total_items", "total_completed",
        "total_pages_completed", "total_observed_items", "shipments", "completed_shipments",
    }
    missing = required_keys - data.keys()
    if missing:
        print(f"Refusing to write site: built data is missing expected keys: {sorted(missing)}")
        sys.exit(1)

    if not sanity_check(html, data, force="--force" in sys.argv):
        sys.exit(1)

    new_html = inject(html, data, snapshot_date)

    with open(SITE_PATH, "w", encoding="utf-8") as f:
        f.write(new_html)

    # Deferred until AFTER a successful write, so a refused/failed run never
    # pollutes the enumeration cache (or the completed-shipment history) with
    # data from a snapshot nobody actually published.
    save_enum_cache(new_enum_cache)
    save_completed_history(new_completed_history)

    print()
    print("Done. Site data updated:")
    print(f"  Active shipments (last {ACTIVE_WINDOW_DAYS} days): {data['shipment_count']}")
    print(f"  Items completed / total: {data['total_completed']:,} / {data['total_items']:,}")
    print(f"  Pages completed: {data['total_pages_completed']:,}")
    print(f"  Snapshot date: {snapshot_date}")
    for s in data["shipments"]:
        flag = "" if s["discovery"] == "enumerated" else "  (indexed-only, may undercount stubs)"
        if not s.get("total_confirmed", True):
            flag += "  [ESTIMATED]"
        if s.get("total_source") == "manifest-exceeded":
            flag += f"  [MANIFEST MISMATCH: expected {s['expected_items']}, found {s['observed_items']}]"
        if s.get("unresolved"):
            flag += f"  ({s['unresolved']} probe(s) unresolved)"
        if s.get("finalizing"):
            flag += "  [FINALIZING -- just reached 100%]"
        label = f"{s['code']} - {s['name']}" if s.get("name") else s["code"]
        print(f"    {label:44s} {s['completed']:4d} / {s['total']:4d}{flag}")
    if data["completed_shipments"]:
        print(f"  Recently completed (showing {len(data['completed_shipments'])} of {len(new_completed_history)} tracked):")
        for s in data["completed_shipments"]:
            label = f"{s['code']} - {s['name']}" if s.get("name") else s["code"]
            print(f"    {label:44s} {s['completed']:4d} / {s['total']:4d}  (completed {s['completed_date']})")
    if data.get("manifest_warnings"):
        print(f"  {len(data['manifest_warnings'])} manifest warning(s) -- see above.")


if __name__ == "__main__":
    main()
