"""
shipment_core.py -- the shared engine behind every partner-shipment dashboard.

Both dashboard scripts import this file and add only what is specific to their
shape:

    update_site.py                  one partner: active / recently-completed
                                    lists, written into that partner's site
    refresh_center_dashboard.py     a whole scanning center: every partner
                                    with a current project, rolled up

Keep this file next to whichever script imports it (for a hosted site that
means committing BOTH files to the site repo -- the scheduled Action runs
update_site.py, which imports this). A fix made here reaches every dashboard
that uses it; do not fork a private copy per partner.

What it does, in the order a run uses it:

1. TWO-PHASE FETCH. One cheap query finds which group codes were active in
   the last ACTIVE_WINDOW_DAYS, then each code's COMPLETE item history is
   pulled with its own query. (--full-scan pulls the whole collection
   instead, as an audit.) A code whose query fails is retried once; if it
   still fails the caller must not publish, because a silently missing
   shipment is a wrong number on a page someone reads.

2. STUB DISCOVERY. archive.org hides received-but-unprocessed items from
   search (noindex:true). Where a shipment's identifiers follow
   "prefix + sequential number", or the manifest declares a prefix, this
   probes archive.org/metadata/<id> directly to recover the true total.

3. THE MANIFEST. A per-partner JSON file (see shipments.example.json) giving
   each shipment a friendly name and, optionally, its identifier prefix,
   packing-list item count and received date. A declared shipment shows up
   the day it is received, before archive.org has anything to show for it.

4. CACHING. Numbers conclusively settled (a finished item, or a slot checked
   and found empty) are not re-probed every run. Empty slots are re-checked
   every ABSENT_RECHECK_DAYS because archive.org can create an item for a
   number that was empty earlier.

5. POLITE RETRIES. Every request backs off exponentially with jitter, honors
   Retry-After, and a 429/503 pauses ALL worker threads, not just the one
   that was refused.

Configuration is module-level state set once by the calling script through
configure(); every function below reads it at call time.
"""

import glob
import json
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone, timedelta


SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
METADATA_URL = "https://archive.org/metadata/"

# ---- Set by the calling script via configure() ------------------------------
COLLECTION_QUERY = "(set by configure)"   # e.g. "collection:allen_county"
GROUPING_FIELD = "(set by configure)"       # e.g. "shiptracking"
COMPLETE_FIELD = "(set by configure)"       # e.g. "repub_state"
COMPLETE_VALUE = "(set by configure)"       # e.g. "19"
EXTRA_FIELDS = ()                            # extra search fields to pull (the center dashboard adds its partner field)
ACTIVE_WINDOW_DAYS = 90
MANIFEST_PATH = "(set by configure)"
ENUM_CACHE_PATH = "(set by configure)"
USER_AGENT = "partner-shipment-dashboard/1.0"
MAX_DECLARED_RANGE = 2000       # never probe further than this past a declared number_start, even if expected_items claims more -- typo guard
STALE_DECLARED_DAYS = 120       # a declared shipment with zero items this long after `received` gets flagged (still shown)
DEFAULT_NUMBER_WIDTH = 2        # zero-padding width to assume for a declared shipment with nothing to detect it from
ABSENT_RECHECK_DAYS = 30        # how long an empty slot stays cached as "doesn't exist" before it is probed again
FORCE_RECHECK_ABSENT = False    # set by --full-scan / --recheck-absent

_CONFIG_NAMES = {
    "COLLECTION_QUERY", "GROUPING_FIELD", "COMPLETE_FIELD", "COMPLETE_VALUE", "EXTRA_FIELDS",
    "ACTIVE_WINDOW_DAYS", "MANIFEST_PATH", "ENUM_CACHE_PATH", "USER_AGENT", "MAX_DECLARED_RANGE",
    "STALE_DECLARED_DAYS", "DEFAULT_NUMBER_WIDTH", "ABSENT_RECHECK_DAYS", "FORCE_RECHECK_ABSENT",
}

ID_PATTERN = re.compile(r"^([a-zA-Z]+?)(\d+)$")


def configure(**settings):
    """Set module-level configuration. Unknown names are an error, not ignored --
    a misspelled setting that silently did nothing would be very hard to find."""
    unknown = set(settings) - _CONFIG_NAMES
    if unknown:
        raise TypeError(f"configure() got unknown setting(s): {sorted(unknown)}")
    globals().update(settings)


def require_filled_in(namespace):
    """Exit with a plain message if any UPPERCASE setting in `namespace` (a
    script's globals()) still starts with TODO -- i.e. its CONFIG block was
    never filled in. Without this a half-configured script runs happily and
    queries nonsense."""
    todo = sorted(
        name for name, value in namespace.items()
        if name.isupper() and isinstance(value, str) and value.startswith("TODO")
    )
    if todo:
        print("This script's CONFIG block still has placeholder values to fill in:")
        for name in todo:
            print(f"  {name} = {namespace[name]!r}")
        sys.exit(1)


# ---------- HTTP with backoff ----------

_gate_lock = threading.Lock()
_pause_until = 0.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _wait_for_gate():
    while True:
        with _gate_lock:
            remaining = _pause_until - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 5))


def _close_gate(seconds):
    """Make every worker thread wait `seconds`. Returns True if this call is
    what closed it (so only one thread prints the message)."""
    global _pause_until
    with _gate_lock:
        was_open = _pause_until <= time.monotonic()
        _pause_until = max(_pause_until, time.monotonic() + seconds)
    return was_open


def _retry_after_seconds(err):
    try:
        return float(err.headers.get("Retry-After"))
    except (TypeError, ValueError, AttributeError):
        return None


def http_get_json(url, timeout=60, retries=4):
    """
    GET a URL and parse it as JSON, retrying transient failures with
    exponential backoff plus jitter. A 429/503 (archive.org telling us to slow
    down) closes a gate that pauses ALL threads for the Retry-After period --
    with six workers hammering the API, only backing off the one that was
    refused just gets the next five refused too. A non-retryable HTTP error
    (e.g. 404) is raised immediately. Raises the last error once `retries`
    attempts are used up.
    """
    last = None
    for attempt in range(retries):
        _wait_for_gate()
        delay = 0.0
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code not in RETRYABLE_STATUS:
                raise
            if e.code in (429, 503):
                wait = min(_retry_after_seconds(e) or 2 ** (attempt + 2), 120)
                if _close_gate(wait):
                    print(f"  (archive.org asked us to slow down -- pausing all requests {wait:.0f}s)", flush=True)
            else:
                delay = 2 ** (attempt + 1)
        except Exception as e:  # network error, timeout, truncated or garbled body
            last = e
            delay = 1.5 * 2 ** attempt
        if delay and attempt < retries - 1:
            time.sleep(delay + random.uniform(0, delay / 2))
    raise last


def fetch_scrape_page(params, retries=5):
    return http_get_json(SCRAPE_URL + "?" + urllib.parse.urlencode(params), timeout=60, retries=retries)


def search_fields():
    """Fields pulled from the search index, in a stable order, without duplicates."""
    wanted = ("identifier", GROUPING_FIELD, COMPLETE_FIELD, "republisher_date", "publicdate", "imagecount") + tuple(EXTRA_FIELDS)
    return ",".join(dict.fromkeys(wanted))


def collection_item_count():
    """How many items the collection query matches right now. Used to tell
    'nothing was recently active' (fine) from 'the query matches nothing at all'
    (a typo, or archive.org unreachable)."""
    data = fetch_scrape_page({"q": COLLECTION_QUERY, "count": "100", "fields": "identifier"})
    total = data.get("total")
    return total if isinstance(total, int) else len(data.get("items", []))


def as_int(value):
    """imagecount (and a few other fields) come back as an int from the
    scrape API but as a string from the metadata API -- summing a mix of
    both with a plain sum() raises TypeError. Coerce leniently; anything
    missing or unparseable counts as 0 pages, not a crash."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

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

# ---------- Bulk fetch of the searchable index ----------

def fetch_all_items():
    """Full-collection pull, only used with --full-scan. See fetch_discovery_items
    and fetch_items_for_codes for the default, much faster path."""
    print(f"Fetching EVERY searchable item from archive.org ({COLLECTION_QUERY}) -- full scan, the slow path...")
    items = []
    cursor = None
    page = 0
    total = None
    while True:
        page += 1
        params = {"q": COLLECTION_QUERY, "count": "10000", "fields": search_fields()}
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
    q = (f"{COLLECTION_QUERY} AND (republisher_date:[{cutoff.strftime('%Y%m%d%H%M%S')} TO 99991231235959] "
         f"OR publicdate:[{cutoff.strftime('%Y-%m-%d')} TO 2099-12-31])")
    print(f"Discovering {GROUPING_FIELD} codes with activity in the last {ACTIVE_WINDOW_DAYS} days...")
    items = []
    cursor = None
    page = 0
    while True:
        page += 1
        params = {"q": q, "count": "10000", "fields": f"identifier,{GROUPING_FIELD}"}
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
    codes = {it[GROUPING_FIELD] for it in items if it.get(GROUPING_FIELD)}
    print(f"  found {len(codes)} recently-active {GROUPING_FIELD} code(s)")
    return codes

def fetch_items_for_codes(codes):
    """
    Phase B of the default path: for each candidate code, pull its COMPLETE
    indexed item set with its own targeted query -- not date-restricted --
    so pattern-detection and totals are exactly as accurate as scanning the
    whole collection would produce. Codes are fetched concurrently since
    each query is independent and small.

    Returns (items, failed_codes). A code whose query fails is retried once
    after a pause; one that still fails is returned in failed_codes, and the
    caller must refuse to publish. Carrying on without it would put a wrong
    number (a shipment silently missing, or shrunk) on the page -- and the
    shrink guard only notices when it's a big share of the total.
    """
    if not codes:
        return [], []
    print(f"Fetching complete item history for {len(codes)} {GROUPING_FIELD} code(s)...")

    def fetch_one(code):
        results = []
        cursor = None
        while True:
            params = {"q": f"{COLLECTION_QUERY} AND {GROUPING_FIELD}:{code}", "count": "1000", "fields": search_fields()}
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
    failed = []
    done = 0
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_one, code): code for code in sorted(codes)}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                all_items.extend(fut.result())
            except Exception as e:
                failed.append(code)
                print(f"  WARNING: could not fetch items for {code}: {e}")
            done += 1
            if done % 10 == 0 or done == len(codes):
                print(f"  fetched {done}/{len(codes)} codes ({len(all_items):,} items so far)", flush=True)

    if failed:
        print(f"  retrying {len(failed)} failed code(s) once...")
        time.sleep(5)
        still_failed = []
        for code in failed:
            try:
                all_items.extend(fetch_one(code))
            except Exception as e:
                still_failed.append(code)
                print(f"  WARNING: {code} failed again: {e}")
        failed = still_failed
    return all_items, sorted(failed)


def load_items(now, extra_codes=(), full_scan=False):
    """
    The fetch step both dashboards share. Default path: discover recently
    active codes, add `extra_codes` (manifest-declared or counted shipments,
    which may have no recent activity to be discovered by), then pull each
    code's full history. full_scan pulls the whole collection instead.
    Returns (items, failed_codes).
    """
    if full_scan:
        print(f"--full-scan given: pulling the ENTIRE {COLLECTION_QUERY} collection "
              "(the slow, exhaustive audit path).")
        return fetch_all_items(), []
    discovered = fetch_discovery_items(now)
    return fetch_items_for_codes(discovered | set(extra_codes))


def check_not_empty(items, has_declared):
    """
    Decide whether an empty fetch is a legitimate quiet period or a broken
    query/outage. Returns True when it is safe to continue with no items.

    Nothing recently active is NORMAL once every shipment has finished --
    refusing to update then would fail the daily run until a new shipment
    arrived. But a typo'd COLLECTION_QUERY (or archive.org down) looks
    identical, so confirm the collection itself still has items.
    """
    if items or has_declared:
        return True
    count = collection_item_count()
    if count > 0:
        print(f"No items were recently active, but {COLLECTION_QUERY} has {count:,} items -- treating this as a quiet period.")
        return True
    print(f"{COLLECTION_QUERY} matches no items at all -- refusing to write. Check the query. Nothing was changed.")
    return False

# ---------- Stub discovery (noindex items invisible to search) ----------

def fetch_metadata(identifier, retries=4, timeout=15):
    """
    Fetch one item's metadata. Returns the parsed JSON, None when the item
    does not exist (archive.org answers a missing identifier with HTTP 200 and
    an empty body), or "FAIL" when the request could not be completed even
    after backoff -- which is NOT the same as 'does not exist', and callers
    count it separately so a throttled run can't silently shrink a total.
    """
    try:
        data = http_get_json(METADATA_URL + urllib.parse.quote(identifier), timeout=timeout, retries=retries)
    except Exception:
        return "FAIL"
    return data if data else None


def detect_padding_width(numbers_and_strings):
    smallest = min(numbers_and_strings, key=lambda t: t[0])
    return len(smallest[1])

def format_candidate_id(prefix, n, width):
    if n < 10 ** width:
        return prefix + str(n).zfill(width)
    return prefix + str(n)

def discover_width(prefix, lo, default=None):
    """
    Probe a handful of candidate zero-padding widths at position `lo` to
    guess how a brand-new declared shipment's identifiers are numbered, when
    no items exist yet to infer it from. Falls back to `default` if none
    hit -- probing will simply come up empty until a real item appears (or
    number_width is set explicitly in the manifest).
    """
    if default is None:
        default = DEFAULT_NUMBER_WIDTH
    for width in (2, 3, 4, 1):
        ident = format_candidate_id(prefix, lo, width)
        data = fetch_metadata(ident)
        if data and data != "FAIL":
            return width
    return default

def _is_settled(entry):
    """
    True for a number that will never need re-probing: a confirmed-absent
    slot (None), a real item that has finished digitization (COMPLETE_FIELD == COMPLETE_VALUE, which doesn't regress), or a slot confirmed to belong to a
    DIFFERENT shipment ({"foreign": ...} -- see enumerate_full_shipment). A
    found-but-still-in-progress item is deliberately NOT settled -- it must
    keep being re-probed until it actually finishes, or its progress would
    freeze in the cache.
    """
    return (
        entry is None
        or (isinstance(entry, dict) and entry.get(COMPLETE_FIELD) == COMPLETE_VALUE)
        or (isinstance(entry, dict) and "foreign" in entry)
    )

def enumerate_full_shipment(prefix, known_numbers, cached_resolved=None, batch_size=15,
                            max_extra_batches=8, lo=1, min_hi=0, width=None,
                            expect_code=None, suppress_absent_upto=0):
    """
    known_numbers: {number: {"identifier", COMPLETE_FIELD, "publicdate", "imagecount"}}
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
    expect_code: when set, a probed identifier whose own `GROUPING_FIELD`
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
                         GROUPING_FIELD code -- signals a colliding prefix guess.
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
            actual = md.get(GROUPING_FIELD)
            if actual and str(actual).upper() != expect_code.upper():
                return (n, {"foreign": actual}, True)
        return (n, {
            "identifier": ident,
            COMPLETE_FIELD: md.get(COMPLETE_FIELD),
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
    Detect GROUPING_FIELD codes whose identifiers show a clean prefix+number
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
        # archive.org identifiers are CASE-SENSITIVE (merwinfam01 exists, MERWINFAM01
        # does not), so the prefix must keep its original case -- lowercasing it
        # here made every probe for a mixed-case batch (SmithFam01) 'not found'.
        prefixes = set(m.group(1) for _, m in good)
        if len(prefixes) != 1:
            continue
        prefix = next(iter(prefixes))
        numbers = {}
        for e, m in good:
            numbers[int(m.group(2))] = {
                "identifier": e["identifier"],
                COMPLETE_FIELD: e.get(COMPLETE_FIELD),
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

# ---------- Manifest + caches ----------

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

def load_manifest(path=None):
    """
    {code: {"name", "identifier_prefix", "number_width", "number_start",
    "expected_items", "received", "closed"}} -- see shipments.example.json
    for the field meanings. Every field but the key is optional. An invalid
    individual field is dropped with a warning; the rest of the entry
    survives (this is what lets a plain {"name": "..."} entry behave exactly
    like the old names-only file).
    """
    path = path or MANIFEST_PATH
    raw = _load_json_map(path, dict, "shipment")
    manifest = {}
    for code, entry in raw.items():
        clean = {"closed": bool(entry.get("closed", False))}

        name = entry.get("name")
        if name is not None:
            if isinstance(name, str):
                clean["name"] = name
            else:
                print(f"  WARNING: {path}: '{code}'.name is not a string -- ignoring it.")

        prefix = entry.get("identifier_prefix")
        if prefix is not None:
            if isinstance(prefix, str) and re.match(r"^[a-zA-Z][a-zA-Z0-9_.-]*$", prefix):
                clean["identifier_prefix"] = prefix
            else:
                print(f"  WARNING: {path}: '{code}'.identifier_prefix is invalid -- ignoring it.")

        width = entry.get("number_width")
        if width is not None:
            if isinstance(width, int) and width > 0:
                clean["number_width"] = width
            else:
                print(f"  WARNING: {path}: '{code}'.number_width is invalid -- ignoring it.")

        start = entry.get("number_start", 1)
        if isinstance(start, int) and start > 0:
            clean["number_start"] = start
        else:
            print(f"  WARNING: {path}: '{code}'.number_start is invalid -- defaulting to 1.")
            clean["number_start"] = 1

        expected = entry.get("expected_items")
        if expected is not None:
            if isinstance(expected, int) and expected > 0:
                clean["expected_items"] = expected
            else:
                print(f"  WARNING: {path}: '{code}'.expected_items is invalid -- ignoring it.")

        received = entry.get("received")
        if received is not None:
            if isinstance(received, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", received):
                clean["received"] = received
            else:
                print(f"  WARNING: {path}: '{code}'.received is not a YYYY-MM-DD date -- ignoring it.")

        if "expected_items" in clean and "received" not in clean:
            print(f"  WARNING: {path}: '{code}' has expected_items but no received date -- "
                  f"add one so a brand-new shipment with zero items still has a reason to stay visible.")

        manifest[code] = clean
    return manifest

def load_manifests(patterns):
    """
    Merge every manifest file matching these glob patterns into one
    {code: entry} map -- the center-wide dashboard uses this to pick up each
    partner's own manifest. The first file loaded wins when two define the
    same code, with a warning (it should not happen in practice).
    """
    merged = {}
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            if path.endswith(".example.json"):
                continue
            found = load_manifest(path)
            for code, entry in found.items():
                if code in merged:
                    if merged[code] != entry:
                        print(f"  WARNING: {GROUPING_FIELD} code '{code}' appears in more than one manifest -- keeping the first loaded.")
                    continue
                merged[code] = entry
            if found:
                print(f"  loaded {len(found)} shipment(s) from {path}")
    return merged


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

    # "completed" is deliberately NOT sum(1 for v in full.values() if COMPLETE_FIELD == COMPLETE_VALUE).
    # `full` includes items recovered by direct metadata probing, which can be complete
    # (COMPLETE_FIELD == COMPLETE_VALUE) before archive.org's search index has caught up with them -- a lag
    # of a few days is normal. A partner clicking the GROUPING_FIELD:<code> search link on
    # the dashboard would then see fewer items than "completed" claimed, with no way to
    # know why. Restricting to identifiers that are BOTH recognized by the enumeration (in
    # `full`) AND actually present in this run's live search results (`entries`) keeps
    # "completed" equal to what that link shows right now. It also keeps completed <= total
    # -- but only together with the max() above, which guarantees total is never smaller
    # than what's actually been observed. Do not "simplify" completed to count every
    # completed items in `full` without that guarantee alongside it.
    full_identifiers = {v["identifier"] for v in full.values() if isinstance(v, dict) and "identifier" in v}
    completed = sum(
        1 for e in entries
        if e.get(COMPLETE_FIELD) == COMPLETE_VALUE and e.get("identifier") in full_identifiers
    )
    pages_completed = sum(
        as_int(e.get("imagecount")) for e in entries
        if e.get(COMPLETE_FIELD) == COMPLETE_VALUE and e.get("identifier") in full_identifiers
    )
    pages_scanned = sum(as_int(v.get("imagecount")) for v in full.values() if isinstance(v, dict))

    # Finished in metadata but not yet visible in search -- operator-only signal,
    # deliberately not folded into `completed` (see the comment above).
    full_done_identifiers = {
        v["identifier"] for v in full.values()
        if isinstance(v, dict) and v.get(COMPLETE_FIELD) == COMPLETE_VALUE
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

# ---------- Whole-collection aggregation ----------

def build_groups(items, manifest, enum_cache=None):
    """
    Turn a flat list of search-index items into one display-ready group per
    GROUPING_FIELD code: enumerated (auto-detected or manifest-declared) where
    hidden items could be recovered, indexed-only otherwise. Shared by every
    dashboard shape -- single-partner and center-wide differ only in what they
    do with these groups afterward.

    Returns {"groups", "indexed_by_code", "enum_cache", "warnings",
    "hard_block", "unresolved"}. hard_block means a manifest prefix collided
    with a different shipment's real items (or differs only in case): the
    caller must refuse to publish, and --force must not override it.
    """
    if enum_cache is None:
        enum_cache = load_enum_cache()
    indexed_by_code = defaultdict(list)
    for it in items:
        code = it.get(GROUPING_FIELD)
        if code:
            indexed_by_code[code].append(it)

    declared_codes = {code for code, m in manifest.items() if m.get("identifier_prefix")}
    candidates = find_enumerable_candidates(indexed_by_code, declared_codes=declared_codes)

    print(f"Detected {len(candidates)} auto-pattern-matched {GROUPING_FIELD} code(s), "
          f"{len(declared_codes)} manifest-declared.")

    new_enum_cache = {}

    today = date.today()

    def cached_numbers_for(code, prefix, width):
        """
        Returns (numbers, absent_checked) -- the settled numbers to trust this
        run, and the date to stamp on the cache we write back.

        The cache is invalidated for a code whose declared prefix or width
        changed since it was written -- otherwise a corrected typo in the
        manifest would have its old prefix's confirmed-absent slots poison
        the new range forever.

        "Confirmed absent" is the one cached fact that can go stale: archive.org
        can create an item for a number that was empty last month (items
        arrive out of order). A finished item never regresses, so those stay
        cached; but once ABSENT_RECHECK_DAYS pass (or --full-scan /
        --recheck-absent is given) the absent slots are dropped from the
        cache so this run probes them again.
        """
        raw = enum_cache.get(code, {})
        meta = raw.get("_meta") if isinstance(raw, dict) else None
        if meta and (meta.get("prefix") != prefix or (width is not None and meta.get("width") not in (None, width))):
            print(f"  {code}: identifier_prefix/number_width changed -- discarding its stale cache.")
            return {}, today.isoformat()
        numbers = {int(k): v for k, v in raw.items() if k != "_meta"}
        checked = None
        try:
            checked = date.fromisoformat((meta or {}).get("absent_checked"))
        except (TypeError, ValueError):
            pass
        due = FORCE_RECHECK_ABSENT or checked is None or (today - checked).days >= ABSENT_RECHECK_DAYS
        if not due:
            return numbers, checked.isoformat()
        absent = [n for n, e in numbers.items() if e is None]
        if absent:
            print(f"  {code}: re-checking {len(absent)} number(s) last seen as empty "
                  f"(every {ABSENT_RECHECK_DAYS} days, or on --full-scan/--recheck-absent).")
        return {n: e for n, e in numbers.items() if e is not None}, today.isoformat()

    groups = {}
    total_unresolved = 0
    total_cache_hits = 0
    manifest_warnings = []
    hard_block = False

    # Auto-pattern-matched codes: unchanged behavior from before the manifest existed.
    for code, (prefix, numbers) in candidates.items():
        cached, absent_checked = cached_numbers_for(code, prefix, None)
        cache_note = f" ({len(cached)} settled in cache)" if cached else ""
        # A manifest expected_items still applies without an identifier_prefix --
        # it's the only way to know a shipment's real size before the last item
        # is scanned. It only raises the displayed total; it does not change
        # what gets probed.
        expected = (manifest.get(code) or {}).get("expected_items")
        print(f"  enumerating {code} (prefix={prefix}, auto-detected, expected={expected}){cache_note}...", flush=True)
        g, settled_str, cache_hits = process_enumerated_code(
            indexed_by_code, code, prefix, numbers, cached,
            lo=1, min_hi=0, width=None, expect_code=None, manifest_expected=expected,
        )
        new_enum_cache[code] = {"_meta": {"prefix": prefix, "width": None, "absent_checked": absent_checked}, **settled_str}
        total_cache_hits += cache_hits
        total_unresolved += g["unresolved"]
        if g["unresolved"]:
            print(f"    WARNING: {g['unresolved']} identifier probe(s) for {code} could not be "
                  f"resolved (network error or throttling) -- this row may undercount.", flush=True)
        if expected and g["observed_items"] > expected:
            manifest_warnings.append({
                "code": code, "kind": "manifest_exceeded",
                "detail": f"expected_items is {expected} but {g['observed_items']} item(s) actually "
                          f"exist -- update {MANIFEST_PATH}.",
            })
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
            if match and match.group(1).lower() == prefix.lower() and match.group(1) != prefix:
                manifest_warnings.append({
                    "code": code, "kind": "prefix_case",
                    "detail": f"identifier_prefix is '{prefix}' but '{e['identifier']}' differs only in "
                              f"capitalization -- archive.org identifiers are case-sensitive, so fix the "
                              f"manifest entry to match exactly.",
                })
                hard_block = True
                break
            if match and match.group(1) == prefix:
                numbers[int(match.group(2))] = {
                    "identifier": e["identifier"],
                    COMPLETE_FIELD: e.get(COMPLETE_FIELD),
                    "publicdate": e.get("publicdate"),
                    "imagecount": e.get("imagecount"),
                }

        cached, absent_checked = cached_numbers_for(code, prefix, width)
        cache_note = f" ({len(cached)} settled in cache)" if cached else ""
        print(f"  enumerating {code} (prefix={prefix}, declared, expected={expected}){cache_note}...", flush=True)

        g, settled_str, cache_hits = process_enumerated_code(
            indexed_by_code, code, prefix, numbers, cached,
            lo=lo, min_hi=min_hi, width=width, expect_code=code, manifest_expected=expected,
        )
        new_enum_cache[code] = {"_meta": {"prefix": prefix, "width": width, "absent_checked": absent_checked}, **settled_str}
        total_cache_hits += cache_hits
        total_unresolved += g["unresolved"]
        if g["unresolved"]:
            print(f"    WARNING: {g['unresolved']} identifier probe(s) for {code} could not be "
                  f"resolved (network error or throttling) -- this row may undercount.", flush=True)
        if g["foreign"]:
            manifest_warnings.append({
                "code": code, "kind": "foreign",
                "detail": f"{g['foreign']} probed identifier(s) under prefix '{prefix}' belong to a "
                          f"DIFFERENT {GROUPING_FIELD} code -- this prefix is almost certainly wrong.",
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
        observed = len(entries)
        expected = (manifest.get(code) or {}).get("expected_items")
        total = max(expected or 0, observed)
        if expected and observed > expected:
            manifest_warnings.append({
                "code": code, "kind": "manifest_exceeded",
                "detail": f"expected_items is {expected} but {observed} item(s) actually "
                          f"exist -- update {MANIFEST_PATH}.",
            })
        completed = sum(1 for e in entries if e.get(COMPLETE_FIELD) == COMPLETE_VALUE)
        pages_completed = sum(as_int(e.get("imagecount")) for e in entries if e.get(COMPLETE_FIELD) == COMPLETE_VALUE)
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
            # With a packing-list count the total is known, not guessed from search.
            "total_source": ("manifest" if expected >= observed else "manifest-exceeded") if expected else "indexed-only",
            "total_confirmed": bool(expected),
            "expected_items": expected,
            "observed_items": observed,
            "overage": max(0, observed - expected) if expected else 0,
        }

    return {
        "groups": groups,
        "indexed_by_code": indexed_by_code,
        "enum_cache": new_enum_cache,
        "warnings": manifest_warnings,
        "hard_block": hard_block,
        "unresolved": total_unresolved,
    }


# ---------- Writing the data into a page ----------

def inject_const(html, const_name, data, snapshot_date, page_label):
    """Rewrite the page's single-line `const <const_name> = {...};` and
    `const SNAPSHOT_DATE = "...";` statements -- the only two things this
    pipeline ever changes in a page. Raises if either marker is missing."""
    data_json = json.dumps(data, separators=(",", ":"))
    html, n1 = re.subn(
        rf"^const {const_name} = .*;$",
        f"const {const_name} = " + data_json.replace("\\", "\\\\") + ";",
        html, count=1, flags=re.M,
    )
    html, n2 = re.subn(
        r'^const SNAPSHOT_DATE = ".*";$',
        f'const SNAPSHOT_DATE = "{snapshot_date}";',
        html, count=1, flags=re.M,
    )
    if not (n1 and n2):
        raise RuntimeError(
            f"Could not find expected markers in {page_label} "
            f"({const_name} matched {n1}, SNAPSHOT_DATE matched {n2}). "
            "The file may have been edited in a way that moved/renamed these lines."
        )
    return html


def previous_const(html, const_name):
    """The data currently in the page we are about to overwrite, or None."""
    m = re.search(rf"^const {const_name} = (.*);$", html, re.M)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
