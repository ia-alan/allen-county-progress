#!/usr/bin/env python3
"""
Refresh index.html (the live Allen County shipment progress site) with
current data from archive.org.

Run manually with:

    python3 update_site.py

Normally run automatically by .github/workflows/update.yml on a schedule.

It pulls every SEARCHABLE archive.org item in collection:allen_county, groups
by shiptracking code, and additionally recovers "stub" items that archive.org
excludes from search entirely (metadata field noindex:true — used for items
that are received/reserved but not yet fully processed, sitting at
repub_state -1/-2/etc). Those stubs are invisible to any search query, so this
script finds them a different way: for shiptracking codes whose searchable
identifiers follow a detectable "prefix + sequential number" pattern (e.g.
merwinfam04, merwinfam05, ...), it directly probes archive.org/metadata/<id>
for the full number range (including gaps and a run past the highest known
number) to recover the true total. Shiptracking codes with no searchable
items at all (a brand new shipment that hasn't had anything indexed yet) can
only be found this way if you seed one known identifier for them in
allen_county_stub_seeds.json — see that file for the format.

A shipment counts as "active" if it has had a completion (repub_state -> 19)
or a newly-added stub item in the last 90 days.

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
SEEDS_PATH = "allen_county_stub_seeds.json"
NAMES_PATH = "allen_county_shipment_names.json"       # optional {CODE: "Friendly name"} map; see shipment_names.example.json
ENUM_CACHE_PATH = "allen_county_enum_cache.json"  # persists settled stub-discovery numbers across runs -- see README.md "Enumeration cache"

# Guard rails for the write step (see sanity_check below).
MAX_SHRINK_PCT = 40                  # refuse to publish if total items drop more than this vs the current file
MAX_UNRESOLVED_PROBES = 10           # refuse to publish if more than this many identifier probes failed
SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
METADATA_URL = "https://archive.org/metadata/"
QUERY = "collection:allen_county"
FIELDS = "identifier,shiptracking,repub_state,republisher_date,publicdate"
ACTIVE_WINDOW_DAYS = 90

ID_PATTERN = re.compile(r"^([a-zA-Z]+?)(\d+)$")


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
            print(f"  (page fetch failed: {e} — retrying, attempt {attempt + 2}/{retries})", flush=True)
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
        # (5,414,062) instead of this query's. Pin the first value, and never
        # assume it is present at all -- f"{None:,}" raises TypeError.
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
    qualify.
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


def _is_settled(entry):
    """
    True for a number that will never need re-probing: a confirmed-absent
    slot (None), or a real item that has finished digitization
    (COMPLETE_FIELD == COMPLETE_VALUE, which doesn't regress). A found-but-
    still-in-progress item is deliberately NOT settled -- it must keep
    being re-probed until it actually finishes, or its progress would
    freeze in the cache.
    """
    return entry is None or (isinstance(entry, dict) and entry.get("repub_state") == "19")


def enumerate_full_shipment(prefix, known_numbers, cached_resolved=None, batch_size=15, max_extra_batches=8):
    """
    known_numbers: {number: {"identifier", "repub_state", "publicdate"}}
        from THIS run's fresh search index.
    cached_resolved: {number: entry_or_None} of numbers already conclusively
        settled as of a PRIOR run (see _is_settled) -- skipped on reprobe.

    Returns (found, settled, cache_hits, unresolved):
        found      -- {number: entry} for every real item now known.
        settled    -- {number: entry_or_None}, the subset worth caching for
                       next run (see _is_settled).
        cache_hits -- how many numbers were resolved from the cache instead
                       of a network probe, for the summary print.
        unresolved -- probes that FAILED (network/throttling) -- not the
                      same as a number being confirmed absent.

    Walking past the highest known number to look for brand-new hidden
    items always runs here, regardless of the cache -- that check is the
    one thing this whole pipeline exists to guarantee.
    """
    cached_resolved = cached_resolved or {}
    numbers = sorted(known_numbers.keys())
    width = detect_padding_width([(n, known_numbers[n]["identifier"][len(prefix):]) for n in numbers])
    hi = numbers[-1]

    found = dict(known_numbers)
    settled = {n: e for n, e in cached_resolved.items() if _is_settled(e) and n <= hi}
    for n, e in settled.items():
        if isinstance(e, dict) and n not in found:
            found[n] = e

    to_probe = [n for n in range(1, hi + 1) if n not in found and n not in settled]
    # Numbers resolved from the cache instead of a fresh probe, for the summary print.
    cache_hits = sum(1 for n in range(1, hi + 1) if n not in known_numbers and n in settled)

    unresolved = 0

    def probe(n):
        """
        Returns (n, data_or_None, ok). ok=False means the fetch FAILED, which
        is NOT the same as the item not existing -- a missing identifier
        returns HTTP 200 with an empty body (-> None, ok=True); only a
        network error or throttled request yields "FAIL". Collapsing those
        two silently shrinks the total.
        """
        ident = format_candidate_id(prefix, n, width)
        data = fetch_metadata(ident)
        if data == "FAIL":
            return (n, None, False)
        if data is None:
            return (n, None, True)
        md = data.get("metadata", {})
        return (n, {"identifier": ident, "repub_state": md.get("repub_state"), "publicdate": md.get("publicdate")}, True)

    if to_probe:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, n) for n in to_probe]
            for fut in as_completed(futures):
                n, data, ok = fut.result()
                if not ok:
                    unresolved += 1
                    continue
                if data:
                    found[n] = data
                    if _is_settled(data):
                        settled[n] = data
                else:
                    settled[n] = None  # confirmed absent -- safe to cache

    # Extend past the highest known number in parallel batches, stop once a
    # whole batch misses. This is NEVER skipped by the cache -- it's how a
    # genuinely new hidden item beyond the known ceiling gets caught.
    n = hi + 1
    for _ in range(max_extra_batches):
        batch = list(range(n, n + batch_size))
        hits = 0
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = [ex.submit(probe, b) for b in batch]
            for fut in as_completed(futures):
                bn, data, ok = fut.result()
                if not ok:
                    unresolved += 1
                    continue
                if data:
                    found[bn] = data
                    hits += 1
                    if _is_settled(data):
                        settled[bn] = data
                else:
                    settled[bn] = None
        n += batch_size
        if hits == 0:
            break

    return found, settled, cache_hits, unresolved


def find_enumerable_candidates(indexed_by_code):
    """Detect shiptracking codes whose identifiers show a clean prefix+number pattern."""
    candidates = {}
    for code, entries in indexed_by_code.items():
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
            numbers[int(m.group(2))] = {"identifier": e["identifier"], "repub_state": e.get("repub_state"), "publicdate": e.get("publicdate")}
        lo, hi = min(numbers), max(numbers)
        span = hi - lo + 1
        ratio = span / len(numbers)
        if lo <= 5 and ratio <= 8:
            candidates[code] = (prefix, numbers)
    return candidates


def _load_json_map(path, value_type, label):
    """
    Load a {key: value} JSON map, skipping "_"-prefixed keys.

    The shipped example files carry their documentation in "_comment"/
    "_example" keys. Without this skip, copying an example file as-is (which
    is exactly what SKILL.md Step 4 tells you to do) crashes on the first run
    with "TypeError: string indices must be integers", and "_example" would
    otherwise be enumerated as if it were a real shipment.
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


def load_seeds():
    return _load_json_map(SEEDS_PATH, dict, "seed")


def load_names():
    """Optional {CODE: "Friendly name"} map -- see shipment_names.example.json."""
    return _load_json_map(NAMES_PATH, str, "name")


def load_enum_cache():
    """{code: {"n": entry_or_None, ...}} of conclusively settled numbers as of the last successful run."""
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


# ---------- Aggregation ----------

def build_shipments_data(items, seeds=None):
    if seeds is None:
        seeds = load_seeds()

    indexed_by_code = defaultdict(list)
    for it in items:
        code = it.get("shiptracking")
        if code:
            indexed_by_code[code].append(it)

    candidates = find_enumerable_candidates(indexed_by_code)

    for code, seed in seeds.items():
        if code not in candidates:
            prefix = seed["prefix"]
            seed_id = seed["seed_identifier"]
            data = fetch_metadata(seed_id)
            numbers = {}
            if data and data != "FAIL":
                md = data.get("metadata", {})
                numbers[seed["seed_number"]] = {"identifier": seed_id, "repub_state": md.get("repub_state"), "publicdate": md.get("publicdate")}
            else:
                print(f"  WARNING: seed identifier '{seed_id}' for {code} could not be fetched — skipping stub discovery for this shipment.")
            if numbers:
                candidates[code] = (prefix, numbers)

    print(f"Detected {len(candidates)} shiptracking codes eligible for stub discovery (incl. {len(seeds)} seeded).")

    names = load_names()
    if names:
        print(f"Loaded {len(names)} friendly shipment name(s) from {NAMES_PATH}.")

    enum_cache = load_enum_cache()
    new_enum_cache = {}

    groups = {}
    total_unresolved = 0
    total_cache_hits = 0

    # Codes with a detectable/seedable pattern: enumerate the true full set.
    for code, (prefix, numbers) in candidates.items():
        cached = {int(k): v for k, v in enum_cache.get(code, {}).items()}
        cache_note = f" ({len(cached)} settled in cache)" if cached else ""
        print(f"  enumerating {code} (prefix={prefix}){cache_note}...", flush=True)
        full, settled, cache_hits, unresolved = enumerate_full_shipment(prefix, numbers, cached_resolved=cached)
        new_enum_cache[code] = {str(n): e for n, e in settled.items()}
        total_cache_hits += cache_hits
        total_unresolved += unresolved
        if unresolved:
            print(f"    WARNING: {unresolved} identifier probe(s) for {code} could not be "
                  f"resolved (network error or throttling) -- this row may undercount.", flush=True)
        total = len(full)
        # "completed" is deliberately NOT sum(1 for v in full.values() if repub_state == 19).
        # `full` includes items recovered by direct metadata probing (or, for a seeded code,
        # found via a single seeded identifier with no search results at all), which can be
        # complete (repub_state 19) before archive.org's search index has caught up with them
        # -- a lag of a few days is normal. A partner clicking the shiptracking:<code> search
        # link on the dashboard would then see fewer items than "completed" claimed, with no
        # way to know why. Restricting to identifiers that are BOTH recognized by the
        # enumeration (in `full`) AND actually present in this run's live search results
        # (indexed_by_code) keeps "completed" equal to what that link shows right now -- and
        # never exceeds `total`, since a stray non-enumerable identifier search sometimes
        # returns for a code (e.g. a cover/index file with no trailing number) is excluded on
        # both sides. The item still gets counted as soon as archive.org reindexes it.
        full_identifiers = {v["identifier"] for v in full.values()}
        completed = sum(
            1 for e in indexed_by_code.get(code, [])
            if e.get("repub_state") == "19" and e.get("identifier") in full_identifiers
        )
        last_republish = None
        last_added = None
        for v in full.values():
            pd = parse_publicdate(v.get("publicdate"))
            if pd and (last_added is None or pd > last_added):
                last_added = pd
        # republisher_date is only present in the bulk index fields, not the per-item probe results
        for it in indexed_by_code.get(code, []):
            rd = parse_republisher_date(it.get("republisher_date"))
            if rd and (last_republish is None or rd > last_republish):
                last_republish = rd
        groups[code] = {
            "total": total,
            "completed": completed,
            "last_republish": last_republish,
            "last_added": last_added,
            "discovery": "enumerated",
            "unresolved": unresolved,
        }

    if total_cache_hits:
        print(f"  (cache avoided re-probing {total_cache_hits} already-settled number(s) this run)")

    # Everything else: indexed-only counts (search-based, may undercount hidden stubs).
    for code, entries in indexed_by_code.items():
        if code in groups:
            continue
        total = len(entries)
        completed = sum(1 for e in entries if e.get("repub_state") == "19")
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
            "last_republish": last_republish,
            "last_added": last_added,
            "discovery": "indexed-only",
            "unresolved": 0,
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_WINDOW_DAYS)

    active = []
    for code, g in groups.items():
        recent = (g["last_republish"] and g["last_republish"] >= cutoff) or (g["last_added"] and g["last_added"] >= cutoff)
        if recent:
            last_dates = [d for d in (g["last_republish"], g["last_added"]) if d]
            active.append({
                "code": code,
                "name": names.get(code),
                "total": g["total"],
                "completed": g["completed"],
                "discovery": g["discovery"],
                "unresolved": g["unresolved"],
                "last_activity": max(last_dates).strftime("%Y-%m-%d") if last_dates else None,
            })

    active.sort(key=lambda s: (s["completed"] / s["total"] if s["total"] else 0))

    total_items = sum(s["total"] for s in active)
    total_completed = sum(s["completed"] for s in active)

    return {
        "generated_note": "Snapshot of archive.org metadata for collection:allen_county, grouped by shiptracking, including stub items recovered via direct identifier discovery where possible",
        "active_window_days": ACTIVE_WINDOW_DAYS,
        "shipment_count": len(active),
        "total_items": total_items,
        "total_completed": total_completed,
        "unresolved_probes": total_unresolved,
        "shipments": active,
    }, new_enum_cache


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
    live URL with nobody in the loop.
    """
    problems = []

    if not data["shipment_count"] or not data["total_items"]:
        problems.append(
            f"result is empty (shipments={data['shipment_count']}, items={data['total_items']}) "
            "-- archive.org may be unreachable, or the collection query may be wrong"
        )

    if data.get("unresolved_probes", 0) > MAX_UNRESOLVED_PROBES:
        problems.append(
            f"{data['unresolved_probes']} identifier probes could not be resolved "
            f"(limit {MAX_UNRESOLVED_PROBES}) -- totals would undercount"
        )

    prev = previous_data(html)
    if prev and prev.get("total_items"):
        drop = 100.0 * (prev["total_items"] - data["total_items"]) / prev["total_items"]
        if drop > MAX_SHRINK_PCT:
            problems.append(
                f"total items fell {drop:.0f}% ({prev['total_items']:,} -> {data['total_items']:,}), "
                f"more than the {MAX_SHRINK_PCT}% limit"
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
    seeds = load_seeds()

    if "--full-scan" in sys.argv:
        print("--full-scan given: pulling the ENTIRE collection (the slow, exhaustive audit path).")
        items = fetch_all_items()
    else:
        discovered_codes = fetch_discovery_items(now)
        target_codes = discovered_codes | set(seeds.keys())
        items = fetch_items_for_codes(target_codes)

    if not items and not seeds:
        print("archive.org returned no items at all -- refusing to write. Nothing was changed.")
        sys.exit(1)

    data, new_enum_cache = build_shipments_data(items, seeds=seeds)
    snapshot_date = date.today().strftime("%B %-d, %Y")

    required_keys = {"active_window_days", "shipment_count", "total_items", "total_completed", "shipments"}
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
    # pollutes the enumeration cache with data from a snapshot nobody
    # actually published.
    save_enum_cache(new_enum_cache)

    print()
    print("Done. Site data updated:")
    print(f"  Active shipments (last {ACTIVE_WINDOW_DAYS} days): {data['shipment_count']}")
    print(f"  Items completed / total: {data['total_completed']:,} / {data['total_items']:,}")
    print(f"  Snapshot date: {snapshot_date}")
    for s in data["shipments"]:
        flag = "" if s["discovery"] == "enumerated" else "  (indexed-only, may undercount stubs)"
        if s.get("unresolved"):
            flag += f"  ({s['unresolved']} probe(s) unresolved)"
        label = f"{s['code']} - {s['name']}" if s.get("name") else s["code"]
        print(f"    {label:44s} {s['completed']:4d} / {s['total']:4d}{flag}")


if __name__ == "__main__":
    main()
