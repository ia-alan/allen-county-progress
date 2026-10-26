#!/usr/bin/env python3
"""
Refresh index.html (the partner-facing page) with current shipment progress
from archive.org. This is the script the scheduled GitHub Action runs, and
you can run it by hand too:

    python3 update_site.py                 # normal run
    python3 update_site.py --full-scan     # audit: pull the whole collection, re-check empty slots
    python3 update_site.py --recheck-absent
    python3 update_site.py --force         # publish a result the safety checks refused (read why first)

It needs shipment_core.py (the shared engine -- fetching, hidden-item
recovery, the manifest, caching, retries) in the same folder, plus
index.html, the manifest, and the two cache/history JSON files. Everything it
needs is in the site repo; nothing depends on a local machine.

Which shipments appear
----------------------
A shipment is "active" if it had a completion or a newly added item in the
last ACTIVE_WINDOW_DAYS and is not yet complete -- or it is declared in the
manifest and not closed (so a shipment that has been received but has nothing
scanned yet does not just vanish). A shipment that reaches completed == total
is recorded in COMPLETED_HISTORY_PATH and drops out of Active; the page shows
only the COMPLETED_DISPLAY_COUNT most recent completions. The run in which a
shipment's last item finishes still shows it in Active once more, flagged
"finalizing".

A completed record is refreshed on every run while the shipment is still
inside the activity window, and a shipment recorded as complete that turns out
to be bigger (more items arrive) is reopened -- the first snapshot is not
frozen in forever.

Is a total trustworthy?
-----------------------
Search only shows FINISHED items, so a shipment whose total is only "what
search shows" reads 100% at every step of scanning. Each shipment therefore
carries total_confirmed: true when the total comes from enumeration or a
packing-list count in the manifest, false when it is only search-visible. An
unconfirmed shipment is not called complete until it has been quiet for
UNCONFIRMED_QUIET_DAYS, and the page labels it "still receiving items".

Everything in the HTML outside the single-line `const SHIPMENTS = {...};` and
`const SNAPSHOT_DATE = "...";` statements is left untouched.

See the skill's SKILL.md and README.md before adapting the CONFIG block below
to a new partner/collection.
"""

import json
import sys
from datetime import date, datetime, timezone, timedelta

import shipment_core as core
from shipment_core import load_manifest

# ============================== CONFIG ======================================
# Adjust these for a new partner/collection. Everything below this block is
# domain-generic and should not need to change.

SITE_PATH = "index.html"                              # the HTML file this script rewrites
MANIFEST_PATH = "allen_county_shipments.json"
ENUM_CACHE_PATH = "allen_county_enum_cache.json"
COMPLETED_HISTORY_PATH = "allen_county_completed_history.json"
COMPLETED_DISPLAY_COUNT = 7                           # only the N most recently completed shipments are shown

COLLECTION_QUERY = "collection:allen_county"
GROUPING_FIELD = "shiptracking"
COMPLETE_FIELD = "repub_state"
COMPLETE_VALUE = "19"

ACTIVE_WINDOW_DAYS = 90
UNCONFIRMED_QUIET_DAYS = 14          # a shipment whose total is only what search shows isn't called complete until it's been this quiet
STALE_DECLARED_DAYS = 120            # a declared shipment with zero items this long after `received` gets flagged (still shown)

# Guard rails for the write step (see sanity_check below).
MAX_SHRINK_PCT = 40                  # refuse to publish if OBSERVED items drop more than this vs the current file
MAX_DISPLAYED_SHRINK_PCT = 70        # much looser check on the manifest-inflated displayed total -- catches a gross manifest typo without tripping on a legitimate recount
MAX_UNRESOLVED_PROBES = 10           # refuse to publish if more than this many identifier probes failed (flat floor)
MAX_UNRESOLVED_PROBE_RATE = 0.05     # ...or more than this fraction of all probes made this run, whichever is larger
# =============================================================================

core.configure(
    COLLECTION_QUERY=COLLECTION_QUERY, GROUPING_FIELD=GROUPING_FIELD,
    COMPLETE_FIELD=COMPLETE_FIELD, COMPLETE_VALUE=COMPLETE_VALUE,
    ACTIVE_WINDOW_DAYS=ACTIVE_WINDOW_DAYS, STALE_DECLARED_DAYS=STALE_DECLARED_DAYS,
    MANIFEST_PATH=MANIFEST_PATH, ENUM_CACHE_PATH=ENUM_CACHE_PATH,
    USER_AGENT="allen-county-site-update/1.0",
)


# ---------- Completed-shipment history ----------

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


# ---------- Aggregation ----------

def build_shipments_data(items, manifest, prev_shipments=None):
    prev_by_code = {s["code"]: s for s in (prev_shipments or [])}

    built = core.build_groups(items, manifest)
    groups = built["groups"]
    new_enum_cache = built["enum_cache"]
    manifest_warnings = built["warnings"]
    total_unresolved = built["unresolved"]
    hard_block = built["hard_block"]

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACTIVE_WINDOW_DAYS)
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DECLARED_DAYS)
    quiet_cutoff = datetime.now(timezone.utc) - timedelta(days=UNCONFIRMED_QUIET_DAYS)

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
        if prev.get("total") and prev["completed"] >= prev["total"] and prev.get("total_confirmed") is not False:
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
        # Likewise, a shipment with a packing-list count it hasn't reached yet is
        # still in progress no matter how long the scanners have paused on it.
        is_declared_unfinished = bool(m.get("expected_items")) and not m.get("closed") and g["completed"] < g["total"]

        if not (recent or is_declared_no_activity or is_declared_unfinished):
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
        # Search only ever shows finished items, so a shipment with no packing-
        # list count and no number pattern always reads 100% -- even mid-scan.
        # Don't call it complete until new items have stopped arriving.
        quiet = not last_dates or max(last_dates) < quiet_cutoff
        is_complete = g["total"] > 0 and g["completed"] >= g["total"] and (g["total_confirmed"] or quiet)

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
            "received": m.get("received"),
        }

        if not is_complete:
            # A shipment recorded as complete earlier can turn out to be bigger
            # than it looked (more items scanned since, or an expected_items
            # added to the manifest). Take it back out of history -- otherwise it
            # would show in both lists, and the stale "done" row would win once
            # it ages out of Active.
            if completed_history.pop(code, None) is not None:
                print(f"  {code}: was recorded complete but is now {g['completed']}/{g['total']} -- reopened.")
            active.append(row)
            continue

        if code in completed_history:
            # Already recorded as complete in a prior run. Refresh its numbers
            # every run it's still active: a shipment whose items each finish
            # before search sees the next one (common for title-named batches
            # with no number pattern) reads 100% at every step, so the first
            # snapshot -- often 1/1 -- would otherwise be frozen in forever.
            h = completed_history[code]
            if (h.get("total"), h.get("completed")) != (g["total"], g["completed"]):
                print(f"  {code}: completed record updated {h.get('completed')}/{h.get('total')} "
                      f"-> {g['completed']}/{g['total']}")
            h.update({
                "total": g["total"],
                "completed": g["completed"],
                "total_confirmed": g["total_confirmed"],
                "discovery": g["discovery"],
                "pages_completed": g["pages_completed"],
                "completed_date": max(h.get("completed_date") or "", last_activity or ""),
            })
            continue

        # First time this code is seen at completed==total: record it permanently.
        completed_history[code] = {
            "name": m.get("name"),
            "total": g["total"],
            "completed": g["completed"],
            "total_confirmed": g["total_confirmed"],
            "discovery": g["discovery"],
            "pages_completed": g["pages_completed"],
            "completed_date": last_activity or date.today().strftime("%Y-%m-%d"),
        }

        prev = prev_by_code.get(code)
        was_in_progress_before = prev is not None and prev.get("total") and (
            prev["completed"] < prev["total"] or prev.get("total_confirmed") is False)
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
            "total_confirmed": h.get("total_confirmed", h.get("discovery") != "indexed-only"),
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
        "generated_note": f"Snapshot of archive.org metadata for {COLLECTION_QUERY}, grouped by {GROUPING_FIELD}, including stub items recovered via direct identifier discovery or manifest declaration where possible",
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

def sanity_check(html, data, force=False, quiet_period=False):
    """
    Refuse to overwrite a working page with implausible data.

    An archive.org outage, a throttled run, or a typo'd collection query all
    produce a perfectly well-formed result with every key present and zeroes
    in it -- which the scheduled Action would then commit and push to the
    partner's live URL with nobody in the loop. This keys off
    total_observed_items rather than the manifest-inflated total_items so a
    declared shipment can never blind the guard.

    quiet_period means the run confirmed the collection still has items but
    nothing was recently active -- the normal state once every shipment has
    finished. That is not an outage, so an empty active list and a drop in
    observed items are allowed then; the displayed-total check still applies.
    """
    problems = []

    if not data.get("total_observed_items") and not quiet_period:
        problems.append(
            f"nothing was observed at all (observed items={data.get('total_observed_items')}) -- "
            "archive.org may be unreachable, or the collection query may be wrong"
        )

    unresolved_limit = max(MAX_UNRESOLVED_PROBES, int(MAX_UNRESOLVED_PROBE_RATE * data.get("total_probes", 0)))
    if data.get("unresolved_probes", 0) > unresolved_limit:
        problems.append(
            f"{data['unresolved_probes']} identifier probes could not be resolved "
            f"(limit {unresolved_limit} of {data.get('total_probes', 0)} probes this run) -- "
            "totals would undercount"
        )

    prev = core.previous_const(html, "SHIPMENTS")
    if prev:
        prev_observed = prev.get("total_observed_items", prev.get("total_items"))
        if prev_observed and not quiet_period:
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
    core.require_filled_in(globals())
    try:
        with open(SITE_PATH, encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        print(f"Could not find {SITE_PATH} in the current directory.")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    manifest = load_manifest()
    declared_codes = {code for code, m in manifest.items() if m.get("identifier_prefix")}
    # Codes with a packing-list count are fetched too, so one that has paused
    # past the activity window still gets its items counted.
    counted_codes = {code for code, m in manifest.items() if m.get("expected_items") and not m.get("closed")}

    full_scan = "--full-scan" in sys.argv
    if full_scan or "--recheck-absent" in sys.argv:
        core.configure(FORCE_RECHECK_ABSENT=True)

    items, failed = core.load_items(now, extra_codes=declared_codes | counted_codes, full_scan=full_scan)
    if failed:
        print()
        print(f"REFUSING TO WRITE -- could not fetch the items for: {', '.join(failed)}.")
        print("Publishing without them would show a wrong number. Nothing was changed; try again shortly.")
        sys.exit(1)
    if not core.check_not_empty(items, bool(declared_codes)):
        sys.exit(1)
    quiet_period = not items

    prev = core.previous_const(html, "SHIPMENTS")
    data, new_enum_cache, new_completed_history, hard_block = build_shipments_data(
        items, manifest, prev_shipments=(prev or {}).get("shipments", [])
    )

    if hard_block:
        print()
        print("REFUSING TO WRITE -- a manifest-declared identifier_prefix is wrong (it collided with a "
              "DIFFERENT shipment's real items, or differs from the real identifiers only in "
              f"capitalization). This cannot be overridden with --force; fix the prefix in {MANIFEST_PATH} "
              "first. See the manifest warning(s) above.")
        sys.exit(1)

    snapshot_date = date.today().strftime("%B %-d, %Y")

    required_keys = {"active_window_days", "shipment_count", "total_items", "total_completed", "shipments", "completed_shipments"}
    missing = required_keys - data.keys()
    if missing:
        print(f"Refusing to write site: built data is missing expected keys: {sorted(missing)}")
        sys.exit(1)

    if not sanity_check(html, data, force="--force" in sys.argv, quiet_period=quiet_period):
        sys.exit(1)

    new_html = core.inject_const(html, "SHIPMENTS", data, snapshot_date, SITE_PATH)

    with open(SITE_PATH, "w", encoding="utf-8") as f:
        f.write(new_html)

    # Deferred until AFTER a successful write, so a refused/failed run never
    # pollutes the enumeration cache (or the completed-shipment history) with
    # data from a snapshot nobody actually published.
    core.save_enum_cache(new_enum_cache)
    save_completed_history(new_completed_history)

    print()
    print("Done. Page updated:")
    print(f"  Active shipments (last {ACTIVE_WINDOW_DAYS} days): {data['shipment_count']}")
    print(f"  Items completed / total: {data['total_completed']:,} / {data['total_items']:,}")
    print(f"  Snapshot date: {snapshot_date}")
    for s in data["shipments"]:
        flag = "" if s["total_confirmed"] else "  (total not confirmed -- may undercount)"
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
    print()


if __name__ == "__main__":
    main()
