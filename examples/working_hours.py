import json
import re
from datetime import datetime, timedelta, timezone, time
from typing import List, Tuple, Dict

from tabulate import tabulate

import aw_client
from aw_core import Event
from aw_transform import flood


def _pretty_timedelta(td: timedelta) -> str:
    s = str(td)
    s = re.sub(r"^(0+[:]?)+", "", s)
    s = s.rjust(len(str(td)), " ")
    s = re.sub(r"[.]\d+", "", s)
    return s


assert _pretty_timedelta(timedelta(seconds=120)) == "   2:00"
assert _pretty_timedelta(timedelta(hours=9, minutes=5)) == "9:05:00"


def generous_approx(events: List[dict], max_break: float) -> timedelta:
    """
    Returns a generous approximation of worked time by including non-categorized time when shorter than a specific duration

    max_break: Max time (in seconds) to flood when there's an empty slot between events
    """
    events_e: List[Event] = [Event(**e) for e in events]
    return sum(
        map(lambda e: e.duration, flood(events_e, max_break)),
        timedelta(),
    )


def query():
    td1d = timedelta(days=1)
    day_offset = timedelta(hours=4)

    now = datetime.now(tz=timezone.utc)
    # TODO: Account for timezone, or maybe it's handled correctly by aw_client?
    today = datetime.combine(now.date(), time()) + day_offset

    timeperiods = [(today - i * td1d, today - (i - 1) * td1d) for i in range(5)]
    timeperiods.reverse()

    categories: List[Tuple[List[str], Dict]] = [
        (
            ["Work"],
            {
                "type": "regex",
                "regex": r"activitywatch|algobit|defiarb|github.com",
                "ignore_case": True,
            },
        )
    ]

    aw = aw_client.ActivityWatchClient()

    # TODO: Move this query somewhere else, as the equivalent of aw-webui's 'canonicalEvents'
    res = aw.query(
        f"""
    window = flood(query_bucket(find_bucket("aw-watcher-window_")));
    afk = flood(query_bucket(find_bucket("aw-watcher-afk_")));
    events = filter_period_intersect(window, filter_keyvals(afk, "status", ["not-afk"]));
    events = categorize(events, {json.dumps(categories)});
    events = filter_keyvals(events, "$category", [["Work"]]);
    duration = sum_durations(events);
    RETURN = {{"events": events, "duration": duration}};
    """,
        timeperiods,
    )

    for break_time in [0, 5 * 60, 10 * 60, 15 * 60]:
        _print(
            timeperiods, res, break_time, {"category_rule": categories[0][1]["regex"]}
        )

    save = True
    if save:
        fn = "working_hours_events.json"
        with open(fn, "w") as f:
            print(f"Saving to {fn}...")
            json.dump(res, f, indent=2)


def _print(timeperiods, res, break_time, params: dict):
    print("Using:")
    print(f"  break_time={break_time}")
    print("\n".join(f"  {key}={val}" for key, val in params.items()))
    print(
        tabulate(
            [
                [
                    start.date(),
                    # Without flooding:
                    # _pretty_timedelta(timedelta(seconds=res[i]["duration"])),
                    # With flooding:
                    _pretty_timedelta(generous_approx(res[i]["events"], break_time)),
                    len(res[i]["events"]),
                ]
                for i, (start, stop) in enumerate(timeperiods)
            ],
            headers=["Date", "Duration", "Events"],
            colalign=(
                "left",
                "right",
            ),
        )
    )

    print(
        f"Total: {sum((generous_approx(res[i]['events'], break_time) for i in range(len(timeperiods))), timedelta())}"
    )
    print("")


if __name__ == "__main__":
    query()
