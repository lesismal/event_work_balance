#!/usr/bin/env python3
"""Fail when any Autobahn case did not pass.

Usage: check_report.py <index.json>...

Several reports, such as those of separate wstest runs, are checked as one.
"""
import json
import sys

PASSING = {"OK", "NON-STRICT", "INFORMATIONAL", "UNIMPLEMENTED"}
PASSING_CLOSE = {"OK", "INFORMATIONAL", "UNIMPLEMENTED"}

if len(sys.argv) < 2:
    sys.exit(__doc__)
report = {}
for path in sys.argv[1:]:
    with open(path) as f:
        for agent, cases in json.load(f).items():
            report.setdefault(agent, {}).update(cases)

failures = []
total = 0
for agent, cases in report.items():
    for case, result in sorted(cases.items(), key=lambda kv: [int(p) for p in kv[0].split(".")]):
        total += 1
        behavior = result.get("behavior")
        close = result.get("behaviorClose")
        if behavior not in PASSING or close not in PASSING_CLOSE:
            failures.append(f"{agent} {case}: behavior={behavior} behaviorClose={close}")

if total == 0:
    sys.exit("no Autobahn results found")
for line in failures:
    print(line)
print(f"{total - len(failures)}/{total} cases passed")
sys.exit(1 if failures else 0)
