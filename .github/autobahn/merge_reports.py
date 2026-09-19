"""Merge the reports of several wstest fuzzingclient runs into one.

Each run writes a report whose index lists every Autobahn case, so the cases
that ran in the other runs show there as Missing. This copies every run's
per-case reports into one directory and has Autobahn's own report writer
build the index of them all from the results they hold.

It runs inside the crossbario/autobahn-testsuite image, under its Python 2:

    python merge_reports.py <outdir> <reportdir>...
"""
import json
import os
import shutil
import sys

from autobahntestsuite.fuzzing import (FuzzingFactory, CaseSet, CaseSetname, CaseBasename,
                                       Cases, CaseCategories, CaseSubCategories)


def plain(value):
    """Turns the unicode strings JSON decodes into the str the writer expects."""
    if isinstance(value, dict):
        return dict((plain(k), plain(v)) for k, v in value.items())
    if isinstance(value, list):
        return [plain(v) for v in value]
    if isinstance(value, unicode):
        return value.encode("utf-8")
    return value


def main(outdir, reportdirs):
    factory = FuzzingFactory(outdir)
    factory.CaseSet = CaseSet(CaseSetname, CaseBasename, Cases, CaseCategories, CaseSubCategories)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for reportdir in reportdirs:
        with open(os.path.join(reportdir, "index.json")) as f:
            index = json.load(f)
        for agent, cases in index.items():
            for case_id, summary in cases.items():
                report = os.path.join(reportdir, summary["reportfile"])
                with open(report) as f:
                    factory.logCase(plain(json.load(f)))
                # The case reports are already right, so they are copied as
                # they are rather than rewritten.
                for ext in ("json", "html"):
                    shutil.copy(os.path.splitext(report)[0] + "." + ext, outdir)
    factory.createMasterReportHTML(outdir)
    factory.createMasterReportJSON(outdir)
    count = sum(len(cases) for cases in factory.agents.values())
    print("merged %d case results from %d reports into %s" % (count, len(reportdirs), outdir))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
