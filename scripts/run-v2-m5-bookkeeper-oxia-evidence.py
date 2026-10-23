#!/usr/bin/env python3
"""Run the existing BK/Oxia source, retention, restart and permanent-capacity recipes once."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--input-closure', type=Path, required=True)
parser.add_argument('--class-logs', type=Path, required=True)
args = parser.parse_args()
spec = importlib.util.spec_from_file_location('m5_bk_inputs', ROOT / 'scripts/m5-candidate-inputs.py')
inputs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inputs)
cohort = inputs.verify(args.input_closure.resolve(strict=True))
assert ROOT == (args.input_closure.resolve() / 'workspace' / inputs.LAYOUT['nereus']).resolve()
args.output.mkdir(parents=True, exist_ok=False)
rows = {}
raw_directories = ('m5-kafka-run-source', 'm5-bookkeeper-task-terminal', 'm5-epoch-grace',
                   'm5-retired-history-oxia', 'm5-gc-quota-oxia')
existing = {path.resolve() for name in raw_directories for path in (ROOT / 'build' / name).glob('*')}
for recipe in ('kafka-run-source', 'retention-retirement', 'retired-history-oxia', 'gc-quota-oxia'):
    started = time.time_ns()
    with (args.output / (recipe + '.log')).open('w') as log:
        subprocess.run(['bash', 'scripts/run-v2-m5-' + recipe + '-check.sh'], cwd=ROOT,
                       env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, check=True)
    # Later recipes reuse some task output paths. Retain each current task's latest full raw suite,
    # before another recipe can overwrite it; never borrow a pre-execution XML.
    for module in ('nereus-storage-api', 'nereus-storage-object', 'nereus-storage-bookkeeper',
                   'nereus-kafka-bookkeeper', 'nereus-metadata-oxia'):
        for xml in sorted((ROOT / module / 'build/test-results').glob('*/TEST-*.xml')):
            if xml.stat().st_mtime_ns < started:
                continue
            relative = Path('junit') / module / xml.parent.name / xml.name
            target = args.output / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(xml, target)
            rows[str(relative)] = {'task': ':' + module + ':' + xml.parent.name,
                                   'path': str(relative), 'producedAtMicros': xml.stat().st_mtime_ns // 1000}
assert rows and inputs.verify(args.input_closure.resolve()) == cohort
for name in raw_directories:
    for run in sorted((ROOT / 'build' / name).glob('*')):
        if not run.is_dir() or run.resolve() in existing:
            continue
        for path in run.rglob('*'):
            if not path.is_file() or 'source-archive' in path.relative_to(run).parts:
                continue
            assert not path.is_symlink()
            target = args.output / 'native-archives' / name / run.name / path.relative_to(run)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
source_runs = sorted((ROOT / 'build/m5-kafka-run-source').glob('*/run-summary.json'),
                     key=lambda path: path.stat().st_mtime_ns)
assert source_runs
(args.output / 'current-source-run.txt').write_text(str(source_runs[-1].parent) + '\n')
(args.output / 'stage-result.json').write_text(json.dumps({'junit': list(rows.values()), 'exitCode': 0},
                                                        sort_keys=True, indent=2) + '\n')
