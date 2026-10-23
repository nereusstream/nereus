#!/usr/bin/env python3
"""Continue only the approved successful v5 prefix, preserving its original execution/input identities."""

import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('m5_continuation_runner', ROOT / 'scripts/run-v2-m5-evidence.py')
R = importlib.util.module_from_spec(spec)
spec.loader.exec_module(R)
C = R.C
PREPARATION = C.module('m5_continuation_preparation', 'm5-native-build-preparation.py')


def prepare_postprocessing_source(prior_prepared, output, reader_only_eb=False):
    """Capture new postprocessing source with the original, byte-identical execution dependencies."""
    I = C.INPUTS
    prior = I.verify(prior_prepared)
    roots = {name: Path(repo['sourceRoot']) for name, repo in prior['repositories'].items()}
    current = {name: I.repository(path) for name, path in roots.items()}
    value = copy.deepcopy(prior)
    value['repositories'] = current
    value['inputRootSha256'] = I.digest(I.canonical({key: value[key] for key in
        ('repositories', 'extraInputs', 'inputManifests')}))
    if reader_only_eb:
        C.module('m5_reader_only_preparation', 'm5-reader-only-evidence.py').check_inputs(prior, value, C)
    else:
        C.EQUIVALENCE.check_native_postprocessing(prior, value, C)
    output = output.resolve()
    if reader_only_eb:
        C.require(output.parent == prior_prepared.resolve().parent and 'build' not in output.parts
                  and not output.exists() and not output.is_symlink()
                  and all(not output.is_relative_to(source) for source in roots.values()),
                  'eb reader preparation requires a new sibling outside source/build directories')
    else:
        C.require(output.is_relative_to(ROOT / 'build') and not output.exists(),
                  'postprocessing preparation requires a new owned build destination')
    output.mkdir(parents=True)
    for name, source in roots.items():
        workspace = output / 'workspace' / I.LAYOUT[name]
        workspace.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(['git', 'clone', '--local', '--no-checkout', str(source), str(workspace)],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        I.git(workspace, 'read-tree', current[name]['baseCommit'])
        for path, expected in current[name]['files'].items():
            actual, raw = I.node(source, path)
            C.require(actual == expected, 'source changed during postprocessing preparation')
            if raw is None:
                continue
            I.store(output, raw)
            target = workspace / path
            target.parent.mkdir(parents=True, exist_ok=True)
            if expected['mode'] == '120000':
                target.symlink_to(os.fsdecode(raw))
            else:
                I.write_new(target, raw)
                target.chmod(0o755 if expected['mode'] == '100755' else 0o644)
    for path, node in value['extraInputs'].items():
        raw = I.regular(prior_prepared / 'objects' / node['sha256'])
        I.store(output, raw)
        for name, source in roots.items():
            if Path(path).is_relative_to(source):
                relative = Path(path).relative_to(source).as_posix()
                if relative not in current[name]['files']:
                    I.write_new(output / 'workspace' / I.LAYOUT[name] / relative, raw)
    C.require(current == {name: I.repository(path) for name, path in roots.items()},
              'source changed during postprocessing capture')
    I.write_new(output / 'input-closure.json', I.canonical(value))
    C.require(I.verify(output) == value, 'postprocessing source closure does not independently verify')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prior-input-closure', type=Path, required=True)
    parser.add_argument('--prior-package', type=Path, required=True)
    parser.add_argument('--input-closure', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--kafka-java-home', type=Path, required=True)
    parser.add_argument('--maven-repository', type=Path, required=True)
    parser.add_argument('--native-input-closure', type=Path)
    parser.add_argument('--native-package', type=Path)
    args = parser.parse_args()
    prepared = args.input_closure.resolve(strict=True)
    inputs = C.INPUTS.verify(prepared)
    prior_prepared = args.prior_input_closure.resolve(strict=True)
    prior = C.INPUTS.verify(prior_prepared)
    C.require(bool(args.native_input_closure) == bool(args.native_package), 'Native origin arguments must be paired')
    native_prepared = args.native_input_closure.resolve(strict=True) if args.native_input_closure else prepared
    native_inputs = C.INPUTS.verify(native_prepared)
    native_delta_sources = C.EQUIVALENCE.check_native_postprocessing(native_inputs, inputs, C) if args.native_package else []
    delta = C.EQUIVALENCE.check_inputs(prior, inputs, C)
    C.require(ROOT == (prepared / 'workspace' / C.INPUTS.LAYOUT['nereus']).resolve(),
              'continuation must execute the actual final prepared workspace')
    old_package = args.prior_package.resolve(strict=True)
    package = args.output.resolve()
    package.mkdir(parents=True, exist_ok=False)
    original = []
    executions = {}
    reused = C.EQUIVALENCE.REUSED_V5_STAGES if args.native_package else C.EQUIVALENCE.STAGES
    for stage in reused:
        path = f'executions/{stage}/record.json'
        raw = C.read(old_package, path)
        expected_bytes, expected_sha = C.EQUIVALENCE.ORIGINAL_RECORDS[stage]
        C.require((len(raw), C.INPUTS.digest(raw)) == (expected_bytes, expected_sha), 'v5 receipt changed')
        record = C.load(raw)
        executions[stage] = C.validate_execution(old_package, record, prior)
        shutil.copytree(old_package / 'executions' / stage, package / 'executions' / stage)
        original.append({'stage': stage, 'record': R.reference(package, path)})
    native_original = []
    if args.native_package:
        native_package = args.native_package.resolve(strict=True)
        for stage in C.EQUIVALENCE.NATIVE_STAGES:
            path = f'executions/{stage}/record.json'
            record = C.load(C.read(native_package, path))
            executions[stage] = C.validate_execution(native_package, record, native_inputs)
            shutil.copytree(native_package / 'executions' / stage, package / 'executions' / stage)
            native_original.append({'stage': stage, 'record': R.reference(package, path)})
    prefix = 'source-continuation'
    prior_ref = R.write(package, prefix + '/prior-input-closure.json', prior)
    objects = []
    prior_sources = {}
    final_sources = []
    for row in delta:
        node = row['before']
        if node is None or node == {'deleted': True} or any(item['sha256'] == node['sha256'] for item in objects):
            continue
        source = prior_prepared / 'objects' / node['sha256']
        target = package / prefix / 'previous-source' / node['sha256']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        objects.append(R.reference(package, target.relative_to(package).as_posix()))
    for row in delta:
        if row['before'] is not None and row['before'] != {'deleted': True}:
            prior_sources[row['path']] = C.INPUTS.regular(prior_prepared / 'objects' / row['before']['sha256'])
        node = row['after']
        if node is None or node == {'deleted': True}:
            continue
        target = package / prefix / 'final-source' / node['sha256']
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(prepared / 'objects' / node['sha256'], target)
        final_sources.append({'repository': row['repository'], 'path': row['path'],
                              'object': R.reference(package, target.relative_to(package).as_posix())})
    native_delta = C.NATIVE_INPUTS.check_inventory(prior, inputs, C)
    build_prefix = prefix + '/native-build'
    shutil.copytree(native_prepared / 'preparation', package / build_prefix / 'preparation')
    preparation_record = native_prepared / 'workspace/GITHUB/nereus' / PREPARATION.RECORD
    shutil.copyfile(preparation_record, package / build_prefix / 'record.json')
    build_ref = R.reference(package, build_prefix + '/record.json')
    pulsar_ref = None
    if args.native_package:
        pulsar_prefix = prefix + '/pulsar-build'
        shutil.copytree(prepared / 'pulsar-preparation', package / pulsar_prefix / 'pulsar-preparation')
        shutil.copyfile(ROOT / C.NATIVE_BUILD.PULSAR_RECORD, package / pulsar_prefix / 'record.json')
        pulsar_ref = R.reference(package, pulsar_prefix + '/record.json')
    native_objects = []
    for row in native_delta:
        item = {'path': row['path']}
        for key, cohort in (('before', prior_prepared), ('after', prepared)):
            node = row[key]
            if node is None:
                item[key] = None
                continue
            shared = package / build_prefix / 'preparation/raw-objects' / node['sha256']
            if key == 'after' and shared.is_file():
                item[key] = R.reference(package, shared.relative_to(package).as_posix())
                continue
            if key == 'after' and node['sha256'] == build_ref['sha256']:
                item[key] = build_ref
                continue
            if key == 'after' and pulsar_ref is not None:
                shared = package / pulsar_prefix / 'pulsar-preparation/raw-objects' / node['sha256']
                if shared.is_file():
                    item[key] = R.reference(package, shared.relative_to(package).as_posix())
                    continue
                if node['sha256'] == pulsar_ref['sha256']:
                    item[key] = pulsar_ref
                    continue
            target = package / prefix / 'native-inputs' / node['sha256']
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(cohort / 'objects' / node['sha256'], target)
            item[key] = R.reference(package, target.relative_to(package).as_posix())
        native_objects.append(item)
    original_source = Path(C.read(old_package, 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').decode().strip())
    C.require(original_source.is_relative_to(prior_prepared / 'workspace/GITHUB/nereus/build/m5-kafka-run-source'),
              'original source output is not owned by the approved v5 execution')
    source = PREPARATION.qualify_source(old_package, prior_prepared, ROOT, inputs)
    relocated = []
    for path in sorted(source.rglob('*')):
        if not path.is_file():
            continue
        C.require(not path.is_symlink(), 'relocated source input is a symlink')
        relative = path.relative_to(source).as_posix()
        target = package / prefix / 'source-inputs' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        relocated.append({'relativePath': relative, 'archive': R.reference(package, target.relative_to(package).as_posix())})
    # The inherited legacy-run-path remains truthful. The original raw legacy directory is retained;
    # the native driver independently compares every legacy source against the final complete cohort.
    legacy = Path((source / 'legacy-run-path.txt').read_text().strip())
    C.require(legacy.is_relative_to(prior_prepared / 'workspace/GITHUB/nereus/build/m5-bookkeeper-task-terminal'),
              'legacy qualification origin differs')
    command = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
               'v2M5EvidenceContractTest', 'v2DocumentationCheck']
    C.require(C.INPUTS.verify(prepared) == inputs, 'final source changed before supplemental verification')
    started = time.time_ns() // 1000
    log_path = prefix + '/verification.log'
    with (package / log_path).open('x') as log:
        code = subprocess.run(command, cwd=ROOT, env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT).returncode
        log.write(f'\nM5_CONTINUATION_VERIFICATION_EXIT={code}\n')
    finished = time.time_ns() // 1000
    C.require(code == 0 and C.INPUTS.verify(prepared) == inputs, 'final supplemental verification failed')
    proof = {'schema': 'NEREUS_V2_M5_STAGE_INPUT_EQUIVALENCE_V4' if args.native_package else
                      'NEREUS_V2_M5_STAGE_INPUT_EQUIVALENCE_V3', 'kind': 'INPUT_EQUIVALENCE_ONLY',
             'promotionEligible': False, 'executionEvidence': False, 'oldInputRootSha256': prior['inputRootSha256'],
             'finalInputRootSha256': inputs['inputRootSha256'], 'priorInputs': prior_ref, 'differences': delta,
             'priorSourceObjects': objects, 'originalExecutions': original,
             'finalSourceObjects': final_sources, 'nativeInputDifferences': native_delta, 'nativeInputObjects': native_objects,
             'nativeBuildPreparation': build_ref,
             'observerChange': C.EQUIVALENCE.check_direct_calls(prior_sources, inputs, C, bool(args.native_package)),
             'directCallSources': {path: C.source_node(prior, 'nereus', path) for path in sorted(C.EQUIVALENCE.CALL_PATHS)},
             'verification': {'inputRootSha256': inputs['inputRootSha256'], 'inputBeforeSha256': inputs['inputRootSha256'],
                 'inputAfterSha256': inputs['inputRootSha256'], 'command': command, 'exitCode': code,
                 'startedAtMicros': started, 'finishedAtMicros': finished, 'log': R.reference(package, log_path)},
             'sourceRelocations': [{'originalDirectory': str(original_source), 'finalDirectory': str(source), 'files': relocated}]}
    if args.native_package:
        native_sources, native_objects = {}, []
        for row in native_delta_sources:
            node = row['before']
            raw = C.INPUTS.regular(native_prepared / 'objects' / node['sha256'])
            native_sources[row['path']] = raw
            target = package / prefix / 'native-origin/source' / node['sha256']
            if not target.exists():
                C.INPUTS.write_new(target, raw)
                native_objects.append(R.reference(package, target.relative_to(package).as_posix()))
        final_raw = {row['path']: C.INPUTS.regular(prepared / 'objects' / row['after']['sha256']) for row in delta
                     if row['after'] is not None and row['after'] != {'deleted': True}}
        proof.update(nativePriorInputs=R.write(package, prefix + '/native-origin/input-closure.json', native_inputs),
                     nativeDifferences=native_delta_sources, nativePriorSourceObjects=native_objects,
                     nativeOriginalExecutions=native_original,
                     nativeSourceBoundaries=C.EQUIVALENCE.native_boundaries(native_sources, final_raw, C),
                     pulsarBuildPreparation=pulsar_ref)
    proof_ref = R.write(package, prefix + '/binding.json', proof)
    C.EQUIVALENCE.validate(package, proof_ref, inputs, C)
    print('PASS_V5_STAGE_INPUT_EQUIVALENCE_ONLY; original three records preserved', flush=True)
    for stage in C.STAGES:
        if stage not in executions:
            executions[stage] = R.execute_stage(stage, package, prepared, inputs, args.kafka_java_home,
                                                 args.maven_repository, source)
    R.assemble(package, inputs, executions)
    value = C.load(C.read(package, 'candidate.json'))
    value['stageInputEquivalence'] = proof_ref
    (package / 'candidate.json').write_bytes(C.canonical(value))
    C.validate_candidate(package, prepared, ROOT)
    print('PASS_V2_M5_COMPLETE_CANDIDATE_AWAITING_BINDING', package, flush=True)


if __name__ == '__main__':
    main()
