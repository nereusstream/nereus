#!/usr/bin/env python3
"""Finish only the reviewed eb cohort after its two aggregate-reader corrections."""

import argparse
import ast
import copy
import hashlib
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys


SCHEMA = 'NEREUS_V2_M5_EB_READER_INPUT_EQUIVALENCE_V1'
PRIOR_ROOT = 'eb378e578695a923a2fb0bfe070dcde5c6e7636e653e48a1fcbdd4f9b5d19d9e'
SCRIPT = 'scripts/m5-reader-only-evidence.py'
RECORDS = {
    'BOOKKEEPER_OXIA': (501377, 'a52702b32dbe02287c9a67833235c009eba08d84231320cb9f98dc43486dfb87'),
    'OBJECT': (77691, '9e3c90e0fe52d53846f183494c835be8273f0141d1ee421af169a4f545ef82c1'),
    'NATIVE_KAFKA': (111833, 'bd65c777ab3d9a6e2e823b3f7f08864e99597f2745bd38e09a61c095f8e1cb4a'),
    'NATIVE_PULSAR': (312602, '2c65d65936e288115292d98822ea9e8742d30f016ab8c0930bc4a2b69d047cb2'),
}
# Exact reviewed source versions, filled before the successor is captured. This is not a filename allowance.
SOURCE_HASHES = {
    'scripts/check-v2-m5-evidence.py': ('51233f4797fcb80ef88041ca0e4c63a47a352d0800dfea521424552062fa30b7', '6cda8f8392e4db2537b514374838a9de63c3f2c50709f1271c5c1a848116c366'),
    'scripts/check-v2-m5-evidence-tests.py': ('f18386e7eee9c79aeada08ce8eabf133056927191c7905344c2cf8ecc98feb60', 'afc46f530d4aa734b64f06a65ffd00f8a85063357160bc101deac5e2495bb06f'),
    'scripts/continue-v2-m5-evidence.py': ('90ce9860d0ebc95302f9cb8431024c027b91445dbd27f94effde93ed336ca417', '42e2a006f6c896f9919f3e3e3ede98dadeade0a36536ca5080ef9ce54ca5732a'),
    'scripts/m5-stage-input-equivalence.py': ('fec44a4cfa7122c867b2bce5222c80718c6716cf2dc23fc752467cc6a373454e', 'f78999390e62c0995311179685138f0128a3d05e22797aa583a7f19450223a2a'),
    'docs/v2/NSIP/nsip-1.md': ('2c3849ad038f04955ca57056e686e448d54336cff70a6a1805c01d1a43ede940', 'eea3f05ea378d4c778ec7d253fed6428b6883a2fa0e839008fd7650434a13c00'),
    'docs/v2/detailed_design/m5/m5-current-contracts.md': ('b5bd7cddea31936dd525ab6beb3059cdc79d1d6288d1ab732c84f482dc50edad', '843913cfa58ea17e027879f91207722e188b85207d41a785e658eaefa2c88a0b'),
    'docs/v2/detailed_design/m5/m5-implementation-log.md': ('f5036889ada7cc50e9d3139d7592de4bcbefa8c580f93d1164fe3593f3383821', '8fb987756e3bfa547d1c7b0603c6d0fe513ac818f995a149a789f772dcf31090'),
}


def check_inputs(prior, final, C):
    C.INPUTS.validate_value(prior)
    C.INPUTS.validate_value(final)
    C.require(prior['inputRootSha256'] == PRIOR_ROOT and final['inputRootSha256'] != PRIOR_ROOT,
              'reader handoff requires the exact original eb input and one successor')
    C.require(prior['extraInputs'] == final['extraInputs'] and prior['inputManifests'] == final['inputManifests'],
              'reader handoff changed a tool, dependency, product or dependency manifest')
    for name in C.INPUTS.REPOSITORIES:
        C.require(all(prior['repositories'][name][key] == final['repositories'][name][key]
                      for key in ('baseCommit', 'sourceRoot')), 'reader handoff changed repository identity')
    delta = C.EQUIVALENCE.differences(prior, final)
    C.require({(row['repository'], row['path']) for row in delta} ==
              {('nereus', path) for path in SOURCE_HASHES.keys() | {SCRIPT}},
              'reader handoff changed an actual product/test/build/driver source or omitted a reviewed change')
    for row in delta:
        before, after = row['before'], row['after']
        if row['path'] == SCRIPT:
            C.require(before is None and after is not None and after['mode'] == '100644'
                      and after['sha256'] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                      'reader handoff entry source differs from the executing reviewed entry')
        else:
            C.require(before is not None and after is not None
                      and before.get('mode') == after.get('mode') in ('100644', '100755')
                      and (before['sha256'], after['sha256']) == SOURCE_HASHES[row['path']],
                      'reader handoff source is not the exact reviewed correction: ' + row['path'])
        logical = str(Path(prior['repositories']['nereus']['sourceRoot']) / row['path'])
        C.require(logical not in prior['extraInputs'], 'reader handoff changed an executed dependency source')
    return delta


def check_boundaries(before, after, C):
    def boundary(raw, functions=(), classes=(), assignments=()):
        tree = ast.parse(raw)
        tree.body = [node for node in tree.body if not (
            isinstance(node, ast.FunctionDef) and node.name in functions
            or isinstance(node, ast.ClassDef) and node.name in classes
            or isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in assignments
                                                  for target in node.targets))]
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in functions:
                node.body = [ast.Pass()]
        return ast.dump(tree)
    checks = {}
    for path, functions, classes, assignments in (
        ('scripts/check-v2-m5-evidence.py', ('validate_current_m4', 'validate_capacity'), (),
         ('CURRENT_M4_ADDITIONAL_TESTS',)),
        ('scripts/check-v2-m5-evidence-tests.py',
         ('pulsar_quota_fixture', 'test_pulsar_budget_refund_readmission_and_final_observations_are_independent_requirements'),
         ('CurrentM4InventoryContracts', 'ReaderOnlyEquivalenceContracts'), ()),
        ('scripts/continue-v2-m5-evidence.py', ('prepare_postprocessing_source',), (), ()),
    ):
        old = boundary(before[path], functions, classes, assignments)
        new = boundary(after[path], functions, classes, assignments)
        C.require(old == new, 'reader correction changed an execution/collection boundary: ' + path)
        checks[path] = hashlib.sha256(old.encode()).hexdigest()
    path = 'scripts/m5-stage-input-equivalence.py'
    old, new = ast.parse(before[path]), ast.parse(after[path])
    old_validate = next(node for node in old.body if isinstance(node, ast.FunctionDef) and node.name == 'validate')
    new_validate = next(node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == 'validate')
    dispatch = ast.parse("if proof.get('schema') == '" + SCHEMA + "':\n"
        "    return C.module('m5_eb_reader_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)\n").body[0]
    C.require(ast.dump(new_validate.body[2]) == ast.dump(dispatch), 'reader dispatcher differs')
    new_validate.body.pop(2)
    C.require(ast.dump(old) == ast.dump(new) and ast.dump(old_validate) == ast.dump(new_validate),
              'reader handoff changed the old v5 continuation')
    checks[path] = hashlib.sha256(ast.dump(old).encode()).hexdigest()
    return checks


def check_record_identity(stage, reference, C):
    C.require(stage in RECORDS and reference['path'] == f'executions/{stage}/record.json'
              and (reference['bytes'], reference['sha256']) == RECORDS[stage],
              'reader handoff original record identity differs: ' + stage)


def check_runtime(record, final, C):
    for row in record['runtimeInputs']:
        path = Path(row['sourcePath'])
        expected = final['extraInputs'].get(str(path))
        for repo in final['repositories'].values():
            if path.is_relative_to(repo['sourceRoot']):
                expected = repo['files'].get(path.relative_to(repo['sourceRoot']).as_posix(), expected)
        C.require(expected is not None and all(row[key] == expected[key] for key in ('bytes', 'sha256')),
                  'reader handoff changed an actually loaded runtime input: ' + str(path))


def source_lineage(root, prior, final, C):
    source = C.read(root, 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').decode().strip()
    native = C.load(C.read(root, 'executions/NATIVE_KAFKA/record.json'))
    command = native['command']
    C.require('--source-run' in command and command[command.index('--source-run') + 1] == source,
              'reader handoff lost the original same-campaign Source input')
    prefix = 'executions/BOOKKEEPER_OXIA/raw/native-archives/m5-kafka-run-source/' + Path(source).name
    facts = C.INPUTS.load(C.read(root, 'executions/NATIVE_KAFKA/raw/inputs.json'))
    for name, field in (('run-summary.json', 'sourceSummarySha256'), ('tested-inputs.json', 'sourceManifestSha256')):
        C.require(hashlib.sha256(C.read(root, prefix + '/' + name)).hexdigest() == facts[field],
                  'reader handoff Source archive differs from the actual native input')
    manifest = C.INPUTS.load(C.read(root, prefix + '/tested-inputs.json'))
    for path, digest in manifest.items():
        C.require(C.source_node(prior, 'nereus', path) == C.source_node(final, 'nereus', path)
                  and C.source_node(final, 'nereus', path)['sha256'] == digest,
                  'reader handoff changed a Source qualification input')
    legacy = Path(C.read(root, prefix + '/legacy-run-path.txt').decode().strip()).name
    legacy_prefix = 'executions/BOOKKEEPER_OXIA/raw/native-archives/m5-bookkeeper-task-terminal/' + legacy
    for name, field in (('run-summary.json', 'legacySummarySha256'), ('tested-inputs.json', 'legacyManifestSha256')):
        raw = C.read(root, legacy_prefix + '/' + name)
        C.require(hashlib.sha256(raw).hexdigest() == facts[field], 'reader handoff Legacy source differs')
    C.require(C.read(root, prefix + '/legacy-regression-summary.json') ==
              C.read(root, legacy_prefix + '/run-summary.json'), 'Source does not retain its actual Legacy result')
    return source


def validate(root, reference, final, C):
    proof = C.load(C.resource(root, reference, 'source-continuation'))
    C.exact(proof, ('schema', 'kind', 'promotionEligible', 'executionEvidence', 'oldInputRootSha256',
        'finalInputRootSha256', 'priorInputs', 'differences', 'priorSourceObjects', 'finalSourceObjects',
        'boundaryAstSha256', 'originalExecutions', 'components', 'nativeSourceDirectory'), 'eb reader handoff')
    C.require(proof['schema'] == SCHEMA and proof['kind'] == 'INPUT_EQUIVALENCE_ONLY'
              and proof['promotionEligible'] is False and proof['executionEvidence'] is False
              and proof['oldInputRootSha256'] == PRIOR_ROOT
              and proof['finalInputRootSha256'] == final['inputRootSha256'], 'reader handoff scope differs')
    prior = C.load(C.resource(root, proof['priorInputs'], 'source-continuation'))
    delta = check_inputs(prior, final, C)
    C.require(proof['differences'] == delta, 'reader handoff source difference inventory differs')
    sources = []
    registered = {reference['path'], proof['priorInputs']['path']}
    for side, key in (('before', 'priorSourceObjects'), ('after', 'finalSourceObjects')):
        changes = [row for row in delta if row[side] is not None]
        C.require(len(proof[key]) == len(changes), 'reader handoff source bytes missing')
        objects = {}
        for item, change in zip(proof[key], changes):
            C.exact(item, ('repository', 'path', 'object'), 'reader source object')
            C.require((item['repository'], item['path']) == (change['repository'], change['path'])
                      and all(item['object'][field] == change[side][field] for field in ('bytes', 'sha256')),
                      'reader handoff source object identity differs')
            objects[item['path']] = C.resource(root, item['object'], 'source-continuation/' + side)
            registered.add(item['object']['path'])
        sources.append(objects)
    C.require(proof['boundaryAstSha256'] == check_boundaries(*sources, C), 'reader AST proof differs')
    C.require([row.get('stage') for row in proof['originalExecutions']] == list(RECORDS),
              'reader handoff must preserve exactly four original backend stages')
    finishes = []
    for item in proof['originalExecutions']:
        C.exact(item, ('stage', 'record'), 'reader original execution')
        check_record_identity(item['stage'], item['record'], C)
        record = C.load(C.resource(root, item['record']))
        C.require(record['stage'] == item['stage'] and record['inputRootSha256'] == PRIOR_ROOT,
                  'reader handoff relabeled an original execution')
        C.validate_execution(root, record, prior)
        check_runtime(record, final, C)
        finishes.append(record['finishedAtMicros'])
    C.require(proof['components']['path'] == 'executions/COMPONENTS/record.json', 'new COMPONENTS record missing')
    components = C.load(C.resource(root, proof['components']))
    C.require(components['stage'] == 'COMPONENTS' and components['inputRootSha256'] == final['inputRootSha256']
              and components['startedAtMicros'] > max(finishes), 'COMPONENTS did not run after the reader correction')
    C.validate_execution(root, components, final)
    log = C.read(root, 'executions/COMPONENTS/raw/gradle.log').decode()
    for task in ('v2M5EvidenceContractTest', 'v2M5CandidateInputsContractTest',
                 'v2M5EvidenceJUnitContractTest', 'v2DocumentationCheck'):
        C.require('> Task :' + task + '\n' in log, 'new reader verification did not execute: ' + task)
    source = source_lineage(root, prior, final, C)
    C.require(proof['nativeSourceDirectory'] == source, 'reader handoff changed the actual Source directory')
    C.require(C.inventory(root, 'source-continuation') == registered, 'reader handoff has unregistered files')
    return {'prior': prior, 'stages': tuple(RECORDS), 'origins': dict.fromkeys(RECORDS, prior),
            'nativeSourceDirectory': source, 'paths': registered, 'proof': proof}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prior-input-closure', type=Path, required=True)
    parser.add_argument('--prior-package', type=Path, required=True)
    parser.add_argument('--input-closure', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--kafka-java-home', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location('m5_reader_handoff_runner', root / 'scripts/run-v2-m5-evidence.py')
    R = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(R)
    C = R.C
    prior_prepared, old_package = args.prior_input_closure.resolve(strict=True), args.prior_package.resolve(strict=True)
    prior = C.INPUTS.verify(prior_prepared)
    C.require(prior['inputRootSha256'] == PRIOR_ROOT, 'reader handoff requires the exact original eb cohort')
    prepared = args.input_closure.resolve()
    if not prepared.exists():
        continuation = C.module('m5_reader_input_copy', 'continue-v2-m5-evidence.py')
        continuation.prepare_postprocessing_source(prior_prepared, prepared, reader_only_eb=True)
    target_root = prepared / 'workspace' / C.INPUTS.LAYOUT['nereus']
    if root != target_root:
        command = [sys.executable, '-u', '-B', str(target_root / SCRIPT), *sys.argv[1:]]
        raise SystemExit(subprocess.run(command, cwd=target_root, env=os.environ.copy()).returncode)
    final = C.INPUTS.verify(prepared)
    delta = check_inputs(prior, final, C)
    for path, node in prior['extraInputs'].items():
        if not any(Path(path).is_relative_to(repo['sourceRoot']) for repo in prior['repositories'].values()):
            raw = C.INPUTS.regular(Path(path))
            C.require(len(raw) == node['bytes'] and hashlib.sha256(raw).hexdigest() == node['sha256'],
                      'live external tool/dependency changed before COMPONENTS: ' + path)
    package = args.output.resolve()
    package.mkdir(parents=True, exist_ok=False)
    executions, originals = {}, []
    for stage, (size, digest) in RECORDS.items():
        reference = {'path': f'executions/{stage}/record.json', 'bytes': size, 'sha256': digest}
        check_record_identity(stage, reference, C)
        record = C.load(C.resource(old_package, reference))
        executions[stage] = C.validate_execution(old_package, record, prior)
        check_runtime(record, final, C)
        shutil.copytree(old_package / 'executions' / stage, package / 'executions' / stage)
        originals.append({'stage': stage, 'record': reference})
    before_sources, after_sources = {}, {}
    lists = {'before': [], 'after': []}
    for row in delta:
        for side, cohort, values in (('before', prior_prepared, before_sources), ('after', prepared, after_sources)):
            node = row[side]
            if node is None:
                continue
            raw = C.INPUTS.regular(cohort / 'objects' / node['sha256'])
            values[row['path']] = raw
            path = 'source-continuation/' + side + '/' + node['sha256']
            if not (package / path).exists():
                C.INPUTS.write_new(package / path, raw)
            lists[side].append({'repository': row['repository'], 'path': row['path'],
                                'object': R.reference(package, path)})
    boundaries = check_boundaries(before_sources, after_sources, C)
    source = source_lineage(package, prior, final, C)
    executions['COMPONENTS'] = R.execute_stage('COMPONENTS', package, prepared, final,
        args.kafka_java_home.resolve(strict=True), target_root / 'build/m5-evidence-ant-repository', None)
    prior_ref = R.write(package, 'source-continuation/prior-input-closure.json', prior)
    proof = {'schema': SCHEMA, 'kind': 'INPUT_EQUIVALENCE_ONLY', 'promotionEligible': False,
        'executionEvidence': False, 'oldInputRootSha256': PRIOR_ROOT, 'finalInputRootSha256': final['inputRootSha256'],
        'priorInputs': prior_ref, 'differences': delta, 'priorSourceObjects': lists['before'],
        'finalSourceObjects': lists['after'], 'boundaryAstSha256': boundaries, 'originalExecutions': originals,
        'components': R.reference(package, 'executions/COMPONENTS/record.json'), 'nativeSourceDirectory': source}
    proof_ref = R.write(package, 'source-continuation/binding.json', proof)
    validate(package, proof_ref, final, C)
    R.assemble(package, final, executions)
    candidate = C.load(C.read(package, 'candidate.json'))
    candidate['stageInputEquivalence'] = proof_ref
    (package / 'candidate.json').write_bytes(C.canonical(candidate))
    C.validate_candidate(package, prepared, target_root)
    print('PASS_V2_M5_COMPLETE_CANDIDATE_AWAITING_BINDING', package, flush=True)


if __name__ == '__main__':
    main()
