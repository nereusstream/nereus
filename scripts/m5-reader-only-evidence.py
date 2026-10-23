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
import time
from types import ModuleType


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


C06_SCHEMA = 'NEREUS_V2_M5_C06_CHECK_INPUT_EQUIVALENCE_V1'
C06_ROOT = 'c06ff0b9179c15e169378e00921454bc2058596b1f577d417982870aed6c78c9'
C06_CANDIDATE_SHA = '3b429bef00636776c8a0b15a8cf155d35740987c6d3763281cf0d90c3e543b28'
C06_BASE_COMMITS = {
    'nereus': '4cd7ee8917a351bf73ca90775a557f6d398d0b82',
    'kafka': '020f33902566ec10f42aaa824d49b40424cee964',
    'pulsar': 'a14e0e6f4e49be0677318b4ceefc7b85b445823b',
}
C06_SOURCE_COMMITS = {
    'nereus': 'e8c12b4308dffa9357961994dc4f0395de48c825',
    'kafka': '6221e3955f4eeeda0ba05a995df3918e6eca7667',
    'pulsar': 'd52ab36ba099ba114f27d5842b9064f428dac5e8',
}
C06_RECORDS = {
    'BOOKKEEPER_OXIA': (502088, 'a3132e603214f503f766d2debedc5d3ecb79e43d2e020bae44978957c8964627'),
    'OBJECT': (77731, 'abe7835757c10edb60f3f0739b953998306285eb841b1c783ec068912c243f8b'),
    'NATIVE_KAFKA': (111884, '6f3a771afb694589d2c55b3863adc635884aca9771585dfa004c2dfaa71758f4'),
    'NATIVE_PULSAR': (103177, '5096c4c6ca359b19cc49825ca28143ef95f034495144cd27c5d950d7d392ce4f'),
}
C06_SELF_BEFORE = '2a5f1a0b1f58be007928fe9df4837893eea9d5edf1ed7e9d679a2c686c1a6848'
C06_SOURCE_HASHES = {'.github/workflows/build.yml': ('c5cf8a1d70348ab3ee2eb5d9008ce7524cb2c1789bf54c83047e554994232451',
                                 'f13be6b35defe87d1d13077fa824ff59e6ae05b720eeecf4965592a25352bb12'),
 'scripts/check-v2-documentation.sh': ('15402e99fbee8c17d41dc11f25ba46613077e406a178e2a162e52e9000c84ee0',
                                       '7a49318150f54b8e9f6f78bf2c331ede829fd722b27c41edf9996a3665597f92'),
 'scripts/check-v2-m5-evidence-tests.py': ('4e0b56f52cb0ed42aaf0d55cb3693f62f91b56f51cdc7d31d152c7c906cf3f3e',
                                           '0691768ce96c3ab7f98b74bf8f0e19d4924fd59182f6b136ed7c0ef7721c225d'),
 'scripts/check-v2-m5-historical-m4-tests.py': ('85ce83acf95e42c4fa2b0b68f23b922fc9c34d2fe4c551955d04deab00f4f016',
                                                '72c614eddf69b672960617f8f326bb12d3d0ae38b4818cfac57c52206404e2d5'),
 'scripts/check-v2-m5-historical-m4.py': ('4618832c71d9e3b18cac94ab07394b05d23f317383b17e80669590aec59c618b',
                                          '470c5a5b7cd5528e0f36be761b8f5ce2fe15669c3bbf245f15f208ddb549a367'),
 'scripts/continue-v2-m5-evidence.py': ('42e2a006f6c896f9919f3e3e3ede98dadeade0a36536ca5080ef9ce54ca5732a',
                                        '0212ac504d28167fd88261be7711b09918b34df2610f522b2c5a78d4346828f6'),
 'scripts/m5-stage-input-equivalence.py': ('f78999390e62c0995311179685138f0128a3d05e22797aa583a7f19450223a2a',
                                           '5d599d5eb6d0b7fae36ba4e66e37b7c61169f4cb4debf3fa3a1f8890eae063ba')}


CI_SCHEMA = 'NEREUS_V2_M5_CI_CHECK_INPUT_EQUIVALENCE_V1'
CI_ROOT = '700d37b25bcd2c40a42d583d608c0a925c580f2f41fbe39bd0cd783a1b79b7a0'
CI_CANDIDATE_SHA = '02fe3a0b8ddc65d5b0eb82d9ee6011e7e70390cf4a05970277e91675436bd8b4'
CI_READER_BEFORE = '2530f5b606cbd94e2392373c5ff1cc6e1d53720bc5209328c02a8a5ec2580362'
CI_CHECKPOINT = {'commit': 'c7d8a6b9fc527a3d1cb48f21f93693b842f6a832',
    'parent': '4d29d0a6be788f3d2e23582ceb0b2adc335f0629',
    'tree': 'bf35bd345e26f77ce0842f1a24023adbe1af304a',
    'sourceTreeCommit': 'a0ba94e01af07711abf00533be523b9216616738'}
CI_SOURCE_HASHES = {'.github/workflows/build.yml': ('f13be6b35defe87d1d13077fa824ff59e6ae05b720eeecf4965592a25352bb12',
                                 'aaa77a96219b2ddf11f39b9f3d96942b3d8cb1d4ee53434b290110efa67e4b2c'),
 'scripts/check-v2-m1-active-graph.sh': ('d0b75c5cddc9b5b9b9d14be1bfef5ead77bcd39e33a42675ac6cc929a4cc62dd',
                                         '3c6c7fe67f2232acc57727f1bff48e8bd4cf1e149e497b554a3b10992df4ae42'),
 'scripts/check-v2-m5-evidence-tests.py': ('0691768ce96c3ab7f98b74bf8f0e19d4924fd59182f6b136ed7c0ef7721c225d',
                                           '6d05e0effb8b76d85ba6537e9b4b47ed6fe19f2c67dbd3b0eab6ad4b59fcf994'),
 'scripts/continue-v2-m5-evidence.py': ('0212ac504d28167fd88261be7711b09918b34df2610f522b2c5a78d4346828f6',
                                        '75c24804cd020e88dbe54daf9590576194d0ae659d0e1e6b6e4e1cad1e6c84df'),
 'scripts/m5-stage-input-equivalence.py': ('5d599d5eb6d0b7fae36ba4e66e37b7c61169f4cb4debf3fa3a1f8890eae063ba',
                                           '441abe4a6c2fd3e6a408819f77fba27ad0d349f8ebf089cdd2900c1dde8e758c')}


def check_ci_inputs(prior, final, C):
    C.INPUTS.validate_value(prior)
    C.INPUTS.validate_value(final)
    C.require(prior['inputRootSha256'] == CI_ROOT and final['inputRootSha256'] != CI_ROOT,
              'CI handoff requires the exact 700d input and one successor')
    C.require(prior['extraInputs'] == final['extraInputs'] and prior['inputManifests'] == final['inputManifests'],
              'CI handoff changed an original runtime, tool, dependency or input manifest')
    for name in C.INPUTS.REPOSITORIES:
        old, new = prior['repositories'][name], final['repositories'][name]
        expected = CI_CHECKPOINT['commit'] if name == 'nereus' else C06_SOURCE_COMMITS[name]
        C.require(old['sourceRoot'] == new['sourceRoot'] and old['baseCommit'] == C06_SOURCE_COMMITS[name]
                  and new['baseCommit'] == expected, 'CI handoff changed a source/base identity')
    delta = C.EQUIVALENCE.differences(prior, final)
    C.require({(row['repository'], row['path']) for row in delta} ==
              {('nereus', path) for path in CI_SOURCE_HASHES.keys() | {SCRIPT}},
              'CI handoff changed a product/fixture/build/driver source or omitted a correction')
    for row in delta:
        before, after = row['before'], row['after']
        expected = (CI_READER_BEFORE, hashlib.sha256(Path(__file__).read_bytes()).hexdigest()) \
            if row['path'] == SCRIPT else CI_SOURCE_HASHES[row['path']]
        C.require(before is not None and after is not None
                  and before.get('mode') == after.get('mode') in ('100644', '100755')
                  and (before['sha256'], after['sha256']) == expected,
                  'CI handoff source is not the exact reviewed correction: ' + row['path'])
        C.require(str(Path(prior['repositories']['nereus']['sourceRoot']) / row['path']) not in prior['extraInputs'],
                  'CI handoff changed an executed runtime dependency')
    return delta


def check_ci_checkpoint(checkpoint, prior, C):
    C.require(checkpoint == CI_CHECKPOINT, 'CI source checkpoint identity differs')
    I = C.INPUTS
    C.require(I.git(C.ROOT, 'show', '-s', '--format=%P', checkpoint['commit']).decode().strip() == checkpoint['parent']
              and I.git(C.ROOT, 'rev-parse', checkpoint['commit'] + '^{tree}').decode().strip() == checkpoint['tree']
              and I.git(C.ROOT, 'rev-parse', checkpoint['sourceTreeCommit'] + '^{tree}').decode().strip() == checkpoint['tree'],
              'CI checkpoint is not the real single-parent restoration of the complete a0 source tree')
    expected = {path: row for path, row in prior['repositories']['nereus']['files'].items() if not row.get('deleted')}
    C.require(I.committed_tree(C.ROOT, checkpoint['commit']) == expected,
              'CI checkpoint differs from the complete 700d source inventory')


def validate_original_c06(root, reference, final, C):
    """Use the exact archived V1 reader, without changing its proof or writing into its archive."""
    proof = C.load(C.resource(root, reference, 'source-continuation'))
    C.require(proof.get('schema') == C06_SCHEMA, 'original c06 schema differs')
    entries = [row for row in proof['finalSourceObjects'] if row.get('path') == SCRIPT]
    C.require(len(entries) == 1 and entries[0]['object']['sha256'] == CI_READER_BEFORE,
              'original c06 reader identity differs')
    item = entries[0]['object']
    raw = C.resource(root, item, 'source-continuation/after')
    C.require(hashlib.sha256(raw).hexdigest() == CI_READER_BEFORE, 'original c06 reader bytes differ')
    frozen = ModuleType('m5_original_700d_reader')
    frozen.__file__ = str((root / item['path']).resolve(strict=True))
    exec(compile(raw, frozen.__file__, 'exec'), frozen.__dict__)
    return frozen.validate(root, reference, final, C)


def check_ci_boundaries(before, after, C):
    path = 'scripts/m5-stage-input-equivalence.py'
    old, new = ast.parse(before[path]), ast.parse(after[path])
    validate = next(node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == 'validate')
    expected = ast.parse("if proof.get('schema') == '" + CI_SCHEMA + "':\n"
        "    return C.module('m5_ci_check_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)\n").body[0]
    C.require(ast.dump(validate.body[4]) == ast.dump(expected), 'CI dispatcher differs')
    validate.body.pop(4)
    original = next(node for node in old.body if isinstance(node, ast.FunctionDef) and node.name == 'validate')
    allowed = copy.deepcopy(original.body[3])
    allowed.body[0].value.func.attr = 'validate_original_c06'
    C.require(ast.dump(validate.body[3]) == ast.dump(allowed), 'frozen c06 dispatcher differs')
    validate.body[3] = copy.deepcopy(original.body[3])
    C.require(ast.dump(old) == ast.dump(new), 'CI handoff changed the old v5/eb continuation')
    workflow = '.github/workflows/build.yml'
    java = b"      - uses: actions/setup-java@v5\n        with:\n          distribution: temurin\n          java-version: '21'\n"
    python = (b"      - uses: actions/setup-python@v6\n        with:\n          python-version: '3.14.7'\n"
        b"      - name: Verify evidence Python interpreter\n"
        b"        run: python3 -c 'import sys; print(sys.executable); print(sys.version); assert sys.version_info[:3] == (3, 14, 7)'\n")
    C.require(before[workflow].count(java) == 3 and after[workflow] == before[workflow].replace(java, java + python),
              'CI workflow changed outside the three exact interpreter pins')
    return {path: hashlib.sha256(ast.dump(old).encode()).hexdigest(),
            workflow: hashlib.sha256(before[workflow]).hexdigest()}


def check_c06_inputs(prior, final, C):
    C.INPUTS.validate_value(prior)
    C.INPUTS.validate_value(final)
    C.require(prior['inputRootSha256'] == C06_ROOT and final['inputRootSha256'] != C06_ROOT,
              'c06 handoff requires the exact complete c06 input and one successor')
    C.require(prior['extraInputs'] == final['extraInputs'] and prior['inputManifests'] == final['inputManifests'],
              'c06 handoff changed a runtime, tool, dependency or input manifest')
    for name in C.INPUTS.REPOSITORIES:
        old, new = prior['repositories'][name], final['repositories'][name]
        C.require(old['sourceRoot'] == new['sourceRoot'] and old['baseCommit'] == C06_BASE_COMMITS[name]
                  and new['baseCommit'] == C06_SOURCE_COMMITS[name], 'c06 handoff changed a source/base identity')
    delta = C.EQUIVALENCE.differences(prior, final)
    C.require({(row['repository'], row['path']) for row in delta} ==
              {('nereus', path) for path in C06_SOURCE_HASHES.keys() | {SCRIPT}},
              'c06 handoff changed a product/fixture/build/driver source or omitted a correction')
    for row in delta:
        before, after = row['before'], row['after']
        expected = (C06_SELF_BEFORE, hashlib.sha256(Path(__file__).read_bytes()).hexdigest()) \
            if row['path'] == SCRIPT else C06_SOURCE_HASHES[row['path']]
        C.require(before is not None and after is not None
                  and before.get('mode') == after.get('mode') in ('100644', '100755')
                  and (before['sha256'], after['sha256']) == expected,
                  'c06 handoff source is not the exact reviewed correction: ' + row['path'])
        C.require(str(Path(prior['repositories']['nereus']['sourceRoot']) / row['path']) not in prior['extraInputs'],
                  'c06 handoff changed an executed runtime dependency')
    for path in C.EQUIVALENCE.CALL_PATHS | {row[0] for row in C.STAGES.values()}:
        C.require(C.source_node(prior, 'nereus', path) == C.source_node(final, 'nereus', path),
                  'c06 handoff changed an actual driver, nested recipe or build input: ' + path)
    return delta


def check_c06_boundaries(before, after, C):
    path = 'scripts/m5-stage-input-equivalence.py'
    old, new = ast.parse(before[path]), ast.parse(after[path])
    validate = next(node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == 'validate')
    dispatch = ast.parse("if proof.get('schema') == '" + C06_SCHEMA + "':\n"
        "    return C.module('m5_c06_check_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)\n").body[0]
    C.require(ast.dump(validate.body[3]) == ast.dump(dispatch), 'c06 dispatcher differs')
    validate.body.pop(3)
    C.require(ast.dump(old) == ast.dump(new), 'c06 handoff changed the old v5/eb continuation')
    workflow = '.github/workflows/build.yml'
    marker = b'  documentation:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v5\n'
    C.require(before[workflow].count(marker) == 1 and after[workflow] ==
              before[workflow].replace(marker, marker + b'        with:\n          fetch-depth: 0\n', 1),
              'c06 workflow changed outside documentation source-history checkout')
    return {path: hashlib.sha256(ast.dump(old).encode()).hexdigest(),
            workflow: hashlib.sha256(before[workflow]).hexdigest()}


def c06_governance_executions(root, C):
    tasks = ('v2DocumentationCheck', 'v2M5HistoricalM4DependencyCheck', 'v2M5HistoricalM4DependencyContractTest')
    result = {}
    for stage in C06_RECORDS:
        record = C.load(C.read(root, f'executions/{stage}/record.json'))
        observed = {}
        for item in record['attachments']:
            if not item['path'].endswith('.log'):
                continue
            raw = C.resource(root, item).decode('utf-8')
            names = [task for task in tasks if '> Task :' + task + '\n' in raw]
            if names:
                observed[item['path']] = names
        result[stage] = observed
    C.require(result['BOOKKEEPER_OXIA'] and all(not result[stage] for stage in C06_RECORDS if stage != 'BOOKKEEPER_OXIA'),
              'c06 governance execution boundary differs')
    return result


def check_inputs(prior, final, C):
    if prior['inputRootSha256'] == CI_ROOT:
        return check_ci_inputs(prior, final, C)
    if prior['inputRootSha256'] == C06_ROOT:
        return check_c06_inputs(prior, final, C)
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
    if SCRIPT in before:
        return check_c06_boundaries(before, after, C)
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


def check_record_identity(stage, reference, C, records=RECORDS):
    C.require(stage in records and reference['path'] == f'executions/{stage}/record.json'
              and (reference['bytes'], reference['sha256']) == records[stage],
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
    ci = proof.get('schema') == CI_SCHEMA
    c06 = ci or proof.get('schema') == C06_SCHEMA
    schema, prior_root, records = (CI_SCHEMA, CI_ROOT, C06_RECORDS) if ci else \
        ((C06_SCHEMA, C06_ROOT, C06_RECORDS) if c06 else (SCHEMA, PRIOR_ROOT, RECORDS))
    extra = ('originalCandidate', 'governanceExecutions', 'governanceVerification') if c06 else ()
    if ci:
        extra += ('checkpoint',)
    C.exact(proof, ('schema', 'kind', 'promotionEligible', 'executionEvidence', 'oldInputRootSha256',
        'finalInputRootSha256', 'priorInputs', 'differences', 'priorSourceObjects', 'finalSourceObjects',
        'boundaryAstSha256', 'originalExecutions', 'components', 'nativeSourceDirectory') + extra, 'reader handoff')
    C.require(proof['schema'] == schema and proof['kind'] == 'INPUT_EQUIVALENCE_ONLY'
              and proof['promotionEligible'] is False and proof['executionEvidence'] is False
              and proof['oldInputRootSha256'] == prior_root
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
    boundaries = check_ci_boundaries(*sources, C) if ci else check_boundaries(*sources, C)
    C.require(proof['boundaryAstSha256'] == boundaries, 'reader AST proof differs')
    origin = prior
    if ci:
        original = root / 'source-continuation/original-complete'
        original_value = C.load(C.read(original, 'candidate.json'))
        original_proof = C.load(C.resource(original, original_value['stageInputEquivalence'], 'source-continuation'))
        origin = C.load(C.resource(original, original_proof['priorInputs'], 'source-continuation'))
        C.require(origin['inputRootSha256'] == C06_ROOT, 'CI handoff lost the four original c06 input origins')
    C.require([row.get('stage') for row in proof['originalExecutions']] == list(records),
              'reader handoff must preserve exactly four original backend stages')
    finishes = []
    for item in proof['originalExecutions']:
        C.exact(item, ('stage', 'record'), 'reader original execution')
        check_record_identity(item['stage'], item['record'], C, records)
        record = C.load(C.resource(root, item['record']))
        C.require(record['stage'] == item['stage'] and record['inputRootSha256'] == origin['inputRootSha256'],
                  'reader handoff relabeled an original execution')
        C.validate_execution(root, record, origin)
        check_runtime(record, final, C)
        finishes.append(record['finishedAtMicros'])
    C.require(proof['components']['path'] == 'executions/COMPONENTS/record.json', 'new COMPONENTS record missing')
    components = C.load(C.resource(root, proof['components']))
    C.require(components['stage'] == 'COMPONENTS' and components['inputRootSha256'] == final['inputRootSha256']
              and components['startedAtMicros'] > max(finishes), 'COMPONENTS did not run after the reader correction')
    C.validate_execution(root, components, final)
    log = C.read(root, 'executions/COMPONENTS/raw/gradle.log').decode()
    for task in ('v2M5EvidenceContractTest', 'v2M5CandidateInputsContractTest',
                 'v2M5EvidenceJUnitContractTest', 'v2DocumentationCheck') + (
                     ('v2M5HistoricalM4DependencyCheck',) if c06 else ()):
        C.require('> Task :' + task + '\n' in log, 'new reader verification did not execute: ' + task)
    source = source_lineage(root, prior, final, C)
    C.require(proof['nativeSourceDirectory'] == source, 'reader handoff changed the actual Source directory')
    if c06:
        verification = proof['governanceVerification']
        C.exact(verification, ('command', 'workingDirectory', 'inputBeforeSha256', 'inputAfterSha256',
                              'startedAtMicros', 'finishedAtMicros', 'exitCode', 'log'), 'c06 governance verification')
        expected_command = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
                            '--max-workers=2', 'v2M5HistoricalM4DependencyContractTest']
        if ci:
            expected_command.append('v2M1FastSourceCheck')
        C.require(verification['command'] == expected_command
                  and verification['workingDirectory'] == components['workingDirectory']
                  and verification['inputBeforeSha256'] == verification['inputAfterSha256'] == final['inputRootSha256']
                  and verification['startedAtMicros'] > components['finishedAtMicros']
                  and verification['finishedAtMicros'] > verification['startedAtMicros']
                  and type(verification['exitCode']) is int and verification['exitCode'] == 0,
                  'c06 historical governance verification differs')
        C.require(verification['log']['path'] == 'source-continuation/historical-contract.log',
                  'c06 historical governance log path differs')
        checked = C.resource(root, verification['log'], 'source-continuation').decode('utf-8')
        C.require('> Task :v2M5HistoricalM4DependencyContractTest\n' in checked
                  and 'Ran 6 tests in ' in checked and '\nOK\n' in checked
                  and 'BUILD SUCCESSFUL' in checked and 'C06_GOVERNANCE_EXIT=0' in checked,
                  'c06 historical six-contract verification did not complete')
        if ci:
            C.require('> Task :v2M1FastSourceCheck\n' in checked,
                      'CI handoff did not execute the original M1 fast source gate')
        registered.add(verification['log']['path'])
        C.require(proof['originalCandidate']['path'] == 'source-continuation/original-complete/candidate.json'
                  and proof['originalCandidate']['sha256'] == (CI_CANDIDATE_SHA if ci else C06_CANDIDATE_SHA),
                  'c06 original complete candidate identity differs')
        original = root / 'source-continuation/original-complete'
        value = C.load(C.resource(root, proof['originalCandidate']))
        C.require(C.read(original, 'input-closure.json') == C.canonical(prior), 'c06 original complete input differs')
        C.validate_package(original, value, prior)
        C.require(proof['governanceExecutions'] == c06_governance_executions(root, C),
                  'c06 governance executions were omitted or relabeled')
        registered.update('source-continuation/original-complete/' + path for path in C.inventory(original, ''))
        if ci:
            check_ci_checkpoint(proof['checkpoint'], prior, C)
        else:
            expected = {path: row for path, row in prior['repositories']['nereus']['files'].items() if row != {'deleted': True}}
            C.require(C.INPUTS.committed_tree(C.ROOT, C06_SOURCE_COMMITS['nereus']) == expected,
                      'c06 original Root source commit differs from its complete tree')
            C.HISTORICAL_M4.require_ancestor(C.ROOT, C06_BASE_COMMITS['nereus'], C06_SOURCE_COMMITS['nereus'])
    C.require(C.inventory(root, 'source-continuation') == registered, 'reader handoff has unregistered files')
    return {'prior': prior, 'stages': tuple(records), 'origins': dict.fromkeys(records, origin),
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
    ci = prior['inputRootSha256'] == CI_ROOT
    c06 = ci or prior['inputRootSha256'] == C06_ROOT
    schema, prior_root, records = (CI_SCHEMA, CI_ROOT, C06_RECORDS) if ci else \
        ((C06_SCHEMA, C06_ROOT, C06_RECORDS) if c06 else (SCHEMA, PRIOR_ROOT, RECORDS))
    C.require(prior['inputRootSha256'] == prior_root, 'reader handoff requires its exact original cohort')
    if c06:
        C.validate_candidate(old_package, prior_prepared, prior_prepared / 'workspace' / C.INPUTS.LAYOUT['nereus'])
    prepared = args.input_closure.resolve()
    if not prepared.exists():
        continuation = C.module('m5_reader_input_copy', 'continue-v2-m5-evidence.py')
        continuation.prepare_postprocessing_source(prior_prepared, prepared, reader_only_eb=not c06,
                                                  reader_only_c06=c06 and not ci, reader_only_ci=ci)
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
    origin = prior
    if ci:
        original_value = C.load(C.read(old_package, 'candidate.json'))
        original_proof = C.load(C.resource(old_package, original_value['stageInputEquivalence'], 'source-continuation'))
        origin = C.load(C.resource(old_package, original_proof['priorInputs'], 'source-continuation'))
    for stage, (size, digest) in records.items():
        reference = {'path': f'executions/{stage}/record.json', 'bytes': size, 'sha256': digest}
        check_record_identity(stage, reference, C, records)
        record = C.load(C.resource(old_package, reference))
        executions[stage] = C.validate_execution(old_package, record, origin)
        check_runtime(record, final, C)
        shutil.copytree(old_package / 'executions' / stage, package / 'executions' / stage)
        originals.append({'stage': stage, 'record': reference})
    if c06:
        C.require(hashlib.sha256(C.read(old_package, 'candidate.json')).hexdigest() ==
                  (CI_CANDIDATE_SHA if ci else C06_CANDIDATE_SHA),
                  'c06 original complete candidate differs')
        shutil.copytree(old_package, package / 'source-continuation/original-complete')
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
    boundaries = check_ci_boundaries(before_sources, after_sources, C) if ci else check_boundaries(before_sources, after_sources, C)
    source = source_lineage(package, prior, final, C)
    executions['COMPONENTS'] = R.execute_stage('COMPONENTS', package, prepared, final,
        args.kafka_java_home.resolve(strict=True), target_root / 'build/m5-evidence-ant-repository', None)
    governance = None
    if c06:
        command = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
                   'v2M5HistoricalM4DependencyContractTest']
        if ci:
            command.append('v2M1FastSourceCheck')
        path = 'source-continuation/historical-contract.log'
        C.require(C.INPUTS.verify(prepared) == final, 'c06 inputs changed before historical governance verification')
        started = time.time_ns() // 1000
        with (package / path).open('x') as log:
            code = subprocess.run(command, cwd=target_root, env=os.environ.copy(),
                                  stdout=log, stderr=subprocess.STDOUT).returncode
            log.write(f'\nC06_GOVERNANCE_EXIT={code}\n')
        finished = time.time_ns() // 1000
        C.require(code == 0 and C.INPUTS.verify(prepared) == final, 'c06 historical governance verification failed')
        governance = {'command': command, 'workingDirectory': str(target_root),
            'inputBeforeSha256': final['inputRootSha256'], 'inputAfterSha256': final['inputRootSha256'],
            'startedAtMicros': started, 'finishedAtMicros': finished, 'exitCode': code, 'log': R.reference(package, path)}
    prior_ref = R.write(package, 'source-continuation/prior-input-closure.json', prior)
    proof = {'schema': schema, 'kind': 'INPUT_EQUIVALENCE_ONLY', 'promotionEligible': False,
        'executionEvidence': False, 'oldInputRootSha256': prior_root, 'finalInputRootSha256': final['inputRootSha256'],
        'priorInputs': prior_ref, 'differences': delta, 'priorSourceObjects': lists['before'],
        'finalSourceObjects': lists['after'], 'boundaryAstSha256': boundaries, 'originalExecutions': originals,
        'components': R.reference(package, 'executions/COMPONENTS/record.json'), 'nativeSourceDirectory': source}
    if c06:
        proof.update(originalCandidate=R.reference(package, 'source-continuation/original-complete/candidate.json'),
                     governanceExecutions=c06_governance_executions(package, C), governanceVerification=governance)
    if ci:
        proof['checkpoint'] = copy.deepcopy(CI_CHECKPOINT)
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
