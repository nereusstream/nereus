#!/usr/bin/env python3
"""The coordination-approved, single-hop v5 continuation; this is never execution evidence by itself."""

import ast
import hashlib
from pathlib import Path
import re


PRIOR_ROOT = '297dc4ef5c0fe1e38e8c5876a5932525fbc4ed9d7d2837b3cef1989714bbc9e7'
STAGES = ('COMPONENTS', 'BOOKKEEPER_OXIA', 'OBJECT')
ORIGINAL_RECORDS = {
    'COMPONENTS': (200738, '8ed54acf9c67a88dafa1e735835ac923122878f690889d526bb45e3e942245b0'),
    'BOOKKEEPER_OXIA': (501680, '2575efe9e447b6d8d527978a1cff039f390131ca12fa361c8f084085250e82aa'),
    'OBJECT': (77935, '885e297887d6d1f4dc2f6e4ff1a73ea865bab3f0320caf632443eac32dfd6588'),
}
NATIVE_DRIVER = 'scripts/run-v2-m5-native-kafka-evidence.py'
OLD_NATIVE_SHA = '68825775f4f2e02b6633da3e343862f4e7c89d0483b3fe6bcebad41c79dc86ae'
NEW_NATIVE_SHA = '9ec56c6f5dbefcc8ffff14cfd66f834844c5972fb85e4f92729fb75fa0f3727f'
NATIVE_ROOT = '6260125ce524bae2dc02fbcf34e8ee7e207a1d58f34a591356c63310fefb554c'
NATIVE_KAFKA_RECORD = '78795a64f97f898bd46f98d61ca0c211a4fbabc452a071f56072765bc9740c63'
NATIVE_STAGES = ('NATIVE_KAFKA',)
REUSED_V5_STAGES = ('BOOKKEEPER_OXIA', 'OBJECT')
PULSAR_SOURCE = 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerImpl.java'
PULSAR_SOURCE_BEFORE = '8b5bd4334a988a8018db17bace276b0f8c6bba8f59a9eb6319ad49909f27e338'
PULSAR_SOURCE_AFTER = '9d1a6e97aa96132c8fdcdbd669de29c0a3f8560ff97f9d08310ef2fcc64ae143'
PULSAR_FIXTURE = 'nereus-pulsar-offload/src/p6ProviderTest/java/com/nereusstream/pulsar/offload/M5NativePulsarOffloadRealTest.java'
PULSAR_FIXTURE_BEFORE = '77a26e091f9d08bd6195411e82907d18bf64e43365c5ef067d74c14fb6b0b7a0'
PULSAR_FIXTURE_AFTER = '83118deb153cea1d82c6711b2969e492da42bfaaab7c0d56f40a0401a6ac00e1'
NATIVE_POSTPROCESSING = {
    'scripts/check-v2-m5-evidence.py', 'scripts/check-v2-m5-evidence-tests.py',
    'scripts/m5-stage-input-equivalence.py', 'scripts/continue-v2-m5-evidence.py',
    'scripts/publish-v2-m5-final.py', 'scripts/run-v2-m5-evidence.py',
    'scripts/m5-native-build-proof.py',
    'scripts/m5-native-build-preparation.py', 'scripts/m5-native-isolated-inputs.py',
    'config/v2/m5/evidence-native-prebuild.init.gradle',
}
POSTPROCESSING = {
    'scripts/check-v2-m5-evidence.py', 'scripts/check-v2-m5-evidence-tests.py',
    'scripts/m5-stage-input-equivalence.py', 'scripts/continue-v2-m5-evidence.py',
    'scripts/publish-v2-m5-final.py',
    'scripts/m5-native-isolated-inputs.py', 'scripts/run-v2-m5-evidence.py',
    'scripts/m5-native-build-preparation.py', 'scripts/m5-native-build-proof.py',
    'config/v2/m5/evidence-native-prebuild.init.gradle',
}
DOCUMENTATION = {
    'docs/v2/detailed_design/m5/m5-current-contracts.md',
    'docs/v2/detailed_design/m5/m5-implementation-log.md', 'docs/v2/NSIP/nsip-1.md',
}
# These actual entry/call paths stay identical. The two exact Pulsar source exceptions above
# are rerun; every reused stage must retain its actual compiler/runtime input closure.
CALL_PATHS = {
    'scripts/run-v2-m5-evidence.py', 'scripts/run-v2-m5-bookkeeper-oxia-evidence.py',
    'scripts/run-v2-m5-object-evidence.py', 'scripts/m5-candidate-inputs.py', 'scripts/m5-evidence-junit.py',
    'scripts/run-v2-m5-kafka-run-source-check.sh', 'scripts/run-v2-m5-bookkeeper-task-terminal-check.sh',
    'scripts/run-v2-m5-epoch-grace-check.py', 'scripts/run-v2-m5-retention-retirement-check.sh',
    'scripts/run-v2-m5-retired-history-oxia-check.sh', 'scripts/run-v2-m5-gc-quota-oxia-check.sh',
    'gradlew', 'settings.gradle.kts', 'build.gradle.kts', 'gradle/libs.versions.toml',
    'config/v2/m5/evidence-native.init.gradle',
    'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml', 'docs/v2/source-locks.json',
}


def differences(prior, final):
    rows = []
    for repository in ('nereus', 'kafka', 'pulsar'):
        old, new = prior['repositories'][repository]['files'], final['repositories'][repository]['files']
        for path in sorted(set(old) | set(new)):
            if old.get(path) != new.get(path):
                rows.append({'repository': repository, 'path': path, 'before': old.get(path), 'after': new.get(path)})
    return rows


def pulsar_fixture_change(row):
    return (row['repository'] == 'nereus' and row['path'] == PULSAR_FIXTURE
            and row['before'] is not None and row['after'] is not None
            and row['before'].get('sha256') == PULSAR_FIXTURE_BEFORE
            and row['after'].get('sha256') == PULSAR_FIXTURE_AFTER
            and row['before'].get('mode') == row['after'].get('mode') == '100644')


def check_pulsar_fixture(before, after, C):
    C.require(hashlib.sha256(before).hexdigest() == PULSAR_FIXTURE_BEFORE
              and hashlib.sha256(after).hexdigest() == PULSAR_FIXTURE_AFTER,
              'Pulsar fixture differs from the exact reviewed budget/observation/finite-cleanup change')
    tests = rb'@Test\s+void\s+(\w+)\([^)]*\)\s+throws Exception\s*\{\s*nativeBindingCapture\((\d+)\);'
    old, new = re.findall(tests, before), re.findall(tests, after)
    C.require(old == new and len(old) == 82, 'Pulsar original fixed test/mode/order inventory changed')


def check_pulsar_fixture_unused(root, record, C):
    C.require(not any('M5NativePulsarOffloadRealTest' in row['sourcePath'] for row in record['runtimeInputs']),
              'reused stage actually loaded the changed Pulsar fixture')
    for row in record['attachments']:
        if row['path'].endswith('.log'):
            raw = C.resource(root, row).decode('utf-8', errors='replace')
            C.require(re.search(r'^> Task :nereus-pulsar-offload:(?:compileP6ProviderTestJava|checkstyleP6ProviderTest|'
                                r'v2M5NativeM4OwnerRealTest|v2M5NativeOffloadRealTest)\b', raw, re.M) is None,
                      'reused stage actually compiled/checked/executed the changed Pulsar fixture')


def check_inputs(prior, final, C):
    C.INPUTS.validate_value(prior)
    C.INPUTS.validate_value(final)
    C.require(prior['inputRootSha256'] == PRIOR_ROOT and final['inputRootSha256'] != PRIOR_ROOT,
              'continuation is limited to the actual v5 input root and one final successor')
    C.NATIVE_INPUTS.check_inventory(prior, final, C)
    for name in C.INPUTS.REPOSITORIES:
        C.require({key: prior['repositories'][name][key] for key in ('baseCommit', 'sourceRoot')} ==
                  {key: final['repositories'][name][key] for key in ('baseCommit', 'sourceRoot')},
                  'continuation changed a source repository identity')
    delta = differences(prior, final)
    C.require(delta, 'empty continuation')
    for row in delta:
        permitted = (row['repository'] == 'nereus' and row['path'] in POSTPROCESSING | DOCUMENTATION | {NATIVE_DRIVER}
                     or pulsar_fixture_change(row)
                     or row['repository'] == 'kafka' and row['path'] in C.NATIVE_INPUTS.SOURCES
                     or row['repository'] == 'pulsar' and row['path'] == PULSAR_SOURCE
                     and row['before']['sha256'] == PULSAR_SOURCE_BEFORE
                     and row['after']['sha256'] == PULSAR_SOURCE_AFTER)
        C.require(permitted,
                  'continuation changed actual product/test/build/call-path input: ' + row['path'])
    native = next((row for row in delta if row['path'] == NATIVE_DRIVER), None)
    C.require(native is not None and native['before']['sha256'] == OLD_NATIVE_SHA
              and native['after']['sha256'] == NEW_NATIVE_SHA,
              'continuation does not describe the approved create-only native archive repair')
    for path in CALL_PATHS:
        if path == 'scripts/run-v2-m5-evidence.py':
            # The full prior source and exact sole-observer AST delta are checked below.
            continue
        C.require(C.source_node(prior, 'nereus', path) == C.source_node(final, 'nereus', path),
                  'continuation changed an actual stage entry or called script: ' + path)
    return delta


def check_original_reference(stage, reference, C):
    C.require(stage in STAGES and reference['path'] == f'executions/{stage}/record.json', 'original stage path differs')
    C.require((reference['bytes'], reference['sha256']) == ORIGINAL_RECORDS[stage],
              'original v5 execution receipt was changed')


def check_reported_differences(prior, final, reported, C):
    actual = check_inputs(prior, final, C)
    C.require(reported == actual, 'continuation omits or misstates a full-inventory difference')
    return actual


def check_function_boundary(before_raw, after_raw, functions, C):
    before, after = ast.parse(before_raw), ast.parse(after_raw)
    def boundary(tree):
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in functions:
                node.body = [ast.Pass()]
        return ast.dump(tree)
    old, new = boundary(before), boundary(after)
    C.require(old == new, 'postprocessing changed an actual dispatch/import/configuration boundary')
    return {'unchangedBoundaryAstSha256': hashlib.sha256(old.encode()).hexdigest(),
            'beforeSha256': hashlib.sha256(before_raw).hexdigest(),
            'afterSha256': hashlib.sha256(after_raw).hexdigest()}


def check_native_postprocessing(prior, final, C):
    C.INPUTS.validate_value(prior)
    C.INPUTS.validate_value(final)
    C.require(prior['inputRootSha256'] == NATIVE_ROOT and final['inputRootSha256'] != NATIVE_ROOT,
              'postprocessing is limited to the original successful v11 Native input')
    C.require(prior['inputManifests'] == final['inputManifests'], 'postprocessing changed dependency manifest identities')
    C.NATIVE_INPUTS.check_pulsar_inventory(prior, final, C)
    for name in C.INPUTS.REPOSITORIES:
        C.require({key: prior['repositories'][name][key] for key in ('baseCommit', 'sourceRoot')} ==
                  {key: final['repositories'][name][key] for key in ('baseCommit', 'sourceRoot')},
                  'postprocessing changed a repository identity')
    delta = differences(prior, final)
    C.require(delta and all((row['repository'] == 'nereus'
              and row['path'] in NATIVE_POSTPROCESSING | DOCUMENTATION
              or pulsar_fixture_change(row)
              or row['repository'] == 'pulsar' and row['path'] == PULSAR_SOURCE
              and row['before']['sha256'] == PULSAR_SOURCE_BEFORE
              and row['after']['sha256'] == PULSAR_SOURCE_AFTER)
              and row['before'] is not None and row['after'] is not None
              and row['before'].get('mode') == row['after'].get('mode') in ('100644', '100755')
              for row in delta), 'postprocessing changed actual product/test/build/call-path source')
    C.require(not {str(Path(prior['repositories'][row['repository']]['sourceRoot']) / row['path'])
                   for row in delta}.intersection(prior['extraInputs']),
              'postprocessing changed an explicitly captured runtime source')
    return delta


def check_observer_change(before_raw, after_raw, C, allow_assembly=False):
    before, after = ast.parse(before_raw), ast.parse(after_raw)
    def without_monitor(tree):
        return ast.Module(body=[node for node in tree.body if not isinstance(node, ast.FunctionDef)
                               or node.name not in ({'docker_inventory', 'assemble'} if allow_assembly
                                                   else {'docker_inventory'})], type_ignores=[])
    old_rest, new_rest = ast.dump(without_monitor(before)), ast.dump(without_monitor(after))
    C.require(old_rest == new_rest, 'parent driver changed outside the approved read-only observer')
    old_function = next(node for node in before.body if isinstance(node, ast.FunctionDef) and node.name == 'docker_inventory')
    new_function = next(node for node in after.body if isinstance(node, ast.FunctionDef) and node.name == 'docker_inventory')
    result = {'path': 'scripts/run-v2-m5-evidence.py', 'function': 'docker_inventory',
            'beforeAstSha256': hashlib.sha256(ast.dump(old_function).encode()).hexdigest(),
            'afterAstSha256': hashlib.sha256(ast.dump(new_function).encode()).hexdigest(),
            'unchangedRemainderAstSha256': hashlib.sha256(old_rest.encode()).hexdigest()}
    if allow_assembly:
        result['postprocessingBoundary'] = check_function_boundary(
            before_raw, after_raw, {'docker_inventory', 'assemble'}, C)
    return result


def check_direct_calls(prior_sources, final, C, allow_assembly=False):
    # Verify the actual dispatch expression and fixed nested shell calls, in addition to the
    # original command/driver records and complete unchanged runtime/build inventories.
    def source(path):
        raw = C.INPUTS.regular(C.ROOT / path)
        node = C.source_node(final, 'nereus', path)
        C.require(len(raw) == node['bytes'] and hashlib.sha256(raw).hexdigest() == node['sha256'],
                  'direct-call source is not the final captured file')
        return raw
    old_checker = ast.parse(prior_sources['scripts/check-v2-m5-evidence.py'])
    stage_node = next(node.value for node in old_checker.body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == 'STAGES' for target in node.targets))
    original_stages = ast.literal_eval(stage_node)
    C.require(all(original_stages[stage] == C.STAGES[stage] for stage in STAGES), 'original stage dispatch changed')
    parent_raw = source('scripts/run-v2-m5-evidence.py')
    parent = ast.parse(parent_raw)
    observer = None
    if 'scripts/run-v2-m5-evidence.py' in prior_sources:
        observer = check_observer_change(prior_sources['scripts/run-v2-m5-evidence.py'], parent_raw, C, allow_assembly)
    dispatch = next(node for node in parent.body if isinstance(node, ast.FunctionDef) and node.name == 'execute_stage')
    assignments = {target.id: node.value for node in ast.walk(dispatch) if isinstance(node, ast.Assign)
                   for target in node.targets if isinstance(target, ast.Name)}
    C.require(ast.unparse(assignments['driver']) == 'C.STAGES[stage][0]'
              and isinstance(assignments['command'], ast.List)
              and ast.literal_eval(assignments['command'].elts[0]) == 'python3'
              and ast.unparse(assignments['command'].elts[1]) == 'driver', 'stage command dispatch differs')
    C.require(any(isinstance(node, ast.Call) and ast.unparse(node.func) == 'subprocess.Popen'
                  and ast.unparse(node.args[0]) == 'command' for node in ast.walk(dispatch)),
              'original stage is not launched through its recorded command')
    bookkeeper = ast.parse(source('scripts/run-v2-m5-bookkeeper-oxia-evidence.py'))
    recipes = next(node for node in bookkeeper.body if isinstance(node, ast.For)
                   and isinstance(node.target, ast.Name) and node.target.id == 'recipe')
    C.require(ast.literal_eval(recipes.iter) == ('kafka-run-source', 'retention-retirement', 'retired-history-oxia', 'gc-quota-oxia')
              and any(isinstance(node, ast.Call) and ast.unparse(node.func) == 'subprocess.run'
                      and ast.unparse(node.args[0]) == "['bash', 'scripts/run-v2-m5-' + recipe + '-check.sh']"
                      for node in ast.walk(recipes)), 'BK/Oxia actual nested call closure differs')
    return observer


def native_boundaries(before, after, C):
    result = {}
    for path, functions in {
            'scripts/check-v2-m5-evidence.py': {'validate_execution', 'validate_child', 'validate_package', 'validate_capacity'},
            'scripts/run-v2-m5-evidence.py': {'assemble'}}.items():
        if path in before:
            result[path] = check_function_boundary(before[path], after[path], functions, C)
    return result


def validate_native_origin(root, proof, final, final_sources, C):
    reference = proof['nativePriorInputs']
    C.require(reference['path'] == 'source-continuation/native-origin/input-closure.json',
              'noncanonical original Native input closure')
    prior = C.load(C.resource(root, reference, 'source-continuation/native-origin'))
    delta = check_native_postprocessing(prior, final, C)
    C.require(proof['nativeDifferences'] == delta, 'Native-to-final complete source difference inventory differs')
    required = {row['before']['sha256']: row['before'] for row in delta}
    objects, paths = {}, {reference['path']}
    for row in proof['nativePriorSourceObjects']:
        raw = C.resource(root, row, 'source-continuation/native-origin/source')
        C.require(row['sha256'] in required and row['sha256'] not in objects
                  and len(raw) == required[row['sha256']]['bytes'], 'original Native source object differs')
        objects[row['sha256']] = raw
        paths.add(row['path'])
    C.require(set(objects) == set(required), 'missing original Native changed-source bytes')
    before = {row['path']: objects[row['before']['sha256']] for row in delta}
    if PULSAR_SOURCE in before:
        check_pulsar_source(before[PULSAR_SOURCE], final_sources[PULSAR_SOURCE], C)
    if PULSAR_FIXTURE in before:
        check_pulsar_fixture(before[PULSAR_FIXTURE], final_sources[PULSAR_FIXTURE], C)
    C.require(proof['nativeSourceBoundaries'] == native_boundaries(before, final_sources, C),
              'Native postprocessing/dispatch AST boundary differs')
    rows = proof['nativeOriginalExecutions']
    C.require(isinstance(rows, list) and [row.get('stage') for row in rows] == list(NATIVE_STAGES),
              'original Native stage inventory differs')
    for row in rows:
        C.exact(row, ('stage', 'record'), 'original Native execution reference')
        C.require(row['record']['path'] == f"executions/{row['stage']}/record.json",
                  'original Native execution path differs')
        if row['stage'] == 'NATIVE_KAFKA':
            C.require(row['record']['sha256'] == NATIVE_KAFKA_RECORD, 'original successful Native63 record changed')
        record = C.load(C.resource(root, row['record']))
        C.require(record['stage'] == row['stage'] and record['inputRootSha256'] == NATIVE_ROOT,
                  'original Native execution was relabeled')
        C.validate_execution(root, record, prior)
        check_pulsar_fixture_unused(root, record, C)
    return prior, paths


def check_pulsar_source(before, after, C):
    original = b'        asyncReadEntry(ledgerId, ledger, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);'
    fixed = (b'        if (ledger != null) {\n'
             b'            asyncReadEntry(ledger, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);\n'
             b'        } else {\n'
             b'            asyncReadEntry(ledgerId, null, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);\n'
             b'        }')
    C.require(hashlib.sha256(before).hexdigest() == PULSAR_SOURCE_BEFORE
              and hashlib.sha256(after).hexdigest() == PULSAR_SOURCE_AFTER
              and before.count(original) == 1 and before.replace(original, fixed) == after,
              'Pulsar source changed outside the exact virtual-read compatibility repair')


def validate(root, reference, final, C):
    C.require(reference['path'] == 'source-continuation/binding.json', 'noncanonical continuation binding')
    proof = C.load(C.resource(root, reference, 'source-continuation'))
    if proof.get('schema') == 'NEREUS_V2_M5_EB_READER_INPUT_EQUIVALENCE_V1':
        return C.module('m5_eb_reader_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)
    if proof.get('schema') == 'NEREUS_V2_M5_C06_CHECK_INPUT_EQUIVALENCE_V1':
        return C.module('m5_c06_check_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)
    native = proof.get('schema') == 'NEREUS_V2_M5_STAGE_INPUT_EQUIVALENCE_V4'
    keys = ('schema', 'kind', 'promotionEligible', 'executionEvidence', 'oldInputRootSha256',
                    'finalInputRootSha256', 'priorInputs', 'differences', 'priorSourceObjects',
                    'originalExecutions', 'directCallSources', 'verification', 'sourceRelocations',
                    'observerChange', 'nativeInputDifferences', 'nativeInputObjects', 'finalSourceObjects',
                    'nativeBuildPreparation')
    if native:
        keys += ('nativePriorInputs', 'nativeDifferences', 'nativePriorSourceObjects',
                 'nativeOriginalExecutions', 'nativeSourceBoundaries', 'pulsarBuildPreparation')
    C.exact(proof, keys, 'stage equivalence')
    C.require(proof['schema'] in ('NEREUS_V2_M5_STAGE_INPUT_EQUIVALENCE_V3',
                                'NEREUS_V2_M5_STAGE_INPUT_EQUIVALENCE_V4')
              and proof['kind'] == 'INPUT_EQUIVALENCE_ONLY' and proof['promotionEligible'] is False
              and proof['executionEvidence'] is False, 'equivalence cannot grant execution/promotion authority')
    C.require(proof['oldInputRootSha256'] == PRIOR_ROOT
              and proof['finalInputRootSha256'] == final['inputRootSha256'], 'continuation input roots differ')
    prior = C.load(C.resource(root, proof['priorInputs'], 'source-continuation'))
    delta = check_reported_differences(prior, final, proof['differences'], C)
    required_sources = {row['before']['sha256']: row['before'] for row in delta if row['before'] is not None
                        and row['before'] != {'deleted': True}}
    C.require(isinstance(proof['priorSourceObjects'], list), 'missing previous source objects')
    objects = {}
    prior_sources = {}
    for row in proof['priorSourceObjects']:
        raw = C.resource(root, row, 'source-continuation/previous-source')
        C.require(row['sha256'] in required_sources and row['sha256'] not in objects,
                  'unregistered or duplicate previous source object')
        record = required_sources[row['sha256']]
        C.require(len(raw) == record['bytes'], 'previous source bytes differ')
        objects[row['sha256']] = raw
    C.require(set(objects) == set(required_sources), 'previous complete source inventory cannot be reconstructed')
    # The native script was only hashed/archived by previous stages. It was not their entry point.
    # Keep the prior and final full driver sources independently available for the bounded C4 review.
    C.require(OLD_NATIVE_SHA in objects, 'missing original native driver source')
    ast.parse(objects[OLD_NATIVE_SHA])
    for row in delta:
        if row['before'] is not None and row['before'] != {'deleted': True}:
            prior_sources[row['path']] = objects[row['before']['sha256']]
    expected_final_sources = [row for row in delta if row['after'] is not None and row['after'] != {'deleted': True}]
    C.require(isinstance(proof['finalSourceObjects'], list) and len(proof['finalSourceObjects']) == len(expected_final_sources),
              'missing complete final changed-source versions')
    final_sources = {}
    for item, change in zip(proof['finalSourceObjects'], expected_final_sources):
        C.exact(item, ('repository', 'path', 'object'), 'final changed source')
        C.require((item['repository'], item['path']) == (change['repository'], change['path']),
                  'final changed-source inventory differs')
        raw = C.resource(root, item['object'], 'source-continuation/final-source')
        C.require({key: item['object'][key] for key in ('bytes', 'sha256')} ==
                  {key: change['after'][key] for key in ('bytes', 'sha256')}, 'final changed-source bytes differ')
        final_sources[item['path']] = raw
    if any(row['repository'] == 'kafka' for row in delta):
        C.NATIVE_INPUTS.check_native_sources(prior_sources, final_sources, C)
    if PULSAR_SOURCE in prior_sources:
        check_pulsar_source(prior_sources[PULSAR_SOURCE], final_sources[PULSAR_SOURCE], C)
    if PULSAR_FIXTURE in prior_sources:
        check_pulsar_fixture(prior_sources[PULSAR_FIXTURE], final_sources[PULSAR_FIXTURE], C)
    C.require(proof['observerChange'] == check_direct_calls(prior_sources, final, C, native),
              'parent observer AST difference proof differs')
    native_delta = C.NATIVE_INPUTS.check_inventory(prior, final, C)
    C.require(proof['nativeInputDifferences'] == native_delta, 'Native input difference inventory is incomplete')
    native_prior, native_paths = validate_native_origin(root, proof, final, final_sources, C) if native else (final, set())
    preparation = C.NATIVE_BUILD.validate(root, proof['nativeBuildPreparation'], final, C, native_prior)
    pulsar_preparation = C.NATIVE_BUILD.validate_pulsar(root, proof['pulsarBuildPreparation'], final, C) if native else None
    if native:
        for path, node in preparation['consumedInputs'].items():
            source = None
            for repo in final['repositories'].values():
                if Path(path).is_relative_to(repo['sourceRoot']):
                    source = repo['files'].get(Path(path).relative_to(repo['sourceRoot']).as_posix()); break
            actual = source or final['extraInputs'].get(path)
            C.require(actual is not None and all(actual[key] == node[key] for key in ('bytes', 'sha256')),
                      'reused Native Kafka actually compiled/loaded a changed input: ' + path)
    native_origin_paths = native_paths
    native_paths = C.NATIVE_INPUTS.validate_objects(root, proof['nativeInputObjects'], native_delta, prior, final, C,
                                                  preparation, pulsar_preparation)
    native_paths.update(preparation['paths'])
    if pulsar_preparation is not None:
        native_paths.update(pulsar_preparation['paths'])
    native_paths.update(native_origin_paths)
    expected_calls = {path: C.source_node(prior, 'nereus', path) for path in sorted(CALL_PATHS)}
    C.require(proof['directCallSources'] == expected_calls, 'stage direct-call/build provenance differs')
    original = proof['originalExecutions']
    reused = REUSED_V5_STAGES if native else STAGES
    C.require(isinstance(original, list) and [row.get('stage') for row in original] == list(reused),
              'continuation must retain exactly its unaffected successful v5 stages')
    for row in original:
        C.exact(row, ('stage', 'record'), 'original execution reference')
        check_original_reference(row['stage'], row['record'], C)
        record = C.load(C.resource(root, row['record']))
        C.require(record['stage'] == row['stage'] and record['inputRootSha256'] == PRIOR_ROOT,
                  'original execution was relabeled')
        C.validate_execution(root, record, prior)
        C.NATIVE_INPUTS.check_stage_usage(native_delta, record, C)
        check_pulsar_fixture_unused(root, record, C)
    verification = proof['verification']
    C.exact(verification, ('inputRootSha256', 'inputBeforeSha256', 'inputAfterSha256', 'command', 'exitCode',
                          'startedAtMicros', 'finishedAtMicros', 'log'), 'fresh verifier execution')
    C.require(all(verification[key] == final['inputRootSha256'] for key in
                  ('inputRootSha256', 'inputBeforeSha256', 'inputAfterSha256')),
              'fresh verifier did not execute the final complete input cohort')
    C.require(verification['command'] == ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
              '--max-workers=2', 'v2M5EvidenceContractTest', 'v2DocumentationCheck']
              and type(verification['exitCode']) is int and verification['exitCode'] == 0,
              'fresh evidence/input/JUnit/documentation verification is incomplete')
    start = C.integer(verification['startedAtMicros'], 'verification start', 1)
    finish = C.integer(verification['finishedAtMicros'], 'verification finish', 1)
    C.require(finish > start, 'verification did not complete')
    log = C.resource(root, verification['log'], 'source-continuation').decode('utf-8')
    for task in ('v2M5EvidenceContractTest', 'v2M5CandidateInputsContractTest', 'v2M5EvidenceJUnitContractTest',
                 'v2DocumentationCheck'):
        C.require('> Task :' + task + '\n' in log, 'fresh verifier task did not execute: ' + task)
    C.require('BUILD SUCCESSFUL' in log and 'M5_CONTINUATION_VERIFICATION_EXIT=0' in log,
              'fresh verifier has no successful actual completion')
    C.require(isinstance(proof['sourceRelocations'], list) and len(proof['sourceRelocations']) == 1,
              'missing exact source-qualification relocation')
    relocation = proof['sourceRelocations'][0]
    C.exact(relocation, ('originalDirectory', 'finalDirectory', 'files'), 'source relocation')
    C.require(isinstance(relocation['files'], list) and relocation['files'], 'empty source relocation')
    original_directory = C.read(root, 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').decode().strip()
    C.require(relocation['originalDirectory'] == original_directory,
              'relocation does not preserve actual source-run origin')
    directory_name = original_directory.rsplit('/', 1)[1]
    C.require(relocation['finalDirectory'].endswith('/workspace/GITHUB/nereus/build/m5-kafka-run-source/' + directory_name),
              'source relocation target is outside the final prepared workspace')
    original_prefix = 'executions/BOOKKEEPER_OXIA/raw/native-archives/m5-kafka-run-source/' + directory_name
    expected = {}
    for path in C.inventory(root, original_prefix):
        raw = C.read(root, path)
        expected[path[len(original_prefix) + 1:]] = (len(raw), hashlib.sha256(raw).hexdigest())
    # Original standalone manifests predate canonical evidence JSON. Read their exact bytes with
    # the existing bounded duplicate-key-rejecting input reader; never reformat the original.
    manifest = C.INPUTS.load(C.read(root, original_prefix + '/tested-inputs.json'))
    for path, sha in manifest.items():
        node = C.source_node(final, 'nereus', path)
        C.require(node['sha256'] == sha, 'source-qualification input differs from final source')
        expected['source-archive/sources/' + path] = (node['bytes'], sha)
    actual = {}
    for row in relocation['files']:
        C.exact(row, ('relativePath', 'archive'), 'relocated source input')
        C.relative(row['relativePath'])
        C.resource(root, row['archive'], 'source-continuation/source-inputs')
        C.require(row['relativePath'] not in actual, 'duplicate relocated input')
        actual[row['relativePath']] = (row['archive']['bytes'], row['archive']['sha256'])
    C.require(actual == expected, 'relocation omitted or changed an actual source-qualification input')
    paths = C.inventory(root, 'source-continuation')
    registered = {reference['path'], proof['priorInputs']['path'], verification['log']['path']}
    registered.update(row['path'] for row in proof['priorSourceObjects'])
    registered.update(row['object']['path'] for row in proof['finalSourceObjects'])
    registered.update(row['archive']['path'] for row in relocation['files'])
    registered.update(native_paths)
    C.require(paths == registered, 'continuation contains missing or unreferenced files')
    origins = dict.fromkeys(reused, prior)
    native_target = relocation['finalDirectory']
    if native:
        origins.update(dict.fromkeys(NATIVE_STAGES, native_prior))
        native_target = str(Path(preparation['record']['workspace']) / 'workspace/GITHUB/nereus/build/m5-kafka-run-source'
                            / directory_name)
    return {'prior': prior, 'stages': reused, 'origins': origins, 'nativeSourceDirectory': native_target,
            'paths': paths, 'proof': proof}
