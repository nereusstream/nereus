#!/usr/bin/env python3
"""Execute the fixed complete M5 campaign in its prepared workspace and assemble a non-promotable candidate."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location('m5_execution', ROOT / 'scripts/check-v2-m5-evidence.py')
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


def write(root, path, value):
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('xb') as output:
        output.write(C.canonical(value))
    return reference(root, path)


def reference(root, path):
    raw = C.read(root, path)
    return {'path': path, 'bytes': len(raw), 'sha256': C.INPUTS.digest(raw)}


def docker_inventory():
    ids = subprocess.check_output(['docker', 'ps', '-aq', '--no-trunc'], text=True).split()
    if not ids:
        return {}
    result = subprocess.run(['docker', 'inspect', *ids], text=True, capture_output=True)
    rows = json.loads(result.stdout)
    observed = {row['Id']: row for row in rows}
    C.require(isinstance(rows, list) and len(observed) == len(rows) and set(observed).issubset(ids),
              'Docker returned an invalid observed inventory')
    if result.returncode:
        # An owned container can finish teardown between ps and inspect. Accept only Docker's
        # explicit per-ID absence, retaining every successful observation; all other errors fail.
        missing = set()
        for line in result.stderr.splitlines():
            match = re.fullmatch(r'error: no such object: ([0-9a-f]{12,64})', line.strip(), re.I)
            C.require(match is not None, 'Docker inventory failed: ' + line)
            candidates = [identity for identity in ids if identity.startswith(match[1])]
            C.require(len(candidates) == 1, 'Docker absence did not identify one listed container')
            missing.add(candidates[0])
        C.require(missing and set(observed).isdisjoint(missing) and set(observed) | missing == set(ids),
                  'Docker inventory has unexplained missing objects')
    else:
        C.require(set(observed) == set(ids), 'Docker inventory silently omitted listed objects')
    return observed


def unchanged_containers(before, after):
    return all(identity in after and after[identity]['Name'] == row['Name']
               and after[identity]['State']['Status'] == row['State']['Status'] for identity, row in before.items())


def components(output, prepared):
    output.mkdir(parents=True, exist_ok=False)
    started = time.time_ns()
    tasks = ['v2DocumentationCheck', 'v2M5HistoricalM4DependencyCheck', 'v2M5EvidenceContractTest',
             'v2M5MaterializationCheck', 'v2M5KafkaCompactionCheck',
             ':nereus-storage-object:v2M5RetentionCoreTest', ':nereus-storage-object:v2M5BindingAuthorityTest',
             ':nereus-storage-object:v2M5ClosedWriterIntegrationTest',
             ':nereus-storage-object:v2M5MaterializationWorkerTest',
             ':nereus-storage-object:v2M5TaskSelectionTest',
             ':nereus-storage-object:v2M5TargetDeleteAuthorityCoordinatorTest',
             ':nereus-storage-object:v2M5MultiWriterTicketTest']
    tasks += [task for group in C.M4_TASKS.values() for task in group]
    with (output / 'gradle.log').open('w') as log:
        subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
                        '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
                        '-PpulsarCheckout=' + str(prepared / 'workspace' / C.INPUTS.LAYOUT['pulsar']),
                        '-Dnereus.m5.evidenceMavenRepository=' + str(ROOT / 'build/m5-evidence-ant-repository'),
                        '-Dnereus.m5.evidenceSdkTestingRepository=' + str(ROOT / 'build/m5-native-pulsar-owner-local-dependencies'),
                        *tasks], cwd=ROOT, env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, check=True)
    rows = []
    for directory in sorted(ROOT.glob('nereus-*/build/test-results')):
        module = directory.parent.parent.name
        for xml in sorted(directory.glob('*/TEST-*.xml')):
            if xml.stat().st_mtime_ns < started:
                continue
            target = output / 'junit' / module / xml.parent.name / xml.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(xml, target)
            rows.append({'task': ':' + module + ':' + xml.parent.name,
                         'path': target.relative_to(output).as_posix(),
                         'producedAtMicros': xml.stat().st_mtime_ns // 1000})
    C.INPUTS.verify(prepared)
    C.require(rows, 'components produced no fresh raw suites')
    write(output, 'stage-result.json', {'junit': rows, 'exitCode': 0})


def loaded_inputs(logs, inputs, prepared):
    # Read JVM observations, then hash the actual loaded file, including individual project classes.
    # All Nereus/native SDK/Oxia/S3/BookKeeper runtime origins must already be frozen inputs.
    expected = {}
    for source, record in inputs['extraInputs'].items():
        expected.setdefault((record['bytes'], record['sha256']), []).append(source)
    loaded = {}
    prefixes = ('com.nereusstream.', 'kafka.', 'org.apache.kafka.', 'org.apache.pulsar.',
                'org.apache.bookkeeper.', 'io.oxia.', 'software.amazon.awssdk.')
    for log in sorted(logs.glob('*.log')):
        for line in log.read_text(errors='strict').splitlines():
            match = re.search(r'\[class,load\]\s+(\S+)\s+source:\s+(file:\S+)', line)
            if match is None or not match[1].startswith(prefixes):
                continue
            path = Path(unquote(urlparse(match[2]).path))
            if path.is_dir():
                path = path / (match[1].split('$$', 1)[0].replace('.', '/') + '.class')
            C.require(path.is_file(), 'observed runtime origin is not an actual regular file: ' + str(path))
            raw = C.INPUTS.regular(path)
            digest = C.INPUTS.digest(raw)
            registered = expected.get((len(raw), digest), [])
            # Equal content can have several frozen Maven cache aliases; prefer the original logical path.
            C.require(registered, 'loaded runtime input was not frozen: ' + str(path))
            source = next((name for name in registered if name == str(path)), sorted(registered)[0])
            loaded[source] = {'sourcePath': source, 'bytes': len(raw), 'sha256': digest}
    C.require(loaded, 'execution produced no observable loaded runtime dependency')
    return [loaded[source] for source in sorted(loaded)]


def execute_stage(stage, package, prepared, inputs, kafka_java, maven_repository, source_run):
    prefix = 'executions/' + stage
    directory = package / prefix
    directory.mkdir(parents=True, exist_ok=False)
    logs = directory / 'classes'
    logs.mkdir()
    raw = directory / 'raw'
    driver = C.STAGES[stage][0]
    command = ['python3', driver, '--input-closure', str(prepared), '--output', str(raw)]
    if stage == 'COMPONENTS':
        command += ['--stage-components']
    else:
        command += ['--class-logs', str(logs)]
    if stage == 'NATIVE_KAFKA':
        C.require(source_run is not None, 'native Kafka requires this campaign current source run')
        command += ['--source-run', str(source_run)]
    if stage == 'NATIVE_PULSAR':
        command += ['--maven-repository', str(maven_repository)]
    C.require(C.INPUTS.verify(prepared) == inputs, 'prepared inputs changed before execution')
    before = docker_inventory()
    observed = {}
    env = os.environ.copy()
    env['NEREUS_M5_KAFKA_JAVA_HOME'] = str(kafka_java)
    # Every fork, including nested existing shell recipes, records its actual class origins.
    env['JAVA_TOOL_OPTIONS'] = env.get('JAVA_TOOL_OPTIONS', '') + ' -Xlog:class+load=info:file=' + str(logs / 'classes-%p.log')
    # The existing nested shell recipes also inherit this JVM option. Enum-only default display
    # names otherwise collide between distinct parameterized methods in the same raw Gradle suite.
    env['JAVA_TOOL_OPTIONS'] += ' -Djunit.jupiter.params.displayname.default={displayName}[{index}]{arguments}'
    env.pop('NEREUS_M5_VERIFIED_LEGACY_OUTPUT', None)
    start = time.time_ns() // 1000
    with (directory / 'execution.log').open('w') as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        while process.poll() is None:
            for identity, row in docker_inventory().items():
                if identity not in before and ('m5-' in row['Name'] or 'v2-m5' in row['Name']):
                    observed[identity] = row
            time.sleep(1)
        code = process.wait()
        log.write(f'\nM5_STAGE_EXIT stage={stage} code={code}\n')
    finish = time.time_ns() // 1000
    C.require(code == 0, f'{stage} failed with actual exit {code}; raw failure archive retained')
    C.require(C.INPUTS.verify(prepared) == inputs, 'prepared inputs changed during execution')
    after = docker_inventory()
    C.require(unchanged_containers(before, after) and not set(observed).intersection(after),
              'owned native cleanup did not complete')
    result = json.loads((raw / 'stage-result.json').read_bytes(), object_pairs_hook=C.INPUTS.pairs)
    C.require(result['exitCode'] == 0 and result['junit'], 'actual stage did not complete its suite inventory')
    rows = []
    for item in result['junit']:
        source = raw / C.relative(item['path'])
        C.require(source.resolve().is_relative_to(raw.resolve()), 'raw result escaped actual stage')
        rows.append({**reference(package, source.relative_to(package).as_posix()),
                     'task': item['task'], 'producedAtMicros': item['producedAtMicros']})
    # Add the direct pre-cleanup observations made by the Python recipes. Shell-owned containers
    # are observed while alive above; no backend is recreated to fabricate a post-run identity.
    for path in raw.glob('*docker-inspect.json'):
        for row in json.loads(path.read_bytes()):
            observed[row['Id']] = row
    write(package, prefix + '/docker-inspect.json', list(observed.values()))
    runtime = loaded_inputs(logs, inputs, prepared)
    selections = []
    for boundary in C.STAGES[stage][1]:
        if boundary in C.IMAGES:
            image, image_id = C.IMAGES[boundary]
            candidates = [row for row in observed.values() if row.get('Image') == image_id
                          and row.get('Config', {}).get('Image') == image]
            C.require(candidates, stage + ' lacks observed actual ' + boundary)
            identity = {'image': image, 'imageId': image_id, 'containerId': candidates[0]['Id']}
            implementation = 'PINNED_CONTAINER'
            raw_ref = reference(package, prefix + '/docker-inspect.json')
        else:
            name = 'kafka' if boundary == 'NATIVE_KAFKA' else 'pulsar'
            loaded = {row['sourcePath']: row['sha256'] for row in runtime}
            raw_ref = write(package, prefix + '/native-loaded.json', loaded)
            identity = {'repository': name, 'baseCommit': inputs['repositories'][name]['baseCommit'],
                        'loadedInputs': loaded}
            implementation = 'ACTUAL_NATIVE_SDK'
        facts = {'schema': 'NEREUS_V2_M5_BACKEND_SELECTION_V1', 'boundary': boundary,
                 'implementation': implementation, 'identity': identity, 'rawEvidence': raw_ref}
        selections.append({'boundary': boundary, 'facts': write(package, prefix + '/' + boundary + '.json', facts)})
    node = C.source_node(inputs, 'nereus', driver)
    record = {'schema': 'NEREUS_V2_M5_EXECUTION_V1', 'stage': stage,
              'inputRootSha256': inputs['inputRootSha256'],
              'sourceLocksSha256': C.source_node(inputs, 'nereus', 'docs/v2/source-locks.json')['sha256'],
              'startedAtMicros': start, 'finishedAtMicros': finish, 'exitCode': code,
              'command': command, 'driver': {'repository': 'nereus', 'path': driver,
                                             'bytes': node['bytes'], 'sha256': node['sha256']},
              'workingDirectory': str(ROOT), 'inputBeforeSha256': inputs['inputRootSha256'],
              'inputAfterSha256': inputs['inputRootSha256'], 'log': reference(package, prefix + '/execution.log'),
              'junit': rows, 'runtimeInputs': runtime, 'boundaries': list(C.STAGES[stage][1]),
              'backendSelections': selections, 'cleanup': {'ownedAbsent': True, 'originalUnchanged': True}}
    record['attachments'] = [reference(package, path) for path in sorted(C.inventory(package, prefix))]
    write(package, prefix + '/record.json', record)
    verified = C.validate_execution(package, record, inputs)
    print('PASS_M5_STAGE', stage, verified['testSummary'], flush=True)
    return verified


def assemble(package, inputs, executions):
    cases = {C.case_key(stage, case): case for stage, execution in executions.items() for case in execution['cases']}
    origins = {stage: C.load(C.read(package, f'executions/{stage}/record.json'))['inputRootSha256']
               for stage in C.STAGES}
    claimed = {}
    children = []
    for kind in C.KINDS:
        obligations = {}
        selected = set()

        def choose(anchors):
            identities = []
            for cls, method in anchors:
                choices = [identity for identity, case in cases.items() if C.matches(case, cls, method)
                           and case['executionStage'] == C.CLASS_STAGES[cls]
                           and claimed.get(identity, kind) == kind]
                C.require(choices, 'no exclusive executed assertion for ' + kind + '/' + method)
                identity = sorted(choices)[0]
                identities.append(identity)
                claimed[identity] = kind
                selected.add(identity)
            return identities

        for obligation, owner in C.OWNERS.items():
            if owner == kind:
                obligations[obligation] = choose(C.ANCHORS[obligation])
        core = choose(C.CORE_ANCHORS[kind])
        xml_hashes = {cases[identity]['xmlSha256'] for identity in selected}
        attachments = []
        for stage, execution in executions.items():
            for suite in execution['suites']:
                if suite['sha256'] not in xml_hashes:
                    continue
                path = f"children/{kind}/raw/{stage}/" + suite['sha256'] + '-' + Path(suite['path']).name
                destination = package / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                with destination.open('xb') as output:
                    output.write(C.read(package, suite['path']))
                attachments.append(reference(package, path))
        summary = dict.fromkeys(C.JUNIT.COUNTERS, 0)
        summary['tests'] = sum(case['xmlSha256'] in xml_hashes for case in cases.values())
        value = {'schema': 'NEREUS_V2_M5_CHILD_EVIDENCE_V1', 'kind': kind,
                 'result': 'PASS_V2_M5_' + kind, 'promotionEligible': False, 'sourceTuple': None,
                 'inputRootSha256': inputs['inputRootSha256'], 'attachments': attachments,
                 'executionInputRoots': {stage: origins[stage] for stage in C.STAGES
                     if any(cases[identity]['executionStage'] == stage for identity in selected)},
                 'obligations': obligations, 'coreCases': core, 'summary': summary, 'exclusions': list(C.EXCLUSIONS)}
        ref = write(package, f'children/{kind}/receipt.json', value)
        children.append({'kind': kind, 'result': value['result'], **ref, 'summary': summary})
    execution_refs = [{'stage': stage, **reference(package, f'executions/{stage}/record.json')} for stage in C.STAGES]
    backend_digests = {}
    for stage in C.STAGES:
        record = C.load(C.read(package, f'executions/{stage}/record.json'))
        for selection in record['backendSelections']:
            backend_digests[stage + '/' + selection['boundary']] = selection['facts']['sha256']
    capacity_xml = []
    for suite in executions['NATIVE_KAFKA']['suites']:
        raw = C.read(package, suite['path'])
        if b'M5_CANONICAL_CAPACITY_BYTES ' in raw:
            capacity_xml.append({key: suite[key] for key in ('path', 'bytes', 'sha256')})
    C.require(capacity_xml, 'actual native fixed-capacity observations are missing')
    capacity = write(package, 'physical-capacity.json', {
        'schema': 'NEREUS_V2_M5_CAPACITY_SELECTION_V1', 'canonicalCapacityBytes': 500_000_000,
        'executionStage': 'NATIVE_KAFKA', 'rawEvidence': capacity_xml})
    pulsar_xml = [{key: suite[key] for key in ('path', 'bytes', 'sha256')}
                  for suite in executions['NATIVE_PULSAR']['suites']
                  if b'NATIVE_PULSAR_QUOTA ' in C.read(package, suite['path'])]
    C.require(pulsar_xml, 'actual Native Pulsar fixed-capacity and settlement observations are missing')
    pulsar_capacity = write(package, 'physical-capacity-pulsar.json', {
        'schema': 'NEREUS_V2_M5_CAPACITY_SELECTION_V1', 'canonicalCapacityBytes': 500_000_000,
        'executionStage': 'NATIVE_PULSAR', 'rawEvidence': pulsar_xml})
    # Capacity selection is postprocessing; preserve every original execution record byte.
    physical = write(package, 'physical-selection.json', {
        'schema': 'NEREUS_V2_M5_PHYSICAL_SELECTION_V1', 'inputRootSha256': inputs['inputRootSha256'],
        'backendSelectionDigests': backend_digests,
        'capacitySelections': [{'executionStage': 'NATIVE_KAFKA', 'facts': capacity},
                               {'executionStage': 'NATIVE_PULSAR', 'facts': pulsar_capacity}]})
    summary = {key: sum(execution['testSummary'][key] for execution in executions.values()) for key in C.JUNIT.COUNTERS}
    write(package, 'input-closure.json', inputs)
    write(package, 'candidate.json', {
        'schema': 'NEREUS_V2_M5_COMPLETE_CANDIDATE_V1', 'kind': 'V2_M5_COMPLETE_CANDIDATE',
        'result': 'PASS_V2_M5_COMPLETE_CANDIDATE_AWAITING_BINDING', 'promotionEligible': False,
        'executionEvidence': True, 'sourceTuple': None, 'inputRootSha256': inputs['inputRootSha256'],
        'executionInputRoots': origins,
        'sourceLocksSha256': C.source_node(inputs, 'nereus', 'docs/v2/source-locks.json')['sha256'],
        'm4Dependency': C.fixed_m4(), 'executionReceipts': execution_refs, 'childReceipts': children,
        'physicalSelection': physical, 'obligations': {key: {'owner': owner, 'result': 'VERIFIED_IN_COMPLETE_CANDIDATE'}
            for key, owner in C.OWNERS.items()} | {'SOURCE_CHILD_ARCHIVE_AGGREGATE': {
                'owner': 'AGGREGATE', 'result': 'VERIFIED_IN_COMPLETE_CANDIDATE'}},
        'scenarios': list(C.PROMOTED), 'sharedPredicates': list(C.SHARED_M4), 'deferredScenarios': list(C.DEFERRED),
        'openGates': ['V2-OPEN-READ-15'], 'exclusions': list(C.EXCLUSIONS), 'summary': summary})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-closure', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stage-components', action='store_true')
    parser.add_argument('--kafka-java-home', type=Path)
    parser.add_argument('--maven-repository', type=Path)
    args = parser.parse_args()
    prepared = args.input_closure.resolve(strict=True)
    inputs = C.INPUTS.verify(prepared)
    C.require(ROOT == (prepared / 'workspace' / C.INPUTS.LAYOUT['nereus']).resolve(),
              'complete campaign must execute its prepared source workspace')
    if args.stage_components:
        components(args.output.resolve(), prepared)
        return
    C.require(args.kafka_java_home is not None and args.maven_repository is not None,
              'explicit actual Kafka JDK and captured Maven repository are required')
    package = args.output.resolve()
    package.mkdir(parents=True, exist_ok=False)
    executions = {}
    source_run = None
    for stage in C.STAGES:
        executions[stage] = execute_stage(stage, package, prepared, inputs, args.kafka_java_home,
                                          args.maven_repository, source_run)
        if stage == 'BOOKKEEPER_OXIA':
            source_run = Path((package / 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').read_text().strip())
    assemble(package, inputs, executions)
    C.validate_candidate(package, prepared, ROOT)
    print('PASS_V2_M5_COMPLETE_CANDIDATE_AWAITING_BINDING', package, flush=True)


if __name__ == '__main__':
    try:
        main()
    except (C.EvidenceError, C.INPUTS.ClosureError, C.JUNIT.JUnitEvidenceError, C.M4.EvidenceError,
            C.HISTORICAL_M4.DependencyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print('M5 execution failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
