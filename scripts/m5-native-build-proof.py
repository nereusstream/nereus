#!/usr/bin/env python3
"""Validate the concrete, two-pass Native build that precedes an M5 input freeze."""

import io
from pathlib import Path
from zipfile import ZipFile


RECORD = 'build/m5-native-build-preparation.json'
PREFIX = 'source-continuation/native-build'
PULSAR_RECORD = 'build/m5-pulsar-build-preparation.json'
PULSAR_PREFIX = 'source-continuation/pulsar-build'
PULSAR_TESTS = (
    ':nereus-pulsar-offload:v2M5NativeOffloadRealTest', ':nereus-pulsar-offload:v2M5S3ProviderDrainRealTest',
    ':nereus-pulsar-offload:v2M5NativeM4OwnerRealTest', ':nereus-pulsar-offload:test', ':nereus-storage-bookkeeper:test',
    ':nereus-storage-object:v2M5RetentionCoreTest', ':nereus-storage-object:v2M5BindingAuthorityTest',
    ':nereus-storage-object:v2M5ClosedWriterIntegrationTest', ':nereus-storage-object:v2M5MaterializationWorkerTest',
    ':nereus-storage-object:v2M5TaskSelectionTest', ':nereus-storage-object:v2M5TargetDeleteAuthorityCoordinatorTest',
    ':nereus-storage-object:v2M5MultiWriterTicketTest', ':nereus-storage-object:v2M4ReadViewHazardEvidenceTest',
    ':nereus-storage-object:v2M4SourcePlanExecutionEvidenceTest',
    ':nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest',
    ':nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest', ':nereus-kafka-bookkeeper:v2M4CurrentSourceKafkaTest',
    ':nereus-pulsar-offload:v2M4CurrentSourcePulsarTest',
)



def allowed_missing(path, kafka):
    path, kafka = Path(path), Path(kafka)
    if not path.is_relative_to(kafka):
        return False
    parts = path.relative_to(kafka).parts
    return ('build' in parts or len(parts) >= 4 and parts[-3] == 'src'
            and parts[-2] in ('main', 'test') and parts[-1] in ('java', 'scala', 'resources'))


def pulsar_generated(path, workspace):
    path = Path(path)
    roots = (Path(workspace) / 'workspace/GITHUB/nereus', Path(workspace) / 'workspace/nereusstream/pulsar')
    for base in roots:
        if path.is_relative_to(base):
            parts = path.relative_to(base).parts
            return 'build' in parts and ('src' not in parts or parts.index('build') < parts.index('src'))
    return False


def allowed_pulsar_missing(path, workspace, task=None):
    path = Path(path)
    roots = (Path(workspace) / 'workspace/GITHUB/nereus', Path(workspace) / 'workspace/nereusstream/pulsar')
    if not any(path.is_relative_to(base) for base in roots):
        return False
    checkstyle_default = (path == roots[0] / 'config/checkstyle' and task in {
        ':nereus-pulsar-offload:checkstyleMain', ':nereus-pulsar-offload:checkstyleTest',
        ':nereus-pulsar-offload:checkstyleP6ProviderTest', ':nereus-storage-bookkeeper:checkstyleMain',
        ':nereus-storage-bookkeeper:checkstyleTest'}
        or path == roots[1] / 'config/checkstyle' and task in {
            ':managed-ledger:checkstyleMain', ':managed-ledger:checkstyleTest'})
    return (checkstyle_default or pulsar_generated(path, workspace)
            or len(path.parts) >= 3 and path.parts[-3] == 'src'
            and path.parts[-2] in ('main', 'test', 'p6ProviderTest')
            and path.parts[-1] in ('java', 'resources'))


def expand_project(value, require):
    schemas = {'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_V2': 'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_V1',
               'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_INPUTS_V2': 'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_INPUTS_V1'}
    require(value.get('schema') in schemas, 'unknown compact Native producer record')
    table = value.pop('files')
    require(isinstance(table, list) and len(table) <= 100_000
            and len({row['path'] for row in table}) == len(table), 'invalid Native producer file table')
    for row in table:
        require(set(row) == {'path', 'bytes', 'sha256'} and Path(row['path']).is_absolute()
                and type(row['bytes']) is int and 0 <= row['bytes'] <= 128 * 1024 * 1024
                and isinstance(row['sha256'], str) and len(row['sha256']) == 64
                and all(char in '0123456789abcdef' for char in row['sha256']),
                'invalid Native producer file identity')
    def node(index):
        require(type(index) is int and 0 <= index < len(table), 'invalid Native producer file reference')
        return table[index]
    for producer in value['producers']:
        for key in ('inputs', 'outputs', 'sources', 'classpath', 'compilerClasspath'):
            if key in producer:
                producer[key] = [node(index) for index in producer[key]]
        if 'compiler' in producer:
            producer['compiler']['executable'] = node(producer['compiler']['executable'])
    if 'runtime' in value:
        value['runtime'] = [node(index) for index in value['runtime']]
    value['schema'] = schemas[value['schema']]
    return value


def validate(root, reference, final, C, build_inputs=None):
    build_inputs = final if build_inputs is None else build_inputs
    C.require(reference['path'] == PREFIX + '/record.json', 'noncanonical Native preparation proof')
    raw = C.resource(root, reference, PREFIX)
    record = C.load(raw)
    C.exact(record, ('schema', 'executionEvidence', 'promotionEligible', 'workspace', 'toolchain',
                     'sourceExternalInputs', 'sourceInventorySha256', 'rootRuntimeArtifacts', 'publication',
                     'publicationLog', 'phases', 'inputsAndOutputs'), 'Native preparation')
    C.require(record['schema'] == 'NEREUS_V2_M5_NATIVE_BUILD_PREPARATION_V1'
              and record['executionEvidence'] is False and record['promotionEligible'] is False,
              'Native preparation cannot grant execution or promotion authority')
    C.require(record['sourceInventorySha256'] == C.INPUTS.digest(C.canonical(build_inputs['repositories'])),
              'Native build used another complete source inventory')
    workspace = Path(record['workspace'])
    C.require(workspace.is_absolute(), 'Native preparation workspace is not absolute')
    paths = {reference['path']}
    objects = {}

    def resource(item):
        rebased = dict(item, path=PREFIX + '/' + item['path'])
        data = C.resource(root, rebased, PREFIX)
        paths.add(rebased['path'])
        return data

    def object_bytes(item):
        C.exact(item, ('path', 'bytes', 'sha256'), 'Native producer file')
        key = item['sha256']
        if key not in objects:
            relative = PREFIX + '/preparation/raw-objects/' + key
            objects[key] = C.resource(root, {'path': relative, 'bytes': item['bytes'], 'sha256': key}, PREFIX)
            paths.add(relative)
        C.require(len(objects[key]) == item['bytes'], 'Native object has conflicting lengths')
        return objects[key]

    def logical(path):
        physical = Path(path)
        for name, repo in final['repositories'].items():
            base = workspace / 'workspace' / C.INPUTS.LAYOUT[name]
            if physical.is_relative_to(base):
                return str(Path(repo['sourceRoot']) / physical.relative_to(base))
        return str(physical)

    bootstrap = C.INPUTS.load(resource(record['sourceExternalInputs']))
    C.INPUTS.validate_value(bootstrap)
    C.require(bootstrap['repositories'] == build_inputs['repositories'], 'prebuild source/external checkpoint differs')
    manifests = {row['path'] for row in bootstrap['inputManifests']}
    C.require(len(manifests) == 1, 'Native preparation external inventory differs')
    for path, node in bootstrap['extraInputs'].items():
        if path in manifests:
            relative = PREFIX + '/preparation/bootstrap-only-objects/' + node['sha256']
            data = C.resource(root, dict(node, path=relative), PREFIX)
            paths.add(relative)
            inventory = C.INPUTS.load(data)
            C.require(set(inventory) == set(bootstrap['extraInputs']) - manifests,
                      'prebuild external dependency inventory is incomplete')
            C.require(all(bootstrap['extraInputs'][path]['sha256'] == digest for path, digest in inventory.items()),
                      'prebuild external manifest edge differs from its frozen input')
        else:
            C.require(build_inputs['extraInputs'].get(path) == node, 'prebuild changed a shared/external frozen input')
    C.require(set(record['toolchain']) == {'java', 'javac'}, 'missing actual Native JDK identity')
    for name, tool in record['toolchain'].items():
        C.exact(tool, ('path', 'bytes', 'sha256', 'version'), 'Native compiler identity')
        C.require(Path(tool['path']).is_absolute() and Path(tool['path']).name == name
                  and '17.0.' in tool['version'], 'Native build did not use the actual JDK17 toolchain')
    publication = record['publication']
    C.require(type(publication['exitCode']) is int and publication['exitCode'] == 0
              and C.integer(publication['finishedAtMicros'], 'publication finish', 1)
              > C.integer(publication['startedAtMicros'], 'publication start', 1)
              and publication['cwd'] == str(workspace / 'workspace/GITHUB/nereus'),
              'Root dependency publication did not complete before freeze')
    modules = {'nereus-storage-api', 'nereus-metadata-spi', 'nereus-storage-object',
               'nereus-storage-bookkeeper', 'nereus-kafka-bookkeeper', 'nereus-metadata-oxia'}
    C.require(set(record['rootRuntimeArtifacts']) == modules, 'Native preparation changed its Root runtime module set')
    original_source = Path(C.read(root, 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').decode().strip()).name
    original_summary = C.INPUTS.load(C.read(root,
        'executions/BOOKKEEPER_OXIA/raw/native-archives/m5-kafka-run-source/' + original_source + '/run-summary.json'))
    C.require(record['rootRuntimeArtifacts'] == original_summary['currentM4RuntimeArtifactSha256'],
              'Root runtime artifacts differ from the independently validated original Source qualification')
    publication_prefix = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
        '-PdevelopmentRepository=' + str(workspace / 'workspace/GITHUB/nereus/build/m5-evidence-development-repository')]
    C.require(publication['command'][:6] == publication_prefix and len(publication['command']) == 12
              and set(publication['command'][6:]) == {
                  ':' + module + ':publishMavenJavaPublicationToDevelopmentRepository' for module in modules},
              'Root dependency preparation invoked another producer recipe')
    publication_log = resource(record['publicationLog']).decode('utf-8')
    C.require('BUILD SUCCESSFUL' in publication_log and 'M5_NATIVE_PREPARATION_EXIT=0' in publication_log,
              'Root dependency publication has no successful actual completion')
    C.require(len(record['phases']) == 2, 'missing Native build stability precheck')
    graphs = []
    all_files = {}
    producers = {}
    runtime = {}
    before_inputs = []
    for phase_index, phase in enumerate(record['phases']):
        C.exact(phase, ('command', 'cwd', 'startedAtMicros', 'finishedAtMicros', 'exitCode', 'projects', 'log'),
                'Native build execution')
        C.require(phase['cwd'] == str(workspace / 'workspace/nereusstream/kafka')
                  and phase['command'][0] == './gradlew'
                  and phase['command'][-1] == ':core:m5CapturePreparedNativeArtifacts'
                  and type(phase['exitCode']) is int and phase['exitCode'] == 0
                  and C.integer(phase['finishedAtMicros'], 'build finish', 1)
                  > C.integer(phase['startedAtMicros'], 'build start', 1),
                  'Native preparation did not execute its actual producer graph')
        summary = [arg.split('=', 1)[1] for arg in phase['command'] if arg.startswith('-Pv2M5KafkaRunSummaryFile=')]
        C.require(len(summary) == 1 and Path(summary[0]).name == 'run-summary.json'
                  and Path(summary[0]).is_relative_to(workspace / 'workspace/GITHUB/nereus/build/m5-kafka-run-source'),
                  'Native preparation source prerequisite differs')
        source_root = workspace / 'workspace/GITHUB/nereus'
        expected_command = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
            '--init-script', str(source_root / 'config/v2/m5/evidence-native.init.gradle'),
            '--init-script', str(source_root / 'config/v2/m5/evidence-native-prebuild.init.gradle'),
            '-PmaxParallelForks=1', '-Pv2M5KafkaSealedGcProfile=true',
            '-PnereusDevelopmentRepository=' + str(source_root / 'build/m5-evidence-development-repository'),
            '-PnereusDevelopmentVersion=0.2.0-SNAPSHOT',
            '-PnereusLockedOxiaRepository=' + str(source_root / 'gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2'),
            '-Pv2M5KafkaRunSourceRoot=' + str(source_root), '-Pv2M5KafkaRunSummaryFile=' + summary[0],
            '-Dnereus.m5.nativeBuildObjects=' + str(workspace / 'preparation/raw-objects'),
            '-Dnereus.m5.nativeBuildCapture=' + str(workspace / 'preparation' / ('build' if phase_index == 0 else 'stability') / 'project'),
            ':core:m5CapturePreparedNativeArtifacts']
        C.require(phase['command'] == expected_command, 'Native preparation configuration differs')
        log = resource(phase['log']).decode('utf-8')
        C.require('BUILD SUCCESSFUL' in log and 'M5_NATIVE_PREPARATION_EXIT=0' in log,
                  'Native producer pass failed or did not complete')
        graph = []
        for item in phase['projects']:
            project = expand_project(C.INPUTS.load(resource(item)), C.require)
            if project['schema'] == 'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_INPUTS_V1':
                for producer in project['producers']:
                    C.require(all(allowed_missing(path, workspace / 'workspace/nereusstream/kafka')
                                  for path in producer['missingInputs']), 'missing external Native build dependency')
                    for node in producer['inputs']:
                        object_bytes(node)
                        before_inputs.append(node)
                continue
            C.require(project['schema'] == 'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_V1', 'unknown Native producer record')
            graph.append(project)
            for producer in project['producers']:
                C.require(producer['task'] not in producers or producers[producer['task']] == producer,
                          'Native producer changed between build and stability precheck')
                producers[producer['task']] = producer
                for node in (producer['inputs'] + producer['outputs'] + producer.get('compilerClasspath', [])
                             + producer.get('sources', []) + producer.get('classpath', [])):
                    object_bytes(node)
                    C.require(node['path'] not in all_files or all_files[node['path']] == node,
                              'Native producer graph reports conflicting file bytes')
                    all_files[node['path']] = node
                if 'compiler' in producer:
                    compiler = producer['compiler']
                    object_bytes(compiler['executable'])
                    C.require(compiler['language'] == 17 and compiler['executable'] == {
                        key: record['toolchain']['javac'][key] for key in ('path', 'bytes', 'sha256')},
                        'Native producer used another Java compiler')
            for node in project.get('runtime', []):
                object_bytes(node)
                runtime[node['path']] = node
        graphs.append(graph)
    C.require(graphs[0] == graphs[1], 'Native input/output graph drifted across the stability precheck')
    commands = [[arg for arg in phase['command'] if not arg.startswith('-Dnereus.m5.nativeBuildCapture=')]
                for phase in record['phases']]
    C.require(commands[0] == commands[1], 'Native stability precheck changed producer parameters')
    C.require(':core:compileNereusM5SourceFenceFixtures' in producers and runtime,
              'Native preparation omitted the actual source-fence fixtures/runtime classpath')
    for module, digest in record['rootRuntimeArtifacts'].items():
        actual = [node for path, node in runtime.items()
                  if Path(path).name.startswith(module + '-') and path.endswith('.jar')]
        C.require(len(actual) == 1 and actual[0]['sha256'] == digest,
                  'Native runtime classpath did not resolve the six exact qualified Root JARs')
    outputs = {node['path']: (producer, node) for producer in producers.values() for node in producer['outputs']}
    sources = {node['path']: node for producer in producers.values() for node in producer.get('sources', [])}
    native = workspace / 'workspace/nereusstream/kafka'
    for node in before_inputs:
        path = Path(node['path'])
        if not path.is_relative_to(native) or 'build' not in path.relative_to(native).parts:
            C.require(all_files.get(node['path']) == node, 'Native prebuild changed an existing source/external input')
    for path, node in sources.items():
        name = Path(logical(path))
        versioned = None
        for repo in final['repositories'].values():
            if name.is_relative_to(repo['sourceRoot']):
                versioned = repo['files'].get(name.relative_to(repo['sourceRoot']).as_posix())
                break
        if versioned is not None:
            C.require(versioned.get('sha256') == node['sha256'] and versioned.get('bytes') == node['bytes'],
                      'Native compiler source differs from the complete frozen source')
        else:
            C.require(path in outputs and outputs[path][1] == node, 'generated compiler source lacks its actual producer')
    provenance = record['inputsAndOutputs']
    C.require(isinstance(provenance, dict), 'Native preparation output inventory is absent')
    for name, item in provenance.items():
        C.exact(item, ('physicalPath', 'producer', 'bytes', 'sha256'), 'Native preparation origin')
        C.require(logical(item['physicalPath']) == name
                  and final['extraInputs'].get(name) == {key: item[key] for key in ('bytes', 'sha256')},
                  'Native generated input is not its exact final frozen file')
        if item['producer'] == 'ROOT_VERIFIED_PUBLICATION':
            relative = Path(item['physicalPath']).relative_to(workspace / 'workspace/GITHUB/nereus/build/m5-evidence-development-repository')
            C.require(len(relative.parts) >= 4 and relative.parts[:2] == ('com', 'nereusstream')
                      and relative.parts[2] in record['rootRuntimeArtifacts'], 'unknown Root development publication')
            if name.endswith('.jar') and not name.endswith('-sources.jar'):
                C.require(item['sha256'] == record['rootRuntimeArtifacts'][relative.parts[2]],
                          'Root runtime JAR changed during Native preparation')
            continue
        node = all_files.get(item['physicalPath']) or runtime.get(item['physicalPath'])
        if node is None:
            tool = next((tool for tool in record['toolchain'].values() if tool['path'] == item['physicalPath']), None)
            if tool is not None:
                node = {key: tool[key] for key in ('path', 'bytes', 'sha256')}
        C.require(node is not None and all(node[key] == item[key] for key in ('bytes', 'sha256')),
                  'Native input lacks an exact recorded producer/dependency')
        if item['producer'] is not None:
            C.require(item['physicalPath'] in outputs and outputs[item['physicalPath']][0]['task'] == item['producer'],
                      'Native artifact has a fabricated producer')
    record_path = str(Path(final['repositories']['nereus']['sourceRoot']) / RECORD)
    C.require(final['extraInputs'].get(record_path) == {key: reference[key] for key in ('bytes', 'sha256')},
              'Native preparation record was not frozen before testing')
    return {'record': record, 'provenance': provenance, 'outputs': outputs, 'sources': sources,
            'objects': objects, 'paths': paths, 'logical': logical,
            'rootSources': final['repositories']['nereus']['files'],
            'consumedInputs': {logical(path): node for path, node in {**all_files, **runtime}.items()}}


def validate_pulsar(root, reference, final, C):
    prefix = PULSAR_PREFIX
    C.require(reference['path'] == prefix + '/record.json', 'noncanonical Pulsar preparation proof')
    record = C.load(C.resource(root, reference, prefix))
    C.exact(record, ('schema', 'executionEvidence', 'promotionEligible', 'workspace', 'toolchain',
                    'sourceExternalInputs', 'sourceInventorySha256', 'nativeOriginInputRootSha256',
                    'tests', 'phases', 'inputsAndOutputs'), 'Pulsar preparation')
    C.require(record['schema'] == 'NEREUS_V2_M5_PULSAR_BUILD_PREPARATION_V1'
              and record['executionEvidence'] is False and record['promotionEligible'] is False
              and record['tests'] == list(PULSAR_TESTS)
              and record['nativeOriginInputRootSha256'] == C.EQUIVALENCE.NATIVE_ROOT,
              'Pulsar preparation authority or exact producer recipe differs')
    C.require(record['sourceInventorySha256'] == C.INPUTS.digest(C.canonical(final['repositories'])),
              'Pulsar producer source inventory differs from final source')
    workspace = Path(record['workspace'])
    C.require(workspace.is_absolute(), 'Pulsar preparation workspace is not absolute')
    paths, objects = {reference['path']}, {}
    def resource(item):
        rebased = dict(item, path=prefix + '/' + item['path'])
        data = C.resource(root, rebased, prefix)
        paths.add(rebased['path'])
        return data
    def object_bytes(item):
        key = item['sha256']
        if key not in objects:
            path = prefix + '/pulsar-preparation/raw-objects/' + key
            objects[key] = C.resource(root, dict(item, path=path), prefix)
            paths.add(path)
        C.require(len(objects[key]) == item['bytes'], 'Pulsar object lengths conflict')
        return objects[key]
    def logical(path):
        path = Path(path)
        for name, repo in final['repositories'].items():
            base = workspace / 'workspace' / C.INPUTS.LAYOUT[name]
            if path.is_relative_to(base):
                return str(Path(repo['sourceRoot']) / path.relative_to(base))
        return str(path)
    bootstrap = C.INPUTS.load(resource(record['sourceExternalInputs']))
    C.INPUTS.validate_value(bootstrap)
    C.require(bootstrap['repositories'] == final['repositories'], 'Pulsar prebuild source checkpoint differs')
    # Preserve every displaced bootstrap object; its original manifest and bytes remain independently readable.
    for node in bootstrap['extraInputs'].values():
        path = prefix + '/pulsar-preparation/bootstrap-only-objects/' + node['sha256']
        if (root / path).is_file():
            C.resource(root, dict(node, path=path), prefix); paths.add(path)
    C.require(set(record['toolchain']) == {'java', 'javac'}, 'Pulsar build toolchain is missing')
    for name, tool in record['toolchain'].items():
        C.require(Path(tool['path']).is_absolute() and Path(tool['path']).name == name
                  and '21.0.' in tool['version']
                  and final['extraInputs'].get(tool['path']) == {key: tool[key] for key in ('bytes', 'sha256')},
                  'Pulsar build did not retain its exact runtime JDK21 identity')
    C.require(len(record['phases']) == 2, 'Pulsar stability precheck is missing')
    source_root = workspace / 'workspace/GITHUB/nereus'
    pulsar_root = workspace / 'workspace/nereusstream/pulsar'
    expected = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--no-parallel', '--max-workers=2',
        '-PtestFailFast=false',
        '--init-script', str(source_root / 'config/v2/m5/evidence-native.init.gradle'),
        '--init-script', str(source_root / 'config/v2/m5/evidence-native-prebuild.init.gradle'),
        '-PpulsarCheckout=' + str(pulsar_root),
        '-Dnereus.m5.evidenceMavenRepository=' + str(source_root / 'build/m5-evidence-ant-repository'),
        '-Dnereus.m5.evidenceSdkTestingRepository=' + str(source_root / 'build/m5-native-pulsar-owner-local-dependencies'),
        '-Dnereus.m5.nativeBuildTarget=PULSAR', '-Dnereus.m5.nativeBuildTests=' + ','.join(PULSAR_TESTS),
        '-Dnereus.m5.nativeBuildObjects=' + str(workspace / 'pulsar-preparation/raw-objects')]
    graphs, all_files, producers, sources, runtime, tests, deferred = [], {}, {}, {}, {}, set(), set()
    for index, phase in enumerate(record['phases']):
        C.exact(phase, ('command', 'cwd', 'startedAtMicros', 'finishedAtMicros', 'exitCode', 'projects', 'log'),
                'actual Pulsar preparation execution')
        C.require(phase['command'] == expected + ['-Dnereus.m5.nativeBuildCapture=' + str(workspace /
                  'pulsar-preparation' / ('build' if index == 0 else 'stability') / 'project'),
                  ':nereus-pulsar-offload:m5CapturePreparedNativeArtifacts']
                  and phase['cwd'] == str(source_root) and type(phase['exitCode']) is int and phase['exitCode'] == 0
                  and C.integer(phase['finishedAtMicros'], 'Pulsar preparation finish', 1)
                  > C.integer(phase['startedAtMicros'], 'Pulsar preparation start', 1),
                  'Pulsar producer command or successful completion differs')
        log = resource(phase['log']).decode('utf-8')
        C.require('BUILD SUCCESSFUL' in log and 'M5_NATIVE_PREPARATION_EXIT=0' in log,
                  'Pulsar producer execution has no successful raw log')
        graph = []
        for item in phase['projects']:
            value = expand_project(C.INPUTS.load(resource(item)), C.require)
            if value['schema'].endswith('_INPUTS_V1'):
                for producer in value['producers']:
                    for node in producer['inputs']:
                        object_bytes(node)
                        physical = Path(node['path'])
                        generated = pulsar_generated(physical, workspace)
                        if not generated:
                            name = Path(logical(node['path']))
                            versioned = next((repo['files'].get(name.relative_to(repo['sourceRoot']).as_posix())
                                              for repo in final['repositories'].values()
                                              if name.is_relative_to(repo['sourceRoot'])), None)
                            frozen = versioned or final['extraInputs'].get(str(name))
                            C.require(frozen is not None and all(frozen[key] == node[key] for key in ('bytes', 'sha256')),
                                      'Pulsar prebuild changed a source/external input')
                    C.require(all(allowed_pulsar_missing(path, workspace, producer['task']) for path in producer['missingInputs']),
                              'Pulsar preparation had a missing external input')
                    C.require(all(pulsar_generated(path, workspace) for path in producer['deferredGeneratedInputs']),
                              'Pulsar capture deferred a source/external input')
                    deferred.update(producer['deferredGeneratedInputs'])
                continue
            C.require(value.get('build') in ('nereus', 'pulsar'), 'Pulsar producer build identity is missing')
            graph.append(value)
            for task in value['runtimeTasks']:
                tests.add((value['build'], task))
            for producer in value['producers']:
                C.require('org.gradle.api.tasks.testing.Test' not in producer['type'],
                          'Pulsar preparation ran an actual Test')
                producers[(value['build'], producer['task'])] = producer
                for key in ('inputs', 'outputs', 'sources', 'classpath', 'compilerClasspath'):
                    for node in producer.get(key, []):
                        object_bytes(node); all_files[node['path']] = node
                if 'compiler' in producer:
                    node = producer['compiler']['executable']
                    object_bytes(node); all_files[node['path']] = node
                for node in producer.get('sources', []):
                    sources[node['path']] = node
            for node in value.get('runtime', []):
                object_bytes(node); runtime[node['path']] = node
        graphs.append(graph)
    C.require(graphs[0] == graphs[1] and tests == {('nereus', task) for task in PULSAR_TESTS}
              | {('pulsar', ':managed-ledger:test')}, 'Pulsar producer graph/runtime drifted or omitted a required Test input')
    C.require(deferred <= set(all_files), 'deferred Pulsar generated input lacks its completed producer graph')
    outputs = {node['path']: (producer, node) for producer in producers.values() for node in producer['outputs']}
    for path, node in sources.items():
        versioned = None
        name = Path(logical(path))
        for repo in final['repositories'].values():
            if name.is_relative_to(repo['sourceRoot']):
                versioned = repo['files'].get(name.relative_to(repo['sourceRoot']).as_posix()); break
        C.require(versioned is not None and all(versioned[key] == node[key] for key in ('bytes', 'sha256'))
                  or path in outputs and outputs[path][1] == node, 'Pulsar compiler source lacks its exact final source/producer')
    provenance = record['inputsAndOutputs']
    for name, item in provenance.items():
        C.exact(item, ('physicalPath', 'producer', 'bytes', 'sha256'), 'Pulsar prepared file origin')
        C.require(logical(item['physicalPath']) == name
                  and final['extraInputs'].get(name) == {key: item[key] for key in ('bytes', 'sha256')},
                  'Pulsar prepared input differs from final frozen dependency')
        node = all_files.get(item['physicalPath']) or runtime.get(item['physicalPath'])
        if node is None:
            node = next(({key: tool[key] for key in ('path', 'bytes', 'sha256')}
                         for tool in record['toolchain'].values() if tool['path'] == item['physicalPath']), None)
        C.require(node is not None and all(node[key] == item[key] for key in ('bytes', 'sha256')),
                  'Pulsar input lacks its actual producer/dependency bytes')
        if item['producer'] is not None:
            C.require(item['physicalPath'] in outputs and outputs[item['physicalPath']][0]['task'] == item['producer'],
                      'Pulsar output has a fabricated producer')
    C.require(final['extraInputs'].get(str(Path(final['repositories']['nereus']['sourceRoot']) / PULSAR_RECORD))
              == {key: reference[key] for key in ('bytes', 'sha256')}, 'Pulsar preparation was not frozen')
    return {'record': record, 'provenance': provenance, 'outputs': outputs, 'sources': sources,
            'objects': objects, 'paths': paths, 'logical': logical, 'rootSources': final['repositories']['nereus']['files']}


def validate_artifact(path, raw, preparation, C):
    item = preparation['provenance'].get(path)
    C.require(item is not None and len(raw) == item['bytes'] and C.INPUTS.digest(raw) == item['sha256'],
              'changed input is not an exact artifact of the actual Native preparation')
    physical = item['physicalPath']
    outputs = preparation['outputs']
    producer = outputs.get(physical, ({}, None))[0]
    copied = any(all(node[key] == item[key] for key in ('bytes', 'sha256'))
                 for node in producer.get('inputs', []))
    if item['producer'] == 'ROOT_VERIFIED_PUBLICATION' and path.endswith('-sources.jar'):
        module = path.split('/build/m5-evidence-development-repository/', 1)[1].split('/')[2]
        with ZipFile(io.BytesIO(raw)) as archive:
            for name in archive.namelist():
                C.relative(name.rstrip('/'))
                C.require(not name.endswith('.class'), 'Root source classifier contains runtime classes')
                if name.endswith('.java'):
                    source = preparation['rootSources'].get(module + '/src/main/java/' + name)
                    data = archive.read(name)
                    C.require(source is not None and source['bytes'] == len(data)
                              and source['sha256'] == C.INPUTS.digest(data),
                              'published Root source classifier differs from the frozen source')
        return
    if path.endswith('.class'):
        C.require(physical in outputs, 'Native class lacks its actual compiler output')
        if 'classDirectory' not in producer and copied:
            return
        C.require('classDirectory' in producer, 'Native class was not produced by an actual compile task')
        binary = Path(physical).relative_to(producer['classDirectory']).as_posix().removesuffix('.class')
        source, declared = C.NATIVE_INPUTS.class_source(raw, C)
        C.require(declared == binary and any(Path(node['path']).name == source for node in producer['sources']),
                  'Native class does not name an actual source of its exact compiler task')
    elif path.endswith('.jar') and item['producer'] not in (None, 'ROOT_VERIFIED_PUBLICATION'):
        if item['producer'] is not None:
            if producer.get('archive') != physical and copied:
                return
            C.require(physical in outputs and producer.get('archive') == physical,
                      'Native JAR is not an actual archive task output')
        with ZipFile(io.BytesIO(raw)) as archive:
            names = [node.filename for node in archive.infolist()]
            C.require(len(names) == len(set(names)), 'Native archive has duplicate entries')
            for name in names:
                C.relative(name.rstrip('/'))
                if name.endswith('.class'):
                    _, binary = C.NATIVE_INPUTS.class_source(archive.read(name), C)
                    actual = name.split('/', 3)[-1] if name.startswith('META-INF/versions/') else name
                    C.require(binary == actual.removesuffix('.class'), 'Native archive class identity differs')
