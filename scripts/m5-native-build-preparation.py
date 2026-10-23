#!/usr/bin/env python3
"""Prepare a fresh complete M5 workspace, or explicitly replay a historical preparation."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location('m5_native_preparation_inputs', ROOT / 'scripts/m5-candidate-inputs.py')
I = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(I)
BUILD_SPEC = importlib.util.spec_from_file_location('m5_native_preparation_proof', ROOT / 'scripts/m5-native-build-proof.py')
B = importlib.util.module_from_spec(BUILD_SPEC)
BUILD_SPEC.loader.exec_module(B)
FLAT = 'build/m5-evidence-c3-complete-runtime-v2.json'
RECORD = 'build/m5-native-build-preparation.json'
INIT = 'config/v2/m5/evidence-native-prebuild.init.gradle'
PULSAR_TESTS = B.PULSAR_TESTS
ROOT_MODULES = ('nereus-storage-api', 'nereus-metadata-spi', 'nereus-storage-object',
                'nereus-storage-bookkeeper', 'nereus-kafka-bookkeeper', 'nereus-metadata-oxia')
FRESH_RECORD = 'build/m5-fresh-build-preparation.json'


def require(value, message):
    if not value:
        raise I.ClosureError(message)


def reference(root, path):
    raw = I.regular(path)
    return {'path': path.relative_to(root).as_posix(), 'bytes': len(raw), 'sha256': I.digest(raw)}


def qualify_source(old_package, old_prepared, root, inputs):
    original = Path((old_package / 'executions/BOOKKEEPER_OXIA/raw/current-source-run.txt').read_text().strip())
    require(original.is_relative_to(old_prepared / 'workspace/GITHUB/nereus/build/m5-kafka-run-source'),
            'source qualifier is outside the original v5 execution')
    archive = old_package / 'executions/BOOKKEEPER_OXIA/raw/native-archives/m5-kafka-run-source' / original.name
    expected = {p.relative_to(archive).as_posix(): I.regular(p) for p in archive.rglob('*') if p.is_file()}
    manifest = I.load(expected['tested-inputs.json'])
    for path, digest in manifest.items():
        raw = I.regular(root / I.relative(path))
        require(I.digest(raw) == digest == inputs['repositories']['nereus']['files'][path]['sha256'],
                'source qualifier differs from the frozen source')
        expected['source-archive/sources/' + path] = raw
    destination = root / 'build/m5-kafka-run-source' / original.name
    if not destination.exists():
        for path, raw in expected.items():
            I.write_new(destination / I.relative(path), raw)
    actual = {p.relative_to(destination).as_posix(): I.regular(p) for p in destination.rglob('*') if p.is_file()}
    require(actual == expected, 'prepared source qualifier changed or has unregistered files')
    legacy = Path((destination / 'legacy-run-path.txt').read_text().strip())
    require(legacy.is_relative_to(old_prepared / 'workspace/GITHUB/nereus/build/m5-bookkeeper-task-terminal'),
            'source qualifier changed its original legacy origin')
    return destination


def logical(path, prepared, inputs):
    path = Path(path)
    for name, repository in inputs['repositories'].items():
        base = prepared / 'workspace' / I.LAYOUT[name]
        if path.is_relative_to(base):
            return str(Path(repository['sourceRoot']) / path.relative_to(base))
    return str(path)


def current_sources(prepared, inputs):
    for name, expected in inputs['repositories'].items():
        actual = I.repository(prepared / 'workspace' / I.LAYOUT[name])
        actual['sourceRoot'] = expected['sourceRoot']
        require(actual == expected, 'source changed during Native preparation: ' + name)


def execute(command, cwd, env, log):
    start = time.time_ns() // 1000
    with log.open('x') as stream:
        code = subprocess.run(command, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT).returncode
        stream.write(f'\nM5_NATIVE_PREPARATION_EXIT={code}\n')
    require(code == 0, 'Native preparation failed; original log retained: ' + str(log))
    return {'command': command, 'cwd': str(cwd), 'startedAtMicros': start,
            'finishedAtMicros': time.time_ns() // 1000, 'exitCode': code}


def prepare_pulsar(args):
    spec = importlib.util.spec_from_file_location('m5_pulsar_source_capture', ROOT / 'scripts/continue-v2-m5-evidence.py')
    continuation = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(continuation)
    prior_prepared = args.pulsar_from_v11.resolve(strict=True)
    prior = I.verify(prior_prepared)
    prepared = args.candidate.resolve()
    bootstrap = continuation.prepare_postprocessing_source(prior_prepared, prepared)
    preparation = prepared / 'pulsar-preparation'
    preparation.mkdir()
    (prepared / 'input-closure.json').rename(preparation / 'source-external-input-closure.json')
    root = prepared / 'workspace/GITHUB/nereus'
    pulsar = prepared / 'workspace/nereusstream/pulsar'
    env = dict(os.environ, JAVA_HOME=str(args.java_home.resolve(strict=True)))
    env['PATH'] = env['JAVA_HOME'] + '/bin:' + env['PATH']
    toolchain = {}
    for tool in ('java', 'javac'):
        path = args.java_home.resolve(strict=True) / 'bin' / tool
        toolchain[tool] = {'path': str(path), 'bytes': path.stat().st_size, 'sha256': I.digest(I.regular(path)),
                          'version': subprocess.check_output([str(path), '-version'], stderr=subprocess.STDOUT,
                                                             text=True).strip()}
    base = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--no-parallel', '--max-workers=2',
            '-PtestFailFast=false',
            '--init-script', str(root / 'config/v2/m5/evidence-native.init.gradle'),
            '--init-script', str(root / INIT), '-PpulsarCheckout=' + str(pulsar),
            '-Dnereus.m5.evidenceMavenRepository=' + str(root / 'build/m5-evidence-ant-repository'),
            '-Dnereus.m5.evidenceSdkTestingRepository=' + str(root / 'build/m5-native-pulsar-owner-local-dependencies'),
            '-Dnereus.m5.nativeBuildTarget=PULSAR', '-Dnereus.m5.nativeBuildTests=' + ','.join(PULSAR_TESTS),
            '-Dnereus.m5.nativeBuildObjects=' + str(preparation / 'raw-objects')]
    phases, captures = [], []
    for phase in ('build', 'stability'):
        prefix = preparation / phase / 'project'
        command = base + ['-Dnereus.m5.nativeBuildCapture=' + str(prefix),
                          ':nereus-pulsar-offload:m5CapturePreparedNativeArtifacts']
        execution = execute(command, root, env, preparation / (phase + '.log'))
        paths = sorted(prefix.parent.glob('project-*.json'))
        require(paths, 'Pulsar producer capture is absent')
        graph = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' not in path.name]
        before = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' in path.name]
        current_sources(prepared, bootstrap)
        execution['projects'] = [reference(prepared, path) for path in paths]
        execution['log'] = reference(prepared, preparation / (phase + '.log'))
        phases.append(execution)
        captures.append((graph, before))
    require(captures[0][0] == captures[1][0], 'Pulsar producer graph/output bytes drift across stability precheck')
    records, outputs = {}, {}
    for project in captures[1][0]:
        for producer in project['producers']:
            for node in producer['inputs'] + producer['outputs'] + producer.get('compilerClasspath', []):
                records[node['path']] = node
            if 'compiler' in producer:
                node = producer['compiler']['executable']; records[node['path']] = node
            for node in producer['outputs']:
                outputs[node['path']] = producer['task']
        for node in project.get('runtime', []):
            records[node['path']] = node
    generated_roots = (root, pulsar)
    def generated(path):
        return B.pulsar_generated(path, prepared)
    for project in captures[0][1]:
        for producer in project['producers']:
            for node in producer['inputs']:
                path = Path(node['path'])
                if not generated(path):
                    require(I.digest(I.regular(path)) == node['sha256'], 'Pulsar prebuild source/external input changed')
            for name in producer['missingInputs']:
                require(B.allowed_pulsar_missing(name, prepared, producer['task']),
                        'missing external input before Pulsar compilation: ' + name)
    flat_path = str(Path(bootstrap['repositories']['nereus']['sourceRoot']) / FLAT)
    extras = {path: node for path, node in bootstrap['extraInputs'].items() if path != flat_path}
    provenance = {}
    for physical, node in records.items():
        name = logical(physical, prepared, bootstrap)
        versioned = next((repo['files'].get(Path(name).relative_to(repo['sourceRoot']).as_posix())
                          for repo in bootstrap['repositories'].values() if Path(name).is_relative_to(repo['sourceRoot'])), None)
        if versioned is not None:
            require(versioned.get('sha256') == node['sha256'], 'Pulsar compiler source differs from complete frozen source')
            continue
        old = extras.get(name)
        if old is not None and old != {key: node[key] for key in ('bytes', 'sha256')}:
            require(generated(Path(physical)), 'Pulsar build changed an external dependency')
        extras[name] = {key: node[key] for key in ('bytes', 'sha256')}
        provenance[name] = {'physicalPath': physical, 'producer': outputs.get(physical), **extras[name]}
    for node in toolchain.values():
        extras[node['path']] = {key: node[key] for key in ('bytes', 'sha256')}
        provenance[node['path']] = {'physicalPath': node['path'], 'producer': None, **extras[node['path']]}
    record = {'schema': 'NEREUS_V2_M5_PULSAR_BUILD_PREPARATION_V1', 'executionEvidence': False,
              'promotionEligible': False, 'workspace': str(prepared), 'toolchain': toolchain,
              'sourceExternalInputs': reference(prepared, preparation / 'source-external-input-closure.json'),
              'sourceInventorySha256': I.digest(I.canonical(bootstrap['repositories'])),
              'nativeOriginInputRootSha256': prior['inputRootSha256'], 'tests': list(PULSAR_TESTS),
              'phases': phases, 'inputsAndOutputs': provenance}
    I.write_new(root / B.PULSAR_RECORD, I.canonical(record))
    data = I.regular(root / B.PULSAR_RECORD)
    extras[str(Path(bootstrap['repositories']['nereus']['sourceRoot']) / B.PULSAR_RECORD)] = {
        'bytes': len(data), 'sha256': I.digest(data)}
    (root / FLAT).write_bytes(I.canonical({path: node['sha256'] for path, node in extras.items()}))
    data = I.regular(root / FLAT)
    extras[flat_path] = {'bytes': len(data), 'sha256': I.digest(data)}
    for name, node in extras.items():
        path = Path(name)
        for repository, source in bootstrap['repositories'].items():
            if path.is_relative_to(source['sourceRoot']):
                path = prepared / 'workspace' / I.LAYOUT[repository] / path.relative_to(source['sourceRoot'])
                break
        data = I.regular(path)
        require(len(data) == node['bytes'] and I.digest(data) == node['sha256'], 'Pulsar input differs before final freeze')
        I.store(prepared, data)
    final = dict(bootstrap, extraInputs=extras)
    final['inputRootSha256'] = I.digest(I.canonical({key: final[key] for key in
        ('repositories', 'extraInputs', 'inputManifests')}))
    I.validate_value(final)
    needed = {node['sha256'] for node in extras.values()}
    needed.update(node['sha256'] for repo in final['repositories'].values() for node in repo['files'].values() if 'sha256' in node)
    for path in (prepared / 'objects').iterdir():
        if path.name not in needed:
            target = preparation / 'bootstrap-only-objects' / path.name
            target.parent.mkdir(exist_ok=True); path.rename(target)
    I.write_new(prepared / 'input-closure.json', I.canonical(final))
    I.verify(prepared)
    return final


def prepare(args):
    roots = {name: getattr(args, name + '_root').resolve(strict=True) for name in I.REPOSITORIES}
    prepared = args.candidate.resolve()
    old_prepared = args.prior_input_closure.resolve(strict=True)
    old = I.verify(old_prepared)
    require(old['inputRootSha256'] == '297dc4ef5c0fe1e38e8c5876a5932525fbc4ed9d7d2837b3cef1989714bbc9e7',
            'preparation must retain the original v5 source prerequisite')
    old_package = args.prior_package.resolve(strict=True)
    original_flat = I.load(I.regular(roots['nereus'] / FLAT))
    native = roots['kafka']
    # Generated Native outputs are produced here, not imported from the shared checkout.
    external = {path: digest for path, digest in original_flat.items()
                if not (Path(path).is_relative_to(native) and 'build' in Path(path).relative_to(native).parts)}
    bootstrap_path = roots['nereus'] / ('build/m5-native-bootstrap-' + prepared.name + '.json')
    I.write_new(bootstrap_path, I.canonical(external))
    bootstrap = I.prepare(roots, [('nereus', bootstrap_path)], prepared)
    preparation = prepared / 'preparation'
    preparation.mkdir()
    (prepared / 'input-closure.json').rename(preparation / 'source-external-input-closure.json')
    root = prepared / 'workspace' / I.LAYOUT['nereus']
    kafka = prepared / 'workspace' / I.LAYOUT['kafka']
    source = qualify_source(old_package, old_prepared, root, bootstrap)
    summary = I.load(I.regular(source / 'run-summary.json'))
    modules = summary['currentM4RuntimeArtifactSha256']
    repository = root / 'build/m5-evidence-development-repository'
    env = dict(os.environ, JAVA_HOME=str(args.java_home.resolve(strict=True)))
    env['PATH'] = env['JAVA_HOME'] + '/bin:' + env['PATH']
    publish = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
               '-PdevelopmentRepository=' + str(repository),
               *[':' + name + ':publishMavenJavaPublicationToDevelopmentRepository' for name in modules]]
    publication = execute(publish, root, env, preparation / 'root-publication.log')
    for module, digest in modules.items():
        jar = root / module / 'build/libs' / (module + '-0.2.0-SNAPSHOT.jar')
        require(I.digest(I.regular(jar)) == digest, 'Root runtime JAR changed during Native preparation: ' + module)
    current_sources(prepared, bootstrap)
    native_env = dict(env, JAVA_HOME=str(args.kafka_java_home.resolve(strict=True)))
    native_env['PATH'] = native_env['JAVA_HOME'] + '/bin:' + env['PATH']
    toolchain = {}
    for tool in ('java', 'javac'):
        executable = args.kafka_java_home.resolve(strict=True) / 'bin' / tool
        toolchain[tool] = {'path': str(executable), 'bytes': executable.stat().st_size,
                           'sha256': I.digest(I.regular(executable)),
                           'version': subprocess.check_output([str(executable), '-version'],
                                      stderr=subprocess.STDOUT, text=True).strip()}
    base = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
            '--init-script', str(root / 'config/v2/m5/evidence-native.init.gradle'),
            '--init-script', str(root / INIT), '-PmaxParallelForks=1', '-Pv2M5KafkaSealedGcProfile=true',
            '-PnereusDevelopmentRepository=' + str(repository), '-PnereusDevelopmentVersion=0.2.0-SNAPSHOT',
            '-PnereusLockedOxiaRepository=' + str(root / 'gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2'),
            '-Pv2M5KafkaRunSourceRoot=' + str(root), '-Pv2M5KafkaRunSummaryFile=' + str(source / 'run-summary.json'),
            '-Dnereus.m5.nativeBuildObjects=' + str(preparation / 'raw-objects')]
    phases = []
    captures = []
    for phase in ('build', 'stability'):
        prefix = preparation / phase / 'project'
        command = base + ['-Dnereus.m5.nativeBuildCapture=' + str(prefix), ':core:m5CapturePreparedNativeArtifacts']
        execution = execute(command, kafka, native_env, preparation / (phase + '.log'))
        paths = sorted(prefix.parent.glob('project-*.json'))
        require(paths, 'Native producer capture is absent')
        graph = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' not in path.name]
        before = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' in path.name]
        current_sources(prepared, bootstrap)
        execution['projects'] = [reference(prepared, path) for path in paths]
        execution['log'] = reference(prepared, preparation / (phase + '.log'))
        phases.append(execution)
        captures.append((graph, before))
    require(captures[0][0] == captures[1][0], 'Native producer graph/output bytes remain unstable before testing')
    graph, before = captures[1]
    records = {}
    producer_by_output = {}
    for project in graph:
        for producer in project['producers']:
            for item in producer['inputs'] + producer['outputs'] + producer.get('compilerClasspath', []):
                records[item['path']] = item
            if 'compiler' in producer:
                item = producer['compiler']['executable']
                records[item['path']] = item
            for item in producer['outputs']:
                producer_by_output[item['path']] = producer['task']
        for item in project.get('runtime', []):
            records[item['path']] = item
    # Every actually present input was captured before its producing build. Source/generated
    # dependencies are then resolved by the actual producer graph; other files cannot change.
    for project in captures[0][1]:
        for producer in project['producers']:
            for item in producer['inputs']:
                path = Path(item['path'])
                if not path.is_relative_to(kafka / '.') or 'build' not in path.relative_to(kafka).parts:
                    require(I.digest(I.regular(path)) == item['sha256'], 'prebuild source/external input changed')
            require(all(B.allowed_missing(path, kafka) for path in producer['missingInputs']),
                    'missing external input before Native compilation')
    manifest = dict(external)
    # The bootstrap inventory itself is preparation provenance, not a new runtime dependency.
    provenance = {}
    for path, item in records.items():
        name = logical(path, prepared, bootstrap)
        versioned = next((repo['files'].get(str(Path(name).relative_to(repo['sourceRoot'])))
                          for repo in bootstrap['repositories'].values() if Path(name).is_relative_to(repo['sourceRoot'])), None)
        if versioned is not None:
            require(versioned.get('sha256') == item['sha256'], 'compiled source differs from frozen source')
            continue
        if name in manifest:
            require(manifest[name] == item['sha256'], 'shared external/Root/Pulsar input changed during preparation')
        manifest[name] = item['sha256']
        provenance[name] = {'physicalPath': path, 'producer': producer_by_output.get(path),
                            'bytes': item['bytes'], 'sha256': item['sha256']}
    for path in sorted(repository.rglob('*')):
        if path.is_file():
            raw = I.regular(path)
            name = logical(path, prepared, bootstrap)
            digest = I.digest(raw)
            if name in manifest:
                require(manifest[name] == digest, 'published Root dependency changed')
            manifest[name] = digest
            provenance[name] = {'physicalPath': str(path), 'producer': 'ROOT_VERIFIED_PUBLICATION',
                                'bytes': len(raw), 'sha256': digest}
    for tool in toolchain.values():
        manifest[tool['path']] = tool['sha256']
        provenance[tool['path']] = {'physicalPath': tool['path'], 'producer': None,
                                    'bytes': tool['bytes'], 'sha256': tool['sha256']}
    record = {'schema': 'NEREUS_V2_M5_NATIVE_BUILD_PREPARATION_V1', 'executionEvidence': False,
              'promotionEligible': False, 'workspace': str(prepared), 'toolchain': toolchain,
              'sourceExternalInputs': reference(prepared, preparation / 'source-external-input-closure.json'),
              'sourceInventorySha256': I.digest(I.canonical(bootstrap['repositories'])),
              'rootRuntimeArtifacts': modules, 'publication': publication,
              'publicationLog': reference(prepared, preparation / 'root-publication.log'),
              'phases': phases, 'inputsAndOutputs': provenance}
    I.write_new(root / RECORD, I.canonical(record))
    manifest[str(roots['nereus'] / RECORD)] = I.digest(I.regular(root / RECORD))
    return freeze_inputs(prepared, bootstrap, manifest, preparation)


def freeze_inputs(prepared, bootstrap, manifest, preparation):
    root = prepared / "workspace" / I.LAYOUT["nereus"]
    roots = {name: Path(repo["sourceRoot"]) for name, repo in bootstrap["repositories"].items()}
    I.write_new(root / FLAT, I.canonical(manifest))
    extras = {}
    for path, digest in manifest.items():
        materialized = Path(path)
        for name, repository_source in roots.items():
            if materialized.is_relative_to(repository_source):
                materialized = prepared / 'workspace' / I.LAYOUT[name] / materialized.relative_to(repository_source)
                break
        raw = I.regular(materialized)
        require(I.digest(raw) == digest, 'generated final dependency differs before freeze')
        extras[path] = {'bytes': len(raw), 'sha256': digest}
        I.store(prepared, raw)
    raw = I.regular(root / FLAT)
    extras[str(roots['nereus'] / FLAT)] = {'bytes': len(raw), 'sha256': I.digest(raw)}
    I.store(prepared, raw)
    final = dict(bootstrap, extraInputs=extras,
                 inputManifests=[{'repository': 'nereus', 'path': str(roots['nereus'] / FLAT)}])
    final['inputRootSha256'] = I.digest(I.canonical({key: final[key] for key in ('repositories', 'extraInputs', 'inputManifests')}))
    I.validate_value(final)
    expected = {row['sha256'] for row in extras.values()}
    expected.update(row['sha256'] for repo in final['repositories'].values() for row in repo['files'].values() if 'sha256' in row)
    for path in (prepared / 'objects').iterdir():
        if path.name not in expected:
            target = preparation / 'bootstrap-only-objects' / path.name
            target.parent.mkdir(exist_ok=True)
            path.rename(target)
    I.write_new(prepared / 'input-closure.json', I.canonical(final))
    I.verify(prepared)
    return final


def actual_tool(path, major, java=False):
    path = path.resolve(strict=True)
    raw = I.regular(path)
    require(os.access(path, os.X_OK), 'tool is not executable: ' + str(path))
    version = subprocess.check_output([str(path), '-version' if java else '--version'],
                                      stderr=subprocess.STDOUT, text=True).strip()
    require(re.search(r'(?<!\d)' + re.escape(major) + r'\.', version) is not None,
            'wrong tool version: ' + str(path) + ': ' + version)
    return {'path': str(path), 'bytes': len(raw), 'sha256': I.digest(raw), 'version': version}


def fresh_preflight(args):
    prepared = args.candidate.resolve()
    require('build' not in prepared.parts, 'fresh workspace must avoid a /build/ ancestor for SDK quality checks')
    require(not prepared.exists() and not prepared.is_symlink(), 'fresh destination already exists')
    require(Path(args.python_executable).resolve(strict=True) == Path(sys.executable).resolve(strict=True),
            'explicit Python differs from the executing preparation runtime')
    tools = {'python': actual_tool(args.python_executable, '3.14')}
    for label, home, major in (('root', args.java_home, '21'), ('kafka', args.kafka_java_home, '17')):
        tools[label] = {name: actual_tool(home / 'bin' / name, major, True) for name in ('java', 'javac')}
    for source in (args.maven_repository, args.sdk_testing_repository):
        source = source.resolve(strict=True)
        require(source.is_dir() and any(source.rglob('*.pom')) and any(source.rglob('*.jar')),
                'dependency repository requires actual Maven POM and JAR files: ' + str(source))
        require(not prepared.is_relative_to(source) and not source.is_relative_to(prepared),
                'dependency repository overlaps fresh destination')
    return tools


def copy_dependencies(source, destination):
    source = source.resolve(strict=True)
    files = []
    total = 0
    for path in source.rglob('*'):
        require(not path.is_symlink(), 'dependency repository contains a symlink: ' + str(path))
        if path.is_dir():
            continue
        size = path.stat().st_size
        files.append(path)
        total += size
        require(size <= I.MAX_FILE_BYTES and total <= I.MAX_TOTAL_BYTES and len(files) <= I.MAX_ENTRIES,
                'dependency repository exceeds the input bound')
    require(files, 'dependency repository is empty')
    for path in sorted(files):
        I.write_new(destination / path.relative_to(source), I.regular(path))


def capture_build(command, cwd, env, directory, prepared, bootstrap):
    directory.mkdir()
    prefix = directory / 'project'
    run = execute(command + ['-Dnereus.m5.nativeBuildCapture=' + str(prefix),
                            ':nereus-pulsar-offload:m5CapturePreparedNativeArtifacts'
                            if cwd.name == 'nereus' else ':core:m5CapturePreparedNativeArtifacts'],
                  cwd, env, directory / 'gradle.log')
    paths = sorted(directory.glob('project-*.json'))
    require(paths, 'fresh producer capture is absent')
    graph = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' not in path.name]
    before = [B.expand_project(I.load(I.regular(path)), require) for path in paths if '-before-' in path.name]
    require(graph and before, 'fresh producer before/after inventory is incomplete')
    current_sources(prepared, bootstrap)
    run['projects'] = [reference(prepared, path) for path in paths]
    run['log'] = reference(prepared, directory / 'gradle.log')
    return run, graph, before


def java_compile_state_audits(graphs, prepared):
    """Keep exact Gradle incremental state as build audit, never classify other unconsumed outputs."""
    consumed, pending = set(), []
    for project in graphs:
        consumed.update(node['path'] for node in project.get('runtime', []))
        for producer in project['producers']:
            for field in ('inputs', 'sources', 'classpath', 'compilerClasspath'):
                consumed.update(node['path'] for node in producer.get(field, []))
            if 'compiler' in producer:
                consumed.add(producer['compiler']['executable']['path'])
            pending.extend(Path(path) for field in ('missingInputs', 'deferredGeneratedInputs')
                           for path in producer.get(field, []))
    roots = {'nereus': prepared / 'workspace' / I.LAYOUT['nereus'],
             'pulsar': prepared / 'workspace' / I.LAYOUT['pulsar'],
             'kafka': prepared / 'workspace' / I.LAYOUT['kafka']}
    result = set()
    for project in graphs:
        root = roots.get(project.get('build', 'kafka'))
        for producer in project['producers']:
            if producer.get('type') != 'org.gradle.api.tasks.compile.JavaCompile_Decorated':
                continue
            owner = Path(producer.get('projectDirectory', ''))
            temporary = Path(producer.get('temporaryDirectory', ''))
            task = producer['task'].rsplit(':', 1)[-1]
            prefix = '' if project['project'] == ':' else project['project']
            if (root is None or not owner.is_absolute() or not owner.is_relative_to(root)
                    or not task or producer['task'] != prefix + ':' + task
                    or temporary != owner / 'build/tmp' / task):
                continue
            expected = temporary / 'previous-compilation-data.bin'
            for node in producer.get('outputs', []):
                path = Path(node['path'])
                if (path == expected and str(path) not in consumed
                        and not any(path == item or path.is_relative_to(item) for item in pending)
                        and path.is_file() and not path.is_symlink()):
                    result.add(str(path))
    return result


def fresh_records(graph, before, prepared):
    records, outputs = {}, {}
    root = prepared / 'workspace' / I.LAYOUT['nereus']
    kafka = prepared / 'workspace' / I.LAYOUT['kafka']
    for project in before:
        for producer in project['producers']:
            for node in producer['inputs']:
                path = Path(node['path'])
                generated = B.pulsar_generated(path, prepared) or path.is_relative_to(kafka) and 'build' in path.relative_to(kafka).parts
                if not generated:
                    require(I.digest(I.regular(path)) == node['sha256'], 'fresh source/external input changed')
            for name in producer['missingInputs']:
                path = Path(name)
                source_set = (path.is_relative_to(root) and len(path.relative_to(root).parts) == 4
                              and path.relative_to(root).parts[1] == 'src'
                              and path.relative_to(root).parts[2] in {'realBookKeeperTest', 'realProviderTest', 'oxiaIntegrationTest'}
                              and path.name in {'java', 'resources'})
                require(B.allowed_missing(path, kafka) or B.allowed_pulsar_missing(path, prepared, producer['task'])
                        or source_set, 'missing external input before fresh compilation: ' + name)
    for project in graph:
        for producer in project['producers']:
            require('org.gradle.api.tasks.testing.Test' not in producer['type'], 'fresh preparation executed a Test')
            for node in producer['inputs'] + producer['outputs'] + producer.get('compilerClasspath', []):
                require(node['path'] not in records or records[node['path']] == node, 'fresh producer file has conflicting identities')
                records[node['path']] = node
            if 'compiler' in producer:
                node = producer['compiler']['executable']; records[node['path']] = node
            for node in producer['outputs']:
                outputs[node['path']] = producer['task']
        for node in project.get('runtime', []):
            require(node['path'] not in records or records[node['path']] == node, 'fresh runtime file has conflicting identities')
            records[node['path']] = node
    require(records, 'fresh preparation captured no actual inputs or outputs')
    return records, outputs


def prepare_fresh(args):
    tools = fresh_preflight(args)
    roots = {name: getattr(args, name + '_root').resolve(strict=True) for name in I.REPOSITORIES}
    prepared = args.candidate.resolve()
    # The bootstrap contains actual tools, not an inherited runtime inventory or an execution receipt.
    tool_nodes = [tools['python'], *tools['root'].values(), *tools['kafka'].values()]
    with tempfile.TemporaryDirectory(prefix='nereus-m5-fresh-bootstrap-') as temporary:
        manifest_path = Path(temporary) / 'tools.json'
        manifest_path.write_bytes(I.canonical({node['path']: node['sha256'] for node in tool_nodes}))
        bootstrap = I.prepare(roots, [('nereus', manifest_path)], prepared)
    preparation = prepared / 'preparation'
    preparation.mkdir()
    (prepared / 'input-closure.json').rename(preparation / 'source-external-input-closure.json')
    root = prepared / 'workspace' / I.LAYOUT['nereus']
    kafka = prepared / 'workspace' / I.LAYOUT['kafka']
    pulsar = prepared / 'workspace' / I.LAYOUT['pulsar']
    ant = root / 'build/m5-evidence-ant-repository'
    sdk = root / 'build/m5-native-pulsar-owner-local-dependencies'
    copy_dependencies(args.maven_repository, ant)
    copy_dependencies(args.sdk_testing_repository, sdk)
    repository = root / 'build/m5-evidence-development-repository'
    env = dict(os.environ, JAVA_HOME=str(args.java_home.resolve(strict=True)))
    env['PATH'] = str(args.python_executable.absolute().parent) + ':' + env['JAVA_HOME'] + '/bin:' + env['PATH']
    nested_python = shutil.which('python3', path=env['PATH'])
    require(nested_python is not None and Path(nested_python).resolve() == args.python_executable.resolve(),
            'nested python3 differs from the explicit runtime')
    common = ['./gradlew', '--no-daemon', '--no-configuration-cache', '--no-parallel', '--max-workers=2']
    publication = execute(common + ['-PdevelopmentRepository=' + str(repository),
        *[':' + name + ':publishMavenJavaPublicationToDevelopmentRepository' for name in ROOT_MODULES]],
        root, env, preparation / 'root-publication.log')
    modules = {name: I.digest(I.regular(root / name / 'build/libs' / (name + '-0.2.0-SNAPSHOT.jar')))
               for name in ROOT_MODULES}
    init = ['--init-script', str(root / 'config/v2/m5/evidence-native.init.gradle'),
            '--init-script', str(root / INIT), '-Dnereus.m5.nativeBuildObjects=' + str(preparation / 'raw-objects')]
    root_command = common + init + ['-PtestFailFast=false', '-PpulsarCheckout=' + str(pulsar),
        '-Dnereus.m5.evidenceMavenRepository=' + str(ant), '-Dnereus.m5.evidenceSdkTestingRepository=' + str(sdk),
        '-Dnereus.m5.nativeBuildTarget=COMPLETE']
    kafka_command = common + init + ['-PmaxParallelForks=1', '-Pv2M5KafkaSealedGcProfile=true',
        '-PnereusDevelopmentRepository=' + str(repository), '-PnereusDevelopmentVersion=0.2.0-SNAPSHOT',
        '-PnereusLockedOxiaRepository=' + str(root / 'gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2'),
        '-Pv2M5KafkaRunSourceRoot=' + str(root)]
    # No Source/Legacy summary exists yet. The complete runner supplies its real BK/Oxia result only at Test execution.
    native_env = dict(env, JAVA_HOME=str(args.kafka_java_home.resolve(strict=True)))
    native_env['PATH'] = native_env['JAVA_HOME'] + '/bin:' + env['PATH']
    captures = {'root': [], 'kafka': []}
    for phase in ('build', 'stability'):
        for label, command, cwd, process_env in (('root', root_command, root, env),
                                                ('kafka', kafka_command, kafka, native_env)):
            if phase == 'stability':
                command = [command[0], '--offline', *command[1:]]
            captures[label].append(capture_build(command, cwd, process_env, preparation / (phase + '-' + label),
                                                 prepared, bootstrap))
    records, outputs = {}, {}
    for label, runs in captures.items():
        require(runs[0][1] == runs[1][1], 'fresh producer graph/output bytes drift: ' + label)
        observed, produced = fresh_records(runs[1][1], runs[0][2], prepared)
        for name, node in observed.items():
            require(name not in records or records[name] == node, 'fresh builds disagree about a shared input')
            records[name] = node
        outputs.update(produced)
    # Include both passes and all three builds when checking external consumers of compiler state.
    audit_states = java_compile_state_audits(
        [project for runs in captures.values() for run in runs for group in run[1:] for project in group], prepared)
    provenance, manifest = {}, {}
    for physical, node in records.items():
        name = logical(physical, prepared, bootstrap)
        source = next((repo['files'].get(Path(name).relative_to(repo['sourceRoot']).as_posix())
                       for repo in bootstrap['repositories'].values() if Path(name).is_relative_to(repo['sourceRoot'])), None)
        if source is not None:
            require(source.get('sha256') == node['sha256'], 'fresh compiler source differs from captured source')
            continue
        require(len(I.regular(Path(physical))) == node['bytes'] and I.digest(I.regular(Path(physical))) == node['sha256'],
                'fresh captured file changed before freeze')
        if physical in audit_states:
            # The original producer graph and bytes remain immutable evidence. Later compilations own the
            # mutable task-local state path; actual source/class/JAR/tool/runtime paths remain frozen below.
            audit = preparation / 'raw-objects' / node['sha256']
            if not audit.exists():
                I.write_new(audit, I.regular(Path(physical)))
            require(I.digest(I.regular(audit)) == node['sha256'], 'JavaCompile audit bytes differ')
            manifest[str(audit)] = node['sha256']
            continue
        manifest[name] = node['sha256']
        provenance[name] = {'physicalPath': physical, 'producer': outputs.get(physical), **{key: node[key] for key in ('bytes', 'sha256')}}
    record = {'schema': 'NEREUS_V2_M5_NATIVE_BUILD_PREPARATION_V1', 'executionEvidence': False,
        'promotionEligible': False, 'workspace': str(prepared), 'toolchain': tools['kafka'],
        'sourceExternalInputs': reference(prepared, preparation / 'source-external-input-closure.json'),
        'sourceInventorySha256': I.digest(I.canonical(bootstrap['repositories'])), 'rootRuntimeArtifacts': modules,
        'publication': publication, 'publicationLog': reference(prepared, preparation / 'root-publication.log'),
        'phases': [run[0] for run in captures['kafka']], 'inputsAndOutputs': provenance}
    I.write_new(root / RECORD, I.canonical(record))
    I.write_new(root / FRESH_RECORD, I.canonical({'schema': 'NEREUS_V2_M5_FRESH_BUILD_PREPARATION_V1',
        'executionEvidence': False, 'promotionEligible': False, 'workspace': str(prepared), 'toolchain': tools,
        'sourceInventorySha256': record['sourceInventorySha256'], 'nativePreparation': RECORD,
        'phases': {label: [run[0] for run in runs] for label, runs in captures.items()}}))
    for directory in (repository, ant, sdk):
        for path in sorted(directory.rglob('*')):
            if path.is_file():
                manifest[logical(path, prepared, bootstrap)] = I.digest(I.regular(path))
    for name, digest in modules.items():
        path = root / name / 'build/libs' / (name + '-0.2.0-SNAPSHOT.jar')
        require(I.digest(I.regular(path)) == digest, 'fresh Root runtime artifact changed after publication')
        manifest[logical(path, prepared, bootstrap)] = digest
    for path in (root / RECORD, root / FRESH_RECORD, *sorted(preparation.rglob('*.json')),
                 *sorted(preparation.rglob('*.log'))):
        manifest[logical(path, prepared, bootstrap)] = I.digest(I.regular(path))
    for node in tool_nodes:
        manifest[node['path']] = node['sha256']
    current_sources(prepared, bootstrap)
    return freeze_inputs(prepared, bootstrap, manifest, preparation)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('fresh', 'historical-kafka', 'historical-pulsar'), default='fresh')
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--prior-input-closure', type=Path)
    parser.add_argument('--prior-package', type=Path)
    for name in I.REPOSITORIES:
        parser.add_argument('--' + name + '-root', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, required=True)
    parser.add_argument('--kafka-java-home', type=Path, required=True)
    parser.add_argument('--pulsar-from-v11', type=Path)
    parser.add_argument('--python-executable', type=Path)
    parser.add_argument('--maven-repository', type=Path)
    parser.add_argument('--sdk-testing-repository', type=Path)
    args = parser.parse_args(argv)
    if args.mode == 'fresh':
        if args.prior_input_closure or args.prior_package or args.pulsar_from_v11:
            parser.error('fresh preparation rejects historical inputs; select an explicit historical mode')
        if not all((args.python_executable, args.maven_repository, args.sdk_testing_repository)):
            parser.error('fresh preparation requires --python-executable, --maven-repository and --sdk-testing-repository')
    elif args.mode == 'historical-kafka':
        if not args.prior_input_closure or not args.prior_package or args.pulsar_from_v11:
            parser.error('historical-kafka requires both original prior inputs and package')
    elif not args.pulsar_from_v11:
        parser.error('historical-pulsar requires its original --pulsar-from-v11 inputs')
    return args


def main(argv=None):
    args = parse_args(argv)
    value = (prepare_fresh(args) if args.mode == 'fresh' else
             prepare_pulsar(args) if args.mode == 'historical-pulsar' else prepare(args))
    print('M5_NATIVE_PREPARED_INPUTS_ONLY', value['inputRootSha256'], flush=True)


if __name__ == '__main__':
    main()
