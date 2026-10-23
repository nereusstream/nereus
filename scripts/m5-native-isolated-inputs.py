#!/usr/bin/env python3
"""Exact unused Native Kafka artifacts admitted by the v5 continuation decision; no directory-wide exemption."""

import hashlib
import io
import re
import struct
import xml.etree.ElementTree as ET
from pathlib import Path
from zipfile import ZipFile


MAIN = 'core/src/main/java/kafka/server/nereus/NereusKafkaNativeInputV2.java'
TEST = 'core/src/test/java/kafka/server/nereus/NereusKafkaNativeInputReleaseTest.java'
CONFIG = 'checkstyle/import-control-core.xml'
SOURCES = {MAIN, TEST, CONFIG}


def input_differences(prior, final):
    old, new = prior['extraInputs'], final['extraInputs']
    return [{'path': path, 'before': old.get(path), 'after': new.get(path)}
            for path in sorted(set(old) | set(new)) if old.get(path) != new.get(path)]


def pulsar_origin(path, prior, C):
    root = Path(prior['repositories']['nereus']['sourceRoot'])
    pulsar = Path(prior['repositories']['pulsar']['sourceRoot'])
    if path in {row['path'] for row in prior['inputManifests']}:
        return 'INPUT_MANIFEST'
    if path == str(root / C.NATIVE_BUILD.PULSAR_RECORD):
        return 'PULSAR_BUILD_RECORD'
    physical = Path(path)
    if any(physical.is_relative_to(base) and 'build' in physical.relative_to(base).parts for base in (root, pulsar)):
        return 'PULSAR_BUILD_OUTPUT'
    if path not in prior['extraInputs'] and not any(physical.is_relative_to(repo['sourceRoot'])
                                                  for repo in prior['repositories'].values()):
        return 'PULSAR_BUILD_OUTPUT'
    raise C.EvidenceError('postprocessing changed an actual dependency or generated input: ' + path)


def check_pulsar_inventory(prior, final, C):
    delta = input_differences(prior, final)
    if delta:
        C.require(str(Path(final['repositories']['nereus']['sourceRoot']) / C.NATIVE_BUILD.PULSAR_RECORD)
                  in final['extraInputs'], 'postprocessing changed an actual dependency or generated input')
    for row in delta:
        pulsar_origin(row['path'], prior, C)
        C.require(row['after'] is not None, 'Pulsar preparation removed an existing frozen input')
    return delta


def origin(path, prior, C, built=False, pulsar=False):
    base = prior['repositories']['kafka']['sourceRoot'].rstrip('/') + '/'
    manifests = {row['path'] for row in prior['inputManifests']}
    if path in manifests:
        return 'INPUT_MANIFEST'
    root = prior['repositories']['nereus']['sourceRoot'].rstrip('/') + '/'
    if built and path == root + C.NATIVE_BUILD.RECORD:
        return 'NATIVE_BUILD_RECORD'
    if built and path.startswith(root + 'build/m5-evidence-development-repository/'):
        return 'NATIVE_BUILD_OUTPUT'
    if pulsar and not path.startswith(base):
        return pulsar_origin(path, prior, C)
    if built and not path.startswith(base) and path not in prior['extraInputs']:
        C.require(not any(Path(path).is_relative_to(repo['sourceRoot']) for repo in prior['repositories'].values()),
                  'new shared repository input is not a Native build output')
        return 'NATIVE_BUILD_OUTPUT'
    C.require(path.startswith(base), 'changed runtime input is not an isolated Native dependency: ' + path)
    relative = path[len(base):]
    if re.fullmatch(r'core/build/classes/scala/main/kafka/server/nereus/NereusKafkaNativeInputV2(?:\$[^/]+)?\.class', relative):
        return MAIN
    if re.fullmatch(r'core/build/classes/scala/test/kafka/server/nereus/NereusKafkaNativeInputReleaseTest(?:\$[^/]+)?\.class', relative):
        return TEST
    if relative == 'core/build/libs/kafka_2.13-4.3.0-SNAPSHOT.jar':
        return 'NATIVE_MAIN_JAR'
    if built and 'build' in Path(relative).parts:
        # This is only an inventory shape check. Acceptance below requires the exact file,
        # task and source/toolchain in the actual two-pass build proof, never this directory.
        return 'NATIVE_BUILD_OUTPUT'
    raise C.EvidenceError('unapproved Native artifact difference: ' + relative)


def check_inventory(prior, final, C):
    C.require(prior['inputManifests'] == final['inputManifests'], 'input manifest identities changed')
    delta = input_differences(prior, final)
    built = str(Path(final['repositories']['nereus']['sourceRoot']) / C.NATIVE_BUILD.RECORD) in final['extraInputs']
    pulsar = str(Path(final['repositories']['nereus']['sourceRoot']) / C.NATIVE_BUILD.PULSAR_RECORD) in final['extraInputs']
    for row in delta:
        origin(row['path'], prior, C, built, pulsar)
        C.require(row['after'] is not None or built and Path(row['path']).is_relative_to(prior['repositories']['kafka']['sourceRoot']),
                  'isolated Native dependency was removed')
    if delta:
        C.require(any(origin(row['path'], prior, C, built, pulsar) == MAIN for row in delta),
                  'changed runtime input/manifest lacks the approved Native runtime repair')
    return delta


def check_stage_usage(delta, record, C):
    changed = {row['path'] for row in delta}
    C.require(not changed.intersection(row['sourcePath'] for row in record['runtimeInputs']),
              'reused stage actually loaded a changed input')


def check_native_sources(before, after, C):
    C.require(MAIN in before and MAIN in after and TEST in after and CONFIG in before and CONFIG in after,
              'Native repair lacks exact production/test/config provenance')
    old, new = ET.fromstring(before[CONFIG]), ET.fromstring(after[CONFIG])
    added = [node for node in new.iter('file') if node.get('name') == r'NereusKafkaNativeInputReleaseTest(\.java)?']
    C.require(len(added) == 1 and added[0].attrib == {'name': r'NereusKafkaNativeInputReleaseTest(\.java)?', 'regex': 'true'}
              and len(added[0]) == 1 and added[0][0].tag == 'allow'
              and added[0][0].attrib == {'class': 'com.nereusstream.storage.object.control.CanonicalControlMetadataStore'},
              'Native test import rule is not confined to the exact release fixture')
    for parent in new.iter():
        if added[0] in list(parent):
            parent.remove(added[0])
            break
    def signature(node):
        return node.tag, sorted(node.attrib.items()), (node.text or '').strip(), [signature(child) for child in node]
    C.require(signature(old) == signature(new), 'Native production import rules or another test configuration changed')


def class_source(raw, C):
    """Read the class-level SourceFile and declared binary name, with all skips bounded by actual bytes."""
    offset = 0
    def take(size):
        nonlocal offset
        C.require(size >= 0 and offset + size <= len(raw), 'truncated Native class provenance')
        value = raw[offset:offset + size]
        offset += size
        return value
    def u2():
        return struct.unpack('>H', take(2))[0]
    def u4():
        return struct.unpack('>I', take(4))[0]
    C.require(take(4) == b'\xca\xfe\xba\xbe', 'invalid Native class file')
    take(4)
    pool = [None] * u2()
    index = 1
    while index < len(pool):
        tag = take(1)[0]
        if tag == 1:
            pool[index] = take(u2())
        elif tag in (7, 8, 16, 19, 20):
            pool[index] = (tag, u2())
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            take(4)
        elif tag in (5, 6):
            take(8)
            index += 1
        elif tag == 15:
            take(3)
        else:
            raise C.EvidenceError('unsupported Native class constant')
        index += 1
    take(2)
    this_class = u2()
    take(2)
    take(2 * u2())
    for _ in range(2):
        for _ in range(u2()):
            take(6)
            for _ in range(u2()):
                take(2)
                take(u4())
    source = None
    for _ in range(u2()):
        name, length = u2(), u4()
        if pool[name] == b'SourceFile':
            C.require(length == 2 and source is None, 'invalid Native SourceFile attribute')
            source = pool[u2()].decode('utf-8', errors='strict')
        else:
            take(length)
    C.require(offset == len(raw) and isinstance(source, str) and pool[this_class][0] == 7,
              'Native class provenance is incomplete')
    return source, pool[pool[this_class][1]].decode('utf-8', errors='strict')


def require_class(raw, path, source, C):
    filename, declared = class_source(raw, C)
    C.require(filename == source.rsplit('/', 1)[1] and declared == path.removesuffix('.class'),
              'Native compiled artifact does not derive from its exact approved source')


def validate_objects(root, rows, delta, prior, final, C, preparation=None, pulsar_preparation=None):
    C.require(isinstance(rows, list) and len(rows) == len(delta), 'missing Native artifact provenance objects')
    objects = {}
    paths = set()
    for row, expected in zip(rows, delta):
        C.exact(row, ('path', 'before', 'after'), 'Native artifact provenance')
        C.require(row['path'] == expected['path'], 'Native artifact provenance inventory differs')
        pair = []
        for key in ('before', 'after'):
            record = expected[key]
            reference = row[key]
            if record is None:
                C.require(reference is None, 'invented previous Native artifact')
                pair.append(None)
            else:
                prefix = 'source-continuation/native-inputs'
                for prepared, candidate_prefix, folder in ((preparation, C.NATIVE_BUILD.PREFIX, 'preparation'),
                        (pulsar_preparation, C.NATIVE_BUILD.PULSAR_PREFIX, 'pulsar-preparation')):
                    if (prepared is not None and key == 'after' and reference['path'] in prepared['paths']
                            and reference['path'] in (candidate_prefix + '/' + folder + '/raw-objects/' + reference['sha256'],
                                                     candidate_prefix + '/record.json')):
                        prefix = candidate_prefix
                raw = C.resource(root, reference, prefix)
                C.require({field: reference[field] for field in ('bytes', 'sha256')} == record,
                          'Native provenance bytes differ from the complete dependency inventory')
                paths.add(reference['path'])
                pair.append(raw)
        objects[row['path']] = pair
        kind = origin(row['path'], prior, C, preparation is not None, pulsar_preparation is not None)
        if (kind == 'PULSAR_BUILD_OUTPUT' and preparation is not None
                and row['path'] in preparation['provenance']):
            # A newly inventoried external dependency can belong to the earlier Kafka build.
            kind = 'NATIVE_BUILD_OUTPUT'
        if kind in ('PULSAR_BUILD_RECORD', 'PULSAR_BUILD_OUTPUT'):
            C.require(pulsar_preparation is not None, 'Pulsar output has no actual stable build provenance')
            if kind == 'PULSAR_BUILD_OUTPUT':
                C.require(pair[1] is not None, 'Pulsar output was removed')
                C.NATIVE_BUILD.validate_artifact(row['path'], pair[1], pulsar_preparation, C)
            continue
        if preparation is not None and kind not in ('INPUT_MANIFEST', 'NATIVE_BUILD_RECORD'):
            if pair[1] is not None:
                C.NATIVE_BUILD.validate_artifact(row['path'], pair[1], preparation, C)
            else:
                C.require(row['path'] not in preparation['provenance'] and row['path'] not in final['extraInputs'],
                          'removed stale Native output is still a declared final dependency')
            continue
        if kind in (MAIN, TEST):
            binary = row['path'].split('/kafka/server/nereus/', 1)[1]
            for raw in pair:
                if raw is not None:
                    require_class(raw, 'kafka/server/nereus/' + binary, kind, C)
        elif kind == 'NATIVE_MAIN_JAR':
            C.require(all(raw is not None for raw in pair), 'Native archive origin is missing')
            with ZipFile(io.BytesIO(pair[0])) as old, ZipFile(io.BytesIO(pair[1])) as new:
                before = {info.filename: old.read(info) for info in old.infolist() if not info.is_dir()}
                after = {info.filename: new.read(info) for info in new.infolist() if not info.is_dir()}
            C.require(set(before) == set(after), 'Native main archive entry inventory changed')
            changed = [name for name in before if before[name] != after[name]]
            C.require(changed, 'Native archive byte change lacks an approved class change')
            for name in changed:
                C.require(re.fullmatch(r'kafka/server/nereus/NereusKafkaNativeInputV2(?:\$[^/]+)?\.class', name),
                          'Native archive changed another source entry')
                require_class(before[name], name, MAIN, C)
                require_class(after[name], name, MAIN, C)
                class_path = prior['repositories']['kafka']['sourceRoot'] + '/core/build/classes/scala/main/' + name
                C.require(class_path in objects and objects[class_path] == [before[name], after[name]],
                          'Native archive class differs from its exact compiled input objects')
    # The same logical flat manifest is retained. Its only altered edges are the exact approved
    # unused Native artifacts; Root/Pulsar/cached dependency entries cannot be changed or dropped.
    native_paths = {row['path'] for row in delta if origin(row['path'], prior, C, preparation is not None,
                                                        pulsar_preparation is not None) != 'INPUT_MANIFEST'}
    for row in delta:
        if origin(row['path'], prior, C, preparation is not None, pulsar_preparation is not None) != 'INPUT_MANIFEST':
            continue
        old_raw, new_raw = objects[row['path']]
        old, new = C.INPUTS.load(old_raw), C.INPUTS.load(new_raw)
        changed = {path for path in set(old) | set(new) if old.get(path) != new.get(path)}
        C.require(changed == native_paths, 'dependency manifest changed other edges or omitted Native changes')
        for path in set(old) | set(new):
            C.require(old.get(path) == (prior['extraInputs'][path]['sha256'] if path in prior['extraInputs'] else None)
                      and new.get(path) == (final['extraInputs'][path]['sha256'] if path in final['extraInputs'] else None),
                      'dependency manifest edge differs from its actual complete cohort')
    return paths
