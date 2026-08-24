#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

projection=docs/v2/wire/nwg1-v1.json
manifest=docs/v2/wire/nwg1-v1-golden-manifest.json
tsv=docs/v2/wire/nwg1-v1-goldens.tsv

for required in \
  "$projection" "$manifest" "$tsv" \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1HeaderCodecV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1DirectoryCodecV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/GroupEncodingPlanV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectWriterV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectReaderV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1CryptoV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1EnvelopeV1.java \
  nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java; do
  test -s "$required" || { echo "missing NWG1 A artifact: $required" >&2; exit 1; }
done

if rg -n 'GENERATE|PLACEHOLDER|TODO' nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1 \
    nereus-storage-object/src/test/java/com/nereusstream/storage/object/nwg1 "$projection" "$manifest" "$tsv"; then
  echo "NWG1 A contains a forbidden dynamic placeholder" >&2
  exit 1
fi

python3 - "$projection" "$manifest" "$tsv" <<'PY'
import csv,hashlib,json,re,sys
projection_path,manifest_path,tsv_path=sys.argv[1:]
p=json.load(open(projection_path,encoding='utf-8'))
fields=p['headerFields']
expected=[
('magic',0,4),('wireVersion',4,2),('headerLength',6,2),('headerFlags',8,4),('protocolKind',12,2),
('requiredZeroA',14,2),('shardId',16,4),('shardRunEpoch',20,8),('laneSequence',28,8),
('packingPolicyVersion',36,4),('resolvedTargetPayloadBytes',40,8),('resolvedLingerNanos',48,8),
('requiredZeroB',56,4),('actualPayloadBytesAtPlanSeal',60,8),('actualCloseLingerNanos',68,8),
('directoryPlaintextLength',76,4),('directoryStoredLength',80,4),('bindingContextCount',84,4),
('appendUnitCount',88,4),('frameCount',92,4),('directoryPrefixEnd',96,8),('canonicalBodyLength',104,8),
('laneId',112,1),('frameCodecRegistryKind',113,1),('frameCodecRegistryVersion',114,1),
('objectDigestKind',115,1),('objectDigestVersion',116,1),('aeadKind',117,1),('aeadVersion',118,1),
('kdfKind',119,1),('kdfVersion',120,1),('nonceLayoutVersion',121,1),('aeadTagBytes',122,1),
('actualCloseReason',123,1),('protocolCellCommitment',124,32),('cellProviderScopeId',156,32),
('walRunRootSha256',188,32),('wrappedEnvelopeCommitment',220,32),('crc32c',252,4)]
actual=[(x['name'],x['offset'],x['bytes']) for x in fields]
assert actual==expected,(actual,expected)
assert all(expected[i][1]+expected[i][2]==expected[i+1][1] for i in range(len(expected)-1))
assert expected[-1][1]+expected[-1][2]==256
assert [(x['offset'],x['bytes']) for x in p['requiredZeroRegions']]==[(8,4),(14,2),(56,4)]
assert p['fixedSizes']=={'aeadTag':16,'bindingContextRow':116,'commonFrameRow':48,'directoryAad':272,
 'directoryPreamble':32,'frameAad':328,'header':256,'kafkaAppendUnitRow':104,'nonce':12,
 'objectKeyInfo':37,'pulsarAppendUnitRow':96}
def exact_fields(name, expected, width):
    actual=[(x['name'],x['offset'],x['bytes']) for x in p[name]]
    assert actual==expected,(name,actual,expected)
    assert expected[0][1]==0
    assert all(expected[i][1]+expected[i][2]==expected[i+1][1] for i in range(len(expected)-1))
    assert expected[-1][1]+expected[-1][2]==width
exact_fields('directoryPreambleFields',[
 ('magic',0,4),('directoryVersion',4,2),('preambleLength',6,2),('protocolKind',8,2),
 ('directoryFlags',10,2),('bindingContextCount',12,4),('appendUnitCount',16,4),
 ('frameCount',20,4),('nti1BlobBytes',24,4),('directoryPlaintextLength',28,4)],32)
exact_fields('bindingContextRowFields',[
 ('bindingId',0,32),('storageEpochId',32,32),('ownerFenceCommitment',64,32),
 ('nti1BlobOffset',96,4),('nti1Length',100,4),('ownerFenceKind',104,2),
 ('ownerFenceVersion',106,2),('positionDomainKind',108,2),('positionDomainVersion',110,2),
 ('framePolicyKind',112,2),('framePolicyVersion',114,2)],116)
exact_fields('kafkaAppendUnitRowFields',[
 ('contextOrdinal',0,4),('firstFrameOrdinal',4,4),('frameCount',8,4),('partitionId',12,4),
 ('kafkaLeaderEpoch',16,4),('reservedZero',20,4),('startOffset',24,8),
 ('endOffsetExclusive',32,8),('appendCommitSetId',40,16),('storageAttemptId',56,16),
 ('assignedPayloadSha256',72,32)],104)
exact_fields('pulsarAppendUnitRowFields',[
 ('contextOrdinal',0,4),('firstFrameOrdinal',4,4),('frameCount',8,4),('reservedZero',12,4),
 ('virtualLedgerId',16,8),('entryId',24,8),('appendCommitSetId',32,16),
 ('storageAttemptId',48,16),('assignedPayloadSha256',64,32)],96)
exact_fields('commonFrameRowFields',[
 ('appendUnitOrdinal',0,4),('storedBlockBytes',4,4),('storedBodyOffset',8,8),
 ('decodedPayloadBytes',16,4),('payloadCrc32c',20,4),('coverage0',24,8),('coverage1',32,8),
 ('actualCodecKind',40,2),('actualCodecVersion',42,2),('payloadChecksumKind',44,2),
 ('payloadChecksumVersion',46,2)],48)
assert p['absoluteCaps']=={
 'maxCanonicalBodyBytes':4294967296,'maxDirectoryPrefixBytes':4194304,
 'maxDirectoryPlaintextBytes':4194032,'maxBindingContexts':256,'maxAppendUnits':65536,
 'maxFrames':65536,'maxNti1Bytes':8214,'maxDecodedFrameBytes':67108864,'maxStoredFrameBytes':67108880,
 'maxTotalDecodedPayloadBytes':4294967296}
assert p['closedCodeTables']=={
 'protocolKind':{'KAFKA':1,'PULSAR':2},
 'permanentPackingClass':{'OBJECT_LATENCY':0,'BALANCED':1,'COST':2},
 'actualCloseReason':{
  'OBJECT_BODY_CAP':1,'DIRECTORY_CAP':2,'APPEND_UNIT_CAP':3,'FRAME_CAP':4,
  'EARLIEST_REQUEST_DEADLINE':5,'HANDOFF':6,'RUN_STOP':7,'POLICY_CHANGE':8,
  'RESOURCE_PRESSURE':9,'EXPLICIT_FLUSH':10,'TARGET_BYTES':11,'LINGER_EXPIRED':12},
 'frameCodec':{'NONE':{'kind':0,'version':0},'ZSTD_STANDARD_FRAME':{'kind':1,'version':1}},
 'payloadChecksum':{'CRC32C':{'kind':1,'version':1}},
 'objectDigest':{'SHA256':{'kind':1,'version':1}},
 'aead':{'AES_256_GCM':{'kind':1,'version':1,'tagBytes':16}},
 'kdf':{'RFC5869_HKDF_SHA256':{'kind':1,'version':1}},
 'nonceLayout':{'NWG1_NONCE_LAYOUT':{'version':1}}}

raw=open(manifest_path,'rb').read()
assert 0<len(raw)<=1_048_576,'manifest exceeds dedicated 1 MiB cap'
m=json.loads(raw)
canonical=json.dumps(m,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
assert raw==canonical,'manifest is not exact RFC-8785/JCS for its closed integer/string schema'
assert len(m['vectors'])==6 and len(m['externalFixtures'])==2
assert len(m['componentInventory'])==16 and len(set(m['componentInventory']))==16
assert len(m['mutations'])==84 and len(m['mutationOperations'])==10 and len(m['resignOperations'])==8
assert len(m['zstdFixtures'])==2
for fixture in m['zstdFixtures']:
    assert fixture['provenanceTool'].startswith('zstd-cli-1.5.7')
    assert hashlib.sha256(bytes.fromhex(fixture['frameHex'])).digest()!=bytes(32)

with open(tsv_path,newline='',encoding='ascii') as f: rows=list(csv.DictReader(f,delimiter='\t'))
assert len(rows)==114,len(rows)
assert list(rows[0])==['vectorId','componentKind','ordinal','length','sha256','hex']
keys=set(); kinds=set(); vectors=set()
for row in rows:
    key=(row['vectorId'],row['componentKind'],int(row['ordinal']))
    assert key not in keys; keys.add(key)
    raw=bytes.fromhex(row['hex'])
    assert row['hex']==row['hex'].lower() and re.fullmatch(r'[0-9a-f]*',row['hex'])
    assert len(raw)==int(row['length'])
    assert hashlib.sha256(raw).hexdigest()==row['sha256']
    kinds.add(row['componentKind']); vectors.add(row['vectorId'])
assert kinds==set(m['componentInventory'])
assert vectors=={v['vectorId'] for v in m['vectors']}
PY

./gradlew :nereus-storage-object:test \
  --tests 'com.nereusstream.storage.object.nwg1.Nwg1WireGoldenV1Test' \
  --rerun-tasks --console=plain

emit_root=$(mktemp -d /tmp/nereus-nwg1-emitter-XXXXXX)
trap 'rm -rf -- "$emit_root"' EXIT
./gradlew :nereus-storage-object:nwg1GoldenEmitter \
  -Pnwg1GoldenEmitterOutput="$emit_root" --rerun-tasks --console=plain
cmp "$emit_root/first/nwg1-v1-goldens.tsv" "$emit_root/second/nwg1-v1-goldens.tsv"
cmp "$emit_root/first/nwg1-v1-goldens.tsv" "$tsv"
test "$(cat "$emit_root/DOUBLE_EMISSION_IDENTICAL")" = true

python3 - "$manifest" "$emit_root/first/nwg1-v1-positive-inputs.json" <<'PY'
import json,sys
with open(sys.argv[1],encoding='utf-8') as source:
    manifest=json.load(source)
with open(sys.argv[2],encoding='utf-8') as emitted:
    positive_inputs=json.load(emitted)
assert manifest['vectors']==positive_inputs,'manifest semantic inputs do not reproduce exactly'
PY

python3 - <<'PY'
import glob,xml.etree.ElementTree as ET
files=glob.glob('nereus-storage-object/build/test-results/test/TEST-com.nereusstream.storage.object.nwg1.Nwg1WireGoldenV1Test.xml')
assert len(files)==1
r=ET.parse(files[0]).getroot()
assert int(r.attrib['tests'])>0
assert int(r.attrib['failures'])==int(r.attrib['errors'])==int(r.attrib['skipped'])==0
print(f"PASS_LOCAL_NWG1_WIRE_SOURCE_ONLY tests={r.attrib['tests']} failures=0 errors=0 skipped=0 rows=114")
PY
