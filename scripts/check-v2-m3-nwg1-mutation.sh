#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
manifest=docs/v2/wire/nwg1-v1-golden-manifest.json

test -s "$manifest" || { echo "missing NWG1 mutation manifest" >&2; exit 1; }
for required in Nwg1MutationOperationV1.java Nwg1ResignOperationV1.java; do
  test -s "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/$required" || exit 1
done

python3 - "$manifest" <<'PY'
import collections,hashlib,json,re,struct,sys
raw=open(sys.argv[1],'rb').read()
assert raw and not raw.endswith(b'\n')
assert len(raw)<=1_048_576
m=json.loads(raw)
assert raw==json.dumps(m,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
assert set(m)=={'artifact','componentInventory','expectedCounts','externalCallProfiles','externalFixtures','mutationOperations','mutations','resignOperations','vectors','zstdFixtures'}
assert m['artifact']=='NWG1_GOLDEN_MANIFEST_V1'
assert m['expectedCounts']=={'componentKinds':16,'deepMutationRoots':50,'externalFixtures':2,'goldenRows':114,'mutationOperations':10,'mutationPathExecutions':240,'mutationRecords':84,'positiveVectors':6,'rejectionCodes':25,'resignOperations':8,'validationStages':16}
ops=['SET_U16','SET_U32','SET_U64','XOR_BYTE','REPLACE_COMPONENT','TRUNCATE_COMPONENT','APPEND_BYTES','SWAP_ROWS','DUPLICATE_ROW','REMOVE_ROW']
resign=['RECOMPUTE_HEADER_CRC','RECOMPUTE_DIRECTORY_CRC','REENCRYPT_DIRECTORY','REENCRYPT_FRAME','RECOMPUTE_BODY_SHA_AND_LEAF','RECOMPUTE_PROTOCOL_CELL_COMMITMENT','RECOMPUTE_OWNER_FENCE_COMMITMENT','RECOMPUTE_ENVELOPE_COMMITMENT']
codes=['NON_CANONICAL_ENCODING','TRUNCATED_INPUT','TRAILING_BYTES','UNSUPPORTED_VERSION','UNKNOWN_CODE','REQUIRED_ZERO_NONZERO','VALUE_DOMAIN_VIOLATION','DECLARED_LENGTH_MISMATCH','COUNT_MISMATCH','LIMIT_EXCEEDED','ARITHMETIC_OVERFLOW','CHECKSUM_MISMATCH','DIGEST_MISMATCH','AUTHORITY_MISMATCH','KEY_UNWRAP_FAILED','AEAD_AUTHENTICATION_FAILED','CANONICAL_ORDER_VIOLATION','DUPLICATE_IDENTITY','REFERENCE_OUT_OF_RANGE','RANGE_GAP','RANGE_OVERLAP','COVERAGE_MISMATCH','CODEC_CONTRACT_VIOLATION','NATIVE_FRAMING_INVALID','NATIVE_CHECKSUM_MISMATCH']
stages=['ROOT_AUTHORITY','LEAF','OBJECT_BODY_DIGEST','HEADER_GRAMMAR','HEADER_CRC','HEADER_AUTHORITY','KMS_ENVELOPE','DIRECTORY_AEAD','DIRECTORY_CRC','DIRECTORY_STRUCTURE','BINDING_SEMANTICS','FRAME_AEAD','FRAME_CODEC','FRAME_PAYLOAD_CRC','NATIVE_FRAME','APPEND_UNIT_SEMANTICS']
scopes=['WALRUN','SHARED_OBJECT','BINDING','APPEND_UNIT']
calls=['ROOT_AUTHORITY_READ','METADATA_READ','METADATA_CONDITIONAL_MUTATION','KMS_WRAP','KMS_UNWRAP','OBJECT_CONDITIONAL_PUT','OBJECT_HEAD','OBJECT_FULL_GET','OBJECT_PREFIX_RANGE_GET','OBJECT_FRAME_RANGE_GET','OBJECT_LIST_PAGE']
paths=['ROUTINE_RANGE_READ','FULL_BODY_RECONCILIATION','OPEN_RUN_RECOVERY']
record_fields={'actualExternalCallsByPath','applicablePaths','baseVectorId','expectedIsolationScope','expectedMaximumExternalCallsByKind','expectedPublication','expectedRejectionCode','expectedStage','mutationClass','mutationId','mutationOperations','mutationRecipeSha256','neutralizedEarlierChecks','resignOperations','verificationEntryCut'}
profiles={profile['token']:profile for profile in m['externalCallProfiles']}
assert set(profiles)=={'NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT','AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT'}
assert profiles['NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT']['count']==30
assert profiles['AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT']['count']==54
for token,profile in profiles.items():
    assert set(profile)=={'count','maximumCalls','token'}
    assert set(profile['maximumCalls'])==set(calls)
    assert all(value==0 for kind,value in profile['maximumCalls'].items() if kind!='KMS_UNWRAP')
    assert profile['maximumCalls']['KMS_UNWRAP']==(1 if token.startswith('AT_MOST_ONE') else 0)
assert m['mutationOperations']==ops and m['resignOperations']==resign
records=m['mutations']; assert len(records)==84
assert len({r['mutationId'] for r in records})==84
assert sum(len(r['applicablePaths']) for r in records)==240
assert collections.Counter(len(r['applicablePaths']) for r in records)=={3:73,2:10,1:1}
assert set(r['expectedRejectionCode'] for r in records)==set(codes)
assert set(r['expectedStage'] for r in records)==set(stages)
assert set(r['expectedIsolationScope'] for r in records)==set(scopes)
assert collections.Counter(r['mutationClass'] for r in records)=={
 'NO_EXTERNAL_CALLS_AFTER_PRELOADED_CUT':30,
 'AT_MOST_ONE_KMS_UNWRAP_CALL_AFTER_PRELOADED_CUT':54}
assert collections.Counter(r['baseVectorId'] for r in records)=={
 'NWG1_KAFKA_MIN_ZERO_RECORD_NONE_V1':60,'NWG1_KAFKA_MULTI_BINDING_COMMIT_SET_NONE_V1':16,
 'NWG1_KAFKA_FIXED_ZSTD_V1':4,'NWG1_PULSAR_MIN_ZERO_BYTE_NONE_V1':2,
 'NWG1_PULSAR_MULTI_BINDING_ADJACENT_NONE_V1':1,'NWG1_PULSAR_FIXED_ZSTD_V1':1}
fixture={x['fixtureId']:x for x in m['externalFixtures']}
vectors={x['vectorId']:x for x in m['vectors']}
roots=[]; keys=[]
for r in records:
    assert set(r) in (record_fields,record_fields|{'mutationRootSha256'})
    assert re.fullmatch(r'[A-Z0-9][A-Z0-9_.-]{0,127}',r['mutationId'])
    assert r['applicablePaths']==[path for path in paths if path in r['applicablePaths']]
    assert r['verificationEntryCut']=='PRELOADED_VERIFIED_ROOT_AND_ACQUIRED_BYTES_V1'
    assert r['expectedPublication']=='NONE'
    assert r['mutationOperations']
    for operation in r['mutationOperations']:
        base_fields={'componentKind','offset','operandHex','operation','rowOrdinal'}
        if operation['componentKind']=='WRAPPED_ENVELOPE_COMMITMENT':
            assert set(operation)==base_fields|{'componentProfile'}
            assert operation['componentProfile']=='RKE_ENVELOPE_PREIMAGE_V1'
        else:
            assert set(operation)==base_fields
        assert operation['operation'] in ops
    assert len(r['resignOperations'])==len(set(r['resignOperations']))
    assert r['resignOperations']==[x for x in resign if x in r['resignOperations']]
    assert set(r['expectedMaximumExternalCallsByKind'])==set(calls)
    assert set(r['actualExternalCallsByPath'])==set(r['applicablePaths'])
    for path,actual in r['actualExternalCallsByPath'].items():
        assert set(actual)==set(calls)
        assert all(type(value) is int and value >= 0 for value in actual.values())
        assert all(actual[kind] <= r['expectedMaximumExternalCallsByKind'][kind] for kind in calls)
    recipe={k:r[k] for k in ['applicablePaths','baseVectorId','mutationId','mutationOperations','neutralizedEarlierChecks','resignOperations','verificationEntryCut']}
    recipe_sha=hashlib.sha256(json.dumps(recipe,sort_keys=True,separators=(',',':')).encode()).digest()
    assert recipe_sha.hex()==r['mutationRecipeSha256']
    if 'mutationRootSha256' in r:
        assert {'RECOMPUTE_HEADER_CRC','REENCRYPT_DIRECTORY','REENCRYPT_FRAME','RECOMPUTE_BODY_SHA_AND_LEAF'} <= set(r['resignOperations'])
        f=fixture['EXT_KAFKA_WALRUN_AUTHORITY_V1' if 'KAFKA' in r['baseVectorId'] else 'EXT_PULSAR_WALRUN_AUTHORITY_V1']
        base=bytes.fromhex(f['walRunRootSha256Hex']); mid=r['mutationId'].encode()
        root=hashlib.sha256(b'NWG1/MUTATION/ROOT/V1\0'+base+struct.pack('>I',len(mid))+mid+recipe_sha).digest()
        assert root.hex()==r['mutationRootSha256'] and root!=bytes(32)
        roots.append(root)
        vector=vectors[r['baseVectorId']]
        authority=fixture[vector['externalFixtureId']]
        wal_run_key=bytes.fromhex(authority['testOnlyPlaintextWalRunKeyHex'])
        info=b'NWG1/OBJ/KEY/V1\0'+struct.pack(
            '>IQBQ',vector['shardId'],vector['shardRunEpoch'],vector['laneId'],vector['laneSequence'])
        prk=__import__('hmac').new(root,wal_run_key,hashlib.sha256).digest()
        keys.append(__import__('hmac').new(prk,info+b'\x01',hashlib.sha256).digest())
assert len(roots)==len(set(roots))==50
assert len(keys)==len(set(keys))==50 and all(key!=bytes(32) for key in keys)
PY

runner=nereus-storage-object/src/test/java/com/nereusstream/storage/object/nwg1/Nwg1MutationRunnerV1.java
test_source=nereus-storage-object/src/test/java/com/nereusstream/storage/object/nwg1/Nwg1ManifestAndMutationV1Test.java
rg -q 'Nwg1ObjectVerifierV1\.verify' "$runner"
rg -q 'execution\.failure\(\)\.rejection\(\)' "$test_source"
rg -q 'actualExternalCallsByPath' "$test_source"
if rg -n 'GENERATE|expectedRejectionCode.*throw|expectedStage.*throw' "$runner" "$test_source"; then
  echo "forbidden generated or expected-driven NWG1 mutation runner" >&2
  exit 1
fi

./gradlew :nereus-storage-object:test \
  --tests 'com.nereusstream.storage.object.nwg1.Nwg1ManifestAndMutationV1Test' \
  --rerun-tasks --console=plain

python3 - <<'PY'
import glob,xml.etree.ElementTree as ET
files=glob.glob('nereus-storage-object/build/test-results/test/TEST-com.nereusstream.storage.object.nwg1.Nwg1ManifestAndMutationV1Test.xml')
assert len(files)==1
r=ET.parse(files[0]).getroot()
assert int(r.attrib['tests'])>0
assert int(r.attrib['failures'])==int(r.attrib['errors'])==int(r.attrib['skipped'])==0
print(f"PASS_LOCAL_NWG1_MUTATION_ONLY tests={r.attrib['tests']} failures=0 errors=0 skipped=0 records=84 paths=240 roots=50")
PY
