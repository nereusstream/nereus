# Pulsar API Spike and Repository Boundary

This document records the F2-M0 source and compile evidence. It is intentionally tied to one Pulsar
fork commit; interface names remembered from another release are not accepted evidence.

## 1. Locked Pulsar Surface

Target: `nereusstream/pulsar@100d3ef0ff7c7da36d497453b141ddff6f34a9d3`,
`version=5.0.0-M1-SNAPSHOT`.

| File | Locked Git blob |
| --- | --- |
| `ManagedLedgerStorage.java` | `720798123e7b41d19c69a1863ec45389627325af` |
| `ManagedLedgerStorageClass.java` | `8cbe5c3b411e5fe37c5db1ff35321a7d5e9186be` |
| `ManagedLedgerFactory.java` | `b9adbe3b353fd92e541f9fd75b5c788b0468a859` |
| `ManagedLedger.java` | `0455f0efa8bb6d0ef248b870b1a68166cdcef2c8` |
| `ManagedCursor.java` | `1e6ab76c80c939cd51a29a590a50b2d079fa1c80` |
| `ManagedLedgerClientFactory.java` | `3dece00e89a7f0d2f72bff71eabe9d2dff519d37` |

The same blobs were present in the local compile checkout at `320fbce6d540...`. The probe therefore
compiled the exact locked interfaces even though the checkout head was newer.

`javap -public` reports the following total declared API members, including default/static members:

| Interface | Declared members | Consequence |
| --- | ---: | --- |
| `ManagedLedgerStorage` | 6 | Broker-private provider lifecycle must be implemented in the fork |
| `ManagedLedgerStorageClass` | 2 | Name and factory only |
| `ManagedLedgerFactory` | 26 | A facade must cover admin/cache/properties methods, not only `open` |
| `ManagedLedger` | 87 | Many BookKeeper-shaped compatibility methods need explicit behavior |
| `ManagedCursor` | 89 methods plus one constant | F2 cannot claim a full cursor implementation |

Counts are drift detectors, not acceptance criteria. A default method can still be semantically wrong
for Nereus, and a source-compatible signature can still change broker behavior.

## 2. Confirmed Broker Call Path

```text
ServiceConfiguration.managedLedgerStorageClassName
  -> ManagedLedgerStorage.create(...)
  -> Reflections.createInstance(..., TCCL)
  -> ManagedLedgerStorage.initialize(...)

BrokerService.getManagedLedgerConfig(topic)
  -> ManagedLedgerConfig.storageClassName from PersistencePolicies
  -> managedLedgerStorage.getManagedLedgerStorageClass(storageClassName)
  -> storageClass.getManagedLedgerFactory()
  -> factory.asyncOpen(topicName.getPersistenceNamingEncoding(), ...)
```

The broker passes a persistence name such as
`tenant/namespace/persistent/encoded-topic`, not the canonical
`persistent://tenant/namespace/topic` URI. The facade must treat this string as the exact external
identity. It must not split on `/`, URL-decode it, remove a partition suffix or infer a domain.

The stock `ManagedLedgerClientFactory` returns only one `bookkeeper` class and makes it the default.
The fork-owned hybrid provider must compose that stock factory rather than reimplement its
BookKeeper client/configuration setup.

## 3. Interface Findings

### 3.1 ManagedLedgerStorage

The exact initialize signature is:

```java
void initialize(
    ServiceConfiguration conf,
    MetadataStoreExtended metadataStore,
    BookKeeperClientFactory bookkeeperProvider,
    EventLoopGroup eventLoopGroup,
    OpenTelemetry openTelemetry) throws Exception;
```

The hybrid provider returns `[bookkeeper, nereus]` in that order, keeps `bookkeeper` as default,
and maps a null class name to the default exactly as the stock implementation does. A configured
`nereus` class that fails initialization is a broker startup failure; it must not silently fall back
to BookKeeper for a topic whose policy names `nereus`.

### 3.2 ManagedLedgerFactory

Required groups are:

- synchronous and asynchronous open;
- read-only cursor and read-only managed-ledger open;
- info, exists, delete and shutdown;
- properties and the open-ledger map;
- cache manager/config/stats compatibility;
- unloaded-topic backlog estimation.

The ownership checker passed to `asyncOpen` does not grant a Nereus append lease. Broker topic
ownership and the L0 append-session epoch are separate checks. The facade may call the ownership
checker only when classifying an open-time ownership conflict; it cannot use a successful result as a
durable fencing token.

### 3.3 ManagedLedger

The broker hot append overload is:

```java
void asyncAddEntry(
    ByteBuf buffer,
    int numberOfMessages,
    AddEntryCallback callback,
    Object ctx);
```

`PersistentTopic.addComplete` consumes `entryData` to extract message metadata before it completes
the producer publish context. A Nereus callback therefore must supply the exact persisted entry bytes,
not an empty placeholder or only the payload after parsing.

The direct read boundary is:

```java
void asyncReadEntry(Position position, ReadEntryCallback callback, Object ctx);
```

The interface also requires synthetic ledgers info, first/previous/next positions, stats/properties,
termination, truncation, migration and offload-shaped methods. Each is classified in
`04-facade-state-machines-and-compatibility.md`; unspecified no-op behavior is forbidden.

### 3.4 ManagedCursor

Dispatcher-facing bounded reads include both:

```java
void asyncReadEntries(
    int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
    Object ctx, Position maxPosition);

void asyncReadEntriesOrWait(
    int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
    Object ctx, Position maxPosition);
```

The current interface also contains durable mark-delete, individual delete/replay, reset, ack-set,
cursor properties and backlog methods. F2 implements read-only/non-durable behavior and the basic
open boundary only. It must not return successful callbacks for durable mutations that Future 3 has
not implemented.

## 4. Compile Probe

`spikes/PulsarManagedLedgerApiProbe.java` uses exact overloads with real Pulsar types. The companion
init script creates an isolated `f2ApiProbe` source set in `:pulsar-broker`. It neither copies a
source into Pulsar nor changes the checkout.

Run:

```bash
export NEREUS_F2_PROBE_DIR="$NEREUS_REPO/docs/phase-2-managed-ledger-facade/spikes"
"$PULSAR_REPO/gradlew" \
  -p "$PULSAR_REPO" \
  -I "$NEREUS_F2_PROBE_DIR/pulsar-api-probe.init.gradle" \
  :pulsar-broker:compileF2ApiProbeJava
```

F2-M0 result: `BUILD SUCCESSFUL`.

For a target upgrade:

1. update the commit and version lock;
2. record all six target blob IDs again;
3. run the compile probe against a checkout containing those blobs;
4. inspect broker call sites, not only interfaces;
5. update compatibility classifications before changing production code.

## 5. Code Ownership

### Nereus repository

- Formula-based Position projection and validation.
- Pulsar Entry byte codec and `Entry` implementation.
- `ManagedLedgerFactory`, `ManagedLedger`, read-only and non-durable cursor facades.
- Projection metadata records/codecs/fake/real Oxia adapter.
- L0 stream lifecycle extensions.
- Product configuration values that do not import broker-private classes.

It is acceptable for `nereus-managed-ledger` to depend on the locked managed-ledger artifact. It must
not pull `pulsar-broker` into `nereus-api`, `nereus-core`, `nereus-metadata-oxia` or
`nereus-object-store`.

### Pulsar fork

- The `ManagedLedgerStorage` implementation and hybrid composition.
- Broker configuration fields/default config documentation.
- Server distribution/runtime dependency packaging.
- Policy-selection, load/unload, restart, failover and stats integration tests.
- Any adaptation required because a broker-private interface changes.

The fork must depend on released/pinned Nereus artifacts or an explicit composite-build development
mode. It must not duplicate Nereus projection or metadata algorithms.

### No-code assumptions

No change is currently required to `ManagedLedgerStorage`,
`ManagedLedgerStorageClass`, `ManagedLedgerFactory`, `ManagedLedger` or `ManagedCursor`
interfaces. If implementation later requires a new method on these Pulsar extension points, stop and
perform the compatibility/PIP review before editing the interface.
