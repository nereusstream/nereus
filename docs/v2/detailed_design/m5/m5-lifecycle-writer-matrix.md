# M5 lifecycle writer integration matrix

Required by ADR 0148. Every row is an implementation obligation, not evidence of completed wiring.
The existing M5TargetDeleteWriterGuardV1 and in-memory coordinator provide generic ordering only.
Activation requires exact entry-point tests and source-bound real-dependency traces for each applicable row.

| Writer / current entry or owner | Target relevance and required path | Response loss / owner / conflict contract | Integration status |
| --- | --- | --- | --- |
| Root/storage fence: metadata-oxia control adapters; M4ReadControlCoordinatorV1.grantTakeover | Ticket any new reference to the old resource; monotonic revocation invalidates observations without waiting for GC | Persist revocation, reject old owner result, refresh READ_FENCED under new epoch | OPEN |
| Manifest/generation: M5MaterializationCoordinatorV1.register/publish; KafkaM5CompactionBridgeV1.createExact/validateForPublication | Ticket old-resource membership changes; unrelated generation advance has a target-neutral token | Exact task/selector successor reread; unknown publication retains ticket and output | OPEN |
| Logical trim/retention: M5LogicalTrimCoordinatorV1.advance and M5RetentionEvidenceAssemblerV1 adapters | Reason-specific coverage; increasing trim unrelated to target cannot block new append; removing/adding target recovery obligation is ticketed | Reconcile exact floor root/version; stale scope or unclassified floor vetoes | OPEN |
| Protection/pins: M4ReadControlCoordinatorV1.introduceFallback/createProtection/releaseProtection | Ticket control admission of a physical reference; ordinary reads use local M4 epoch/pin only; closed fallback and exact RELEASED precede deletion | Cancel/timeout retains pin; exact native drain and terminal proof; old epoch cannot reopen | OPEN |
| Replica/shared member: protocol replica and shared Object membership adapters | Ticket joining/leaving target membership; all member Bindings converge on same resource key | Complete membership enumeration and successor; unknown external mutation retains ticket; new replica cannot use fenced source | OPEN |
| Multipart inventory: M5MultipartCleanupSessionV1 and materialization/offload task ownership | Ticket adopting/creating an upload that a task can publish; enumerate exact key/upload identities | Unknown create/adopt remains owned unresolved work; foreign upload veto; exact abort/relist under intent | OPEN |
| Task/attempt: materialization coordinator and native offload/compaction task adapters | Ticket any task that can select, adopt, retry or recover this output; deterministic multi-resource set | Task owner fencing plus authoritative terminal/non-application proof before ticket removal | OPEN |
| Worker/dispatch ownership: M5TargetDeleteAuthorityCoordinatorV1 and native durable owner authority | READ_FENCED/INTENT ownership moves by same-key CAS plus native old-owner proof; cannot acquire OPEN ticket after fence | Increment observation/dispatch epoch; reject delayed identity/deletion callback; fixed delete attempt | OPEN |
| Provider/KMS/BK capability: provider session admission, OxiaV2CapabilityStore, RealBookKeeperCellSessionV1 | New access/reference authority requires admission; revocation invalidates read/dispatch snapshots; refresh cannot rename resource namespace | New qualification and exact resource reread; no unconditional delete downgrade; old in-flight operations reconciled | OPEN |
| Open handle/writer lease/projection/recovery: M4 control, Kafka recovery and native ManagedLedger interfaces | Local pin path remains local; durable admission/checkpoint/export changes referencing old resource ticketed; disjoint active append needs none | Seal/fence old BK writer and drain handles; recover unknown checkpoint under ticket; no cache-only proof | OPEN |
| Pulsar native offload lifecycle: NereusPulsarLedgerOffloaderV1, native ManagedLedger metadata and M5PulsarObjectCleanupOrderV1 | Native offload attempt/completion/source-selection, BK_DELETE_INTENT/DONE and Nereus authority must agree; root then data then multipart dispatch | Native CAS response-loss reread; current owner and complete replacement before old BK delete; no derived manifest override | OPEN |

Every multi-target row deduplicates canonical PhysicalResourceIdV2 and acquires tickets in unsigned canonical-byte
order before any external mutation. A failed acquisition proves non-dispatch before removing its already acquired
tickets. Unknown or partial external results retain relevant tickets until exact reconciliation. Target-neutral
exemption requires executable evidence of disjoint physical membership; a caller boolean is not proof.

Matrix completion requires code and test links plus source/trace identity, not changing OPEN text to IMPLEMENTED.
Native process activation is M6; M5 still owns the actual adapter/control composition and its real dependency tests.
