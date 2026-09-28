---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: FocusedOnly
authority: NormativeImplementationSlice
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json
---

# M2 Kafka K7 checkpoint kernel and closed-history recovery

The NSIP-1 native BK slice restores a compatible common checkpoint and its validated tail, loads stock producer
state from that shared state, and activates only after native Controller RECOVERED acknowledgement. Same-Owner
rollover checkpoints and closes the current run before attaching a successor. Recovery entry/byte debt covers the
unchecked tail; discovery and fencing still cover all legal unretired ledgers within the elapsed bound. Native create
qualification is admitted for the current run and no longer preallocates 256 future Owner epochs. This slice does
not qualify full M4/M5 retirement, unlimited metadata history, or production/performance authority.

K7 implements a profile-neutral `KafkaProtocolCheckpointStoreV1` and its BookKeeper `NBKE2` control-entry carrier.
`KafkaRecoveryCheckpointVectorV1` binds range-index, producer, transaction/aborted, and leader-epoch component coverage
to the exact prior run identity. The common state model is independent of BookKeeper and the Object NWKCP1 carrier.
The K7 BookKeeper writer emits aligned compound vectors so one exact boundary can seed suffix replay; the already
frozen NBKE2 wire still permits distinct component values for a future mutually compatible component selector.

`KafkaProtocolCheckpointCodecV1` defines three strict-EOF canonical sections under the K0-W 2 MiB section cap.
Producer sections carrying native timestamps use `KPC3`, retaining last producer and recent-batch timestamps as
well as coordinator epoch and last marker offset. `KPC2` retains marker state without timestamps; sequence-free
marker-only producers are representable. Existing `KPC1` sections remain readable and are emitted for states needing
neither extension, preserving unchanged Object fixtures. The transaction format remains unchanged. `KLC2` extends
the leader-epoch section with exact run identities and protected packed source locators; states without those
sources retain `KLC1`.
It checks magic/version and row counts before allocation, reconstructs bounded producer duplicate results,
ongoing/completed/aborted transactions, and the Kafka leader-epoch index, and rejects state beyond the component's
covered-through boundary. Same-vector content substitution, regression, another run identity, unaligned K7 writes,
and concurrent publication fail closed.

`BookKeeperKafkaProtocolCheckpointStoreV1` reserves the control entry only between complete DATA groups through the K3
sequencer. A returned exact quorum proof publishes the checkpoint. `OUTCOME_UNKNOWN` or an exceptional accepted append
does not become success or failure by timing: the lifecycle retains the payload and rereads the same ledger/entry,
requiring the exact handle, entry ID, SHA-256, and bytes. Definitive rejection, fencing, absence, or substituted bytes
fails the run. The footer must bind the latest exact protocol-checkpoint entry.

`KafkaBookKeeperTakeoverRecoveryV1` first opens and fences the exact prior handle. It validates an optional authoritative
checkpoint hint or falls back to the authenticated RUN_HEADER, then scans the unchecked suffix in physical order.
Every remote entry read, encoded byte, and elapsed nanosecond contributes to one cumulative
`KafkaBookKeeperRecoveryEnvelopeV1`; checkpoint fallback never resets those counters. The scan validates complete
append-group identity, member order/count, aggregate payload SHA-256, NBKE2 CRC/identity, raw Kafka header/CRC/leader
epoch, and protocol deltas from the narrow exact-source `KafkaRecoveryBatchProtocolAdapterV1`. A partial group or the
first definitive gap/conflict never advances the physical candidate.

NSIP-1 replaces the previous native election/Observed/Applied cap. A recovery request must bind the exact root
and its closed Owner run membership before opening/fencing the ledger. The legal prefix consists of complete,
contiguous commit sets whose physical identity and protocol content validate. An incomplete/conflicting suffix
remains inert; a sealed root/end contradiction fails closed. Complete batches whose response was lost are adopted,
with their original producer duplicate results. Across runs, the previous aligned state may seed the next header
at the same logical start boundary, preserving protocol state.

`KafkaCoherentCommitCoordinatorV1.bootstrapRecovered` installs HW from that legal shared prefix and derives LSO
from recovered transaction state. It no longer accepts an independent native HW input for BK.
`KafkaBookKeeperClosedHistoryRecoveryV1` authenticates the immutable Owner archive chain and exact admitted links,
discovers at most 1,024 Owner closures and 1,024 history runs, and fences every legal ledger before replay begins.
Discovery/fencing time consumes the elapsed bound for every legal run. The composition reads closed native
footers and selects the latest compatible compound checkpoint with complete protected sources through its cut.
A checkpoint persisted as the last native entry is also usable after Owner closure and native fencing, even when
its response was UNKNOWN and no footer was written. The checkpoint includes its own run's locators for that cut.
Normal sealed sources use native index directories; crash-sealed sources use exact locators persisted in KLC2. Producer timestamps use KPC3; unchanged earlier states retain KPC1/KPC2.
Only the uncovered suffix consumes cumulative replay entry/byte debt, with no counter reset on fallback or rollover
without an exact checkpoint. No compatible checkpoint retains bounded full-history fallback. Replay carries
producer/transaction/leader-epoch state across exact adjacent tail runs. All older runs remain isolated and protected.
Missing membership, predecessor identity, sealed end, archive digest or capacity fails the whole recovery.

Recovery does not write a replacement footer to a fenced crash run. `sealRecoveredRun` verifies its actual native
closed ledger and continuous physical prefix independently before persisting a wire-4 run-root recovery cut.
The cut binds the closed Owner digest, recovered LAC and optional inert residue entry; the sealed root binds the
exact Kafka end. A repeated recovery requires the same native cut and end, and `createAfterRecovery` can attach
a new epoch's run through the existing Owner admission CAS. Native Controller/Broker activation remains required.
A persisted recovery cut does not suppress checkpoint/footer discovery on the next takeover. After fencing every
legal run, discovery requires the cut's closed Owner digest and native LAC to match the current exact proofs. It
examines the accepted terminal entry (strictly before any inert suffix), validates the existing footer or checkpoint
and sealed Kafka end, and retains the same protected source validation. DATA probes used to locate terminal metadata
are charged only if their run belongs to the selected unchecked replay tail; covered crash history cannot accumulate
replay debt again. All discovery, fencing and fallback still consume the elapsed bound. No budget is enlarged.

Historical `v2M2KafkaK7Check` executed 26 zero-skip tests in three suites under the earlier source/contract. It covers KPC1 round-trip and parser faults, vector and
component bounds, aligned publication, response-loss reread, concurrency/regression/footer cuts, exact and inert-tail
recovery, checkpoint selection and corrupt fallback, entry/byte/time envelope exhaustion, partial groups, physical
and Applied shortfall, batch-aligned election cuts, open/fence/header faults, and coherent new-leader bootstrap.

This focused deterministic fake-provider gate alone proves no Kafka broker recovery adapter, native election
implementation, final HW/ISR behavior, real BookKeeper behavior, scenario promotion, Kafka Final, or global M2 PASS.
The historical Kafka Final binds it with K8-K10, and K9 selected checkpoint cadence, suffix, timeout, rollover, and resource values
from real-BookKeeper evidence.

The current repeated-takeover fix passes 336 Kafka BookKeeper unit tests with format and Checkstyle checks.
The preceding checkpoint/rollover slice also passed 32 storage BK tests; they were not rerun for this recovery-only
change. The real BK/Oxia run-root suite passes all 13 cases. Its small-budget case uses eight
entries/8 KiB per run, writes 40 DATA batches through more than ten same-Owner runs, fetches old runs, selects common
checkpoint end 39, and charges three recovery entry reads rather than replaying the full DATA history. It verifies
same-offset PID retry, cross-checkpoint ongoing transaction COMMIT, another takeover and ABORT. Three fault cuts
persist actual checkpoint, close or successor attachment before losing the result; allocation stops and cold recovery
preserves ACKed DATA. The checkpoint UNKNOWN and close UNKNOWN cases are separate parameter branches, each
extended through Owner 2 COMMIT/resign and Owner 3 activation. Both fail before the fix at suite timestamp
`2026-09-27T09:32:30.659Z`; after the fix both reuse common checkpoint end 3, charge three tail entry reads under
unchanged eight-entry/8-KiB limits, restore HW/LSO=4/4, retry at original offset 2, fetch identical original batch
bytes and continue writing. Storage-only Controller identities remain synthetic. Historical Final receipts are unchanged.

The active composition now checkpoints between completed append groups at run rollover. DATA admission reserves
half the configured single-run entry/byte budget (and at most 65,536 locators) for DATA; the remainder is for compound
checkpoint, index and footer. The exact encoded controls must also fit the configured ledger/frame bounds. Seal,
read reopen, successor attachment and one same-fence publication preserve HW/LSO and producer/transaction state;
only then is covered recovery debt released. Failure/UNKNOWN fences new allocation and preserves ACKed DATA.
A checkpoint/source section that grows beyond its persisted cap fails closed; full M4/M5 retirement is out of scope.
The normal native Broker/Controller task passes both cases on this follow-up, including the original response-loss,
superseded-recovery and two-relocation case. The added case uses eight entries and the existing minimum 1-MiB ledger
budget, activates actual Owners 256/257 after test-only Controller PartitionRecord events commit to KRaft, and
continues transaction COMMIT/ABORT, read_committed Fetch and group offsets through another cold relocation. It does
not execute 257 failures or replace the production Owner predicate. Its latest suite timestamp is
`2026-09-27T09:37:34.175Z`; the 13-case storage suite is `2026-09-27T09:35:58.034Z`. Historical receipts do not
validate or promote this new wiring.

NSIP-1 T4 Object recovery uses `KafkaObjectAuthorizedRecoveryV1`: a selected immutable CLOSED Binding Head fixes the
complete legal source set. `KafkaObjectAuthorizationV1` stores the existing NWKCP1 common-state encoding in a SHA-named
immutable control record; the same Head selects its exact common coverage and retains all protected Object locators.
Recovery seeds producer/transaction/leader state from that checkpoint, then authenticates only the explicitly granted
NWG1 tail and recomputes each complete native commit set. It installs that state and the retained source index together,
with HW at the closed continuous end and LSO from the rebuilt transaction index. Two successive fresh partition state
instances are covered by real MinIO/Oxia tests; the shared physical session stays alive, so this is not a whole native
Object Broker restart qualification. Source-index/history caps backpressure until separate protected retirement work.

Physical Object rollover still publishes and verifies the common terminal NWKCP1 Object/Head before the successor
binds its exact digest, independently of the physical Seal. `StorageObjectNwkcp1BackendV1` derives its Oxia Head key
from Root shard/run identity (`v2/object-wal/shards/<shard>/runs/<runEpoch>/protocol/kafka/nwkcp1-v1/head`); the Provider
Object prefix is a distinct constructor input bound to that Root's namespace. A physical terminal Head cannot release
Binding authorization debt. The eight-case real MinIO/Oxia suite covers the actual terminal Head, successor creation,
debt retention and cold recovery with protected locators from both Roots.
