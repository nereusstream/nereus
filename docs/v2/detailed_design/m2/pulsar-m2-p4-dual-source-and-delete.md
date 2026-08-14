---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P4 dual-source reads and BookKeeper deletion

`PulsarDualSourceReadHandleV1` is the ManagedLedger-owned composite for one ledger. Before native offload completion it
admits BookKeeper only. With completion and `BK_DELETE_NONE`, Object and BookKeeper are eligible under the configured
first-source preference. `BK_DELETE_INTENT` and `BK_DELETE_DONE` authorize Object only even if physical BookKeeper bytes
still exist.

Every child range owns one source-specific pin bound to the exact native metadata version and attempt UUID observed at
admission. A successful range retains only that source pin until release. An Object missing/timeout/unavailable/short/
integrity/format failure releases any partial child result and the primary pin, rereads native eligibility, and retries
the complete inclusive range from BookKeeper once. BookKeeper falls back only for native no-such-ledger. Invalid range,
cancellation, close, unsupported operation, and ordinary BookKeeper transient errors never fall back. A double failure
returns the primary with the secondary suppressed. Object integrity/format is always reported to the quarantine observer
even when BookKeeper succeeds.

The BookKeeper deletion coordinator fences new BK pins, waits at most the configured bounded drain interval, performs a
fresh production Object pair/full-reader revalidation, and calls a native CAS that must resolve response uncertainty
before returning `BK_DELETE_INTENT` plus the compatibility boolean. Only then does it close the cached BK child, prove
physical deletion/authoritative absence, and publish `BK_DELETE_DONE`. Pre-INTENT failure reopens BK pin admission when
the same native version remains current; timeout keeps it fenced until the last late pin drains. Failure after INTENT
never rolls back, and restart reconciles INTENT directly to physical deletion and DONE.

Composite close fences both sources, drains accepted ranges, and closes each initialized child exactly once. Child
release and pin release are both attempted even if either fails, so fallback cannot inherit a leaked primary pin.

`v2M2PulsarP4Check` proves the Object child, whole-range source/error table, partial release, no-loop fallback, pin
fence/drain, bounded timeout, final revalidation interface, irreversible CAS ordering, restart reconciliation, and close
races. Native Pulsar fork integration, selected defaults/evidence, scenario promotion, and M2 PASS remain pending.
