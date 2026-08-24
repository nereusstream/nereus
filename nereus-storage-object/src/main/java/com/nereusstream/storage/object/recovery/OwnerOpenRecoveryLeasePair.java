/* Licensed under the Apache License, Version 2.0. */
package com.nereusstream.storage.object.recovery;

import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import java.util.Objects;

/** Atomic raw-to-recovery pair; only common recovery code can mint it. */
public final class OwnerOpenRecoveryLeasePair implements AutoCloseable {
    final C1ObjectProviderSession.RecoveryLease provider;
    final KmsCellSession.RecoveryLease kms;
    private boolean promoted;

    private OwnerOpenRecoveryLeasePair(
            C1ObjectProviderSession.RecoveryLease provider, KmsCellSession.RecoveryLease kms) {
        this.provider = provider;
        this.kms = kms;
    }

    static OwnerOpenRecoveryLeasePair acquire(
            WalRunRootRecord root,
            WalRunLineageRecovery.RecoveredLineage lineage,
            C1ObjectProviderSession rawProvider,
            KmsCellSession rawKms) {
        synchronized (rawProvider) {
            synchronized (rawKms) {
                var authorities = lineage.prepareOwnerOpenTransfers(root);
                rawProvider.requireRecoveryTransferReady(authorities.providerAuthority());
                rawKms.requireRecoveryTransferReady(authorities.kmsAuthority());
                var claimed = authorities.claimForTransfers();
                return new OwnerOpenRecoveryLeasePair(
                        rawProvider.transferToRecovery(claimed.providerTransfer()),
                        rawKms.transferToRecovery(claimed.kmsTransfer()));
            }
        }
    }

    public synchronized Promoted promote(
            WalRunObjectSession.ProviderOwnerAuthority providerAuthority,
            WalRunObjectSession.KmsOwnerAuthority kmsAuthority) {
        synchronized (provider) {
            synchronized (kms) {
                if (promoted) {
                    throw new IllegalStateException("owner-open recovery lease pair was already promoted");
                }
                provider.requireFinalTransferReady(providerAuthority);
                kms.requireFinalTransferReady(kmsAuthority);
                Promoted result =
                        new Promoted(provider.transferToWalRun(providerAuthority), kms.transferToWalRun(kmsAuthority));
                promoted = true;
                return result;
            }
        }
    }

    @Override
    public synchronized void close() {
        if (promoted) {
            return;
        }
        RuntimeException failure = null;
        try {
            provider.close();
        } catch (RuntimeException closeFailure) {
            failure = closeFailure;
        }
        try {
            kms.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    public record Promoted(C1ObjectProviderSession.WalRunLease provider, KmsCellSession.WalRunLease kms) {
        public Promoted {
            Objects.requireNonNull(provider);
            Objects.requireNonNull(kms);
        }
    }
}
