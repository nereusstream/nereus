/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.pulsar.BookKeeperPrimaryWalAdministration;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class BookKeeperNamespaceCommand {

    private BookKeeperNamespaceCommand() {}

    public static AdminExitCode ensure(AdminConfiguration config, CommandLineArguments args) {
        Duration timeout = args.timeout();
        Clock clock = Clock.systemUTC();
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(config.oxia(), clock)) {
            BookKeeperPrimaryWalAdministration admin = BookKeeperPrimaryWalAdministration.usingSharedRuntime(
                    config.bookKeeper(), config.oxia(), runtime, clock);

            BookKeeperLedgerIdNamespaceReservation reservation = admin.provisionNamespace(
                            config.operatorEvidenceSha256(), timeout)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return evaluateEnsure(reservation, args);
        } catch (Exception e) {
            System.err.println("namespace ensure failed: " + AdminFailureClassifier.safeMessage(e));
            return AdminFailureClassifier.classify(e, AdminExitCode.PROVIDER_ERROR);
        }
    }

    public static AdminExitCode verify(AdminConfiguration config, CommandLineArguments args) {
        Duration timeout = args.timeout();
        Clock clock = Clock.systemUTC();
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(config.oxia(), clock)) {
            BookKeeperPrimaryWalAdministration admin = BookKeeperPrimaryWalAdministration.usingSharedRuntime(
                    config.bookKeeper(), config.oxia(), runtime, clock);

            BookKeeperLedgerIdNamespaceReservation reservation =
                    admin.verifyNamespace(timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            return evaluateVerify(config, reservation, args);
        } catch (Exception e) {
            System.err.println("namespace verify failed: " + AdminFailureClassifier.safeMessage(e));
            return AdminFailureClassifier.classify(e, AdminExitCode.CONDITION_FAILED);
        }
    }

    static AdminExitCode evaluateEnsure(BookKeeperLedgerIdNamespaceReservation reservation, CommandLineArguments args) {
        String evidence = AdminEvidenceWriter.namespaceReservationEvidence("bookkeeper namespace ensure", reservation);
        writeOutput(args.outputFile(), evidence);
        return reservation.lifecycle() == BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE
                ? AdminExitCode.SUCCESS
                : AdminExitCode.CONDITION_FAILED;
    }

    static AdminExitCode evaluateVerify(
            AdminConfiguration config, BookKeeperLedgerIdNamespaceReservation reservation, CommandLineArguments args) {
        if (reservation.lifecycle() != BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE) {
            System.err.println("namespace is not ACTIVE: " + reservation.lifecycle());
            return AdminExitCode.CONDITION_FAILED;
        }
        if (!reservation.operatorEvidenceSha256().equals(config.operatorEvidenceSha256())) {
            System.err.println("namespace operator evidence does not match configuration");
            return AdminExitCode.CONDITION_FAILED;
        }

        String evidence = AdminEvidenceWriter.namespaceReservationEvidence("bookkeeper namespace verify", reservation);
        writeOutput(args.outputFile(), evidence);
        return AdminExitCode.SUCCESS;
    }

    private static void writeOutput(Optional<Path> outputFile, String evidence) {
        if (outputFile.isPresent()) {
            AdminEvidenceWriter.writeEvidence(outputFile.get(), evidence);
        }
        AdminEvidenceWriter.printEvidence(evidence);
    }
}
