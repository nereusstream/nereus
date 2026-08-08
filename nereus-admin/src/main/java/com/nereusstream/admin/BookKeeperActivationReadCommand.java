/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.pulsar.BookKeeperPrimaryWalAdministration;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class BookKeeperActivationReadCommand {

    private BookKeeperActivationReadCommand() {}

    public static AdminExitCode execute(AdminConfiguration config, CommandLineArguments args) {
        Duration timeout = args.timeout();
        Clock clock = Clock.systemUTC();
        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(config.oxia(), clock)) {
            BookKeeperPrimaryWalAdministration admin = BookKeeperPrimaryWalAdministration.usingSharedRuntime(
                    config.bookKeeper(), config.oxia(), runtime, clock);

            Optional<BookKeeperProtocolActivation> activation =
                    admin.readActivation(timeout).get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (activation.isEmpty()) {
                String evidence = activationAbsentEvidence();
                writeOutput(args.outputFile(), evidence);
                return AdminExitCode.CONDITION_FAILED;
            }

            String evidence =
                    AdminEvidenceWriter.activationReadEvidence("bookkeeper activation read", activation.get());
            writeOutput(args.outputFile(), evidence);
            return AdminExitCode.SUCCESS;
        } catch (Exception e) {
            System.err.println("activation read failed: " + AdminFailureClassifier.safeMessage(e));
            return AdminFailureClassifier.classify(e, AdminExitCode.PROVIDER_ERROR);
        }
    }

    private static String activationAbsentEvidence() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"schemaVersion\": 1,\n");
        sb.append("  \"command\": \"bookkeeper activation read\",\n");
        sb.append("  \"presence\": false\n");
        sb.append("}");
        return sb.toString();
    }

    private static void writeOutput(Optional<Path> outputFile, String evidence) {
        if (outputFile.isPresent()) {
            AdminEvidenceWriter.writeEvidence(outputFile.get(), evidence);
        }
        AdminEvidenceWriter.printEvidence(evidence);
    }
}
