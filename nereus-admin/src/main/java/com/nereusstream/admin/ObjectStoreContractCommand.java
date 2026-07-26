/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class ObjectStoreContractCommand {

    private static final int LIST_OBJECT_COUNT = 12;
    private static final int LIST_PAGE_SIZE = 3;
    private static final int MAX_LIST_PAGES = 100;
    private static final String PERSISTENCE_PREFIX = "__nereus_restart_gate/v1/";

    private ObjectStoreContractCommand() {
    }

    /**
     * Provider creation executes the production HeadBucket gate. A normal empty bucket is therefore
     * a successful verification and no synthetic object is required.
     */
    public static AdminExitCode verify(AdminConfiguration config, CommandLineArguments args) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schemaVersion", 1);
        evidence.put("command", "object-store verify");
        try (ProviderSession ignored = createSession(config)) {
            evidence.put("headBucketSuccess", true);
            evidence.put("overallSuccess", true);
            printEvidence(createJson(evidence), args.outputFile());
            return AdminExitCode.SUCCESS;
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            evidence.put("headBucketSuccess", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
            printEvidence(createJson(evidence), args.outputFile());
            System.err.println("object-store verify failed: " + safeMessage(cause));
            return exitCode(cause, AdminExitCode.PROVIDER_ERROR);
        }
    }

    public static AdminExitCode contract(AdminConfiguration config, CommandLineArguments args) {
        String runId = UUID.randomUUID().toString();
        try (ProviderSession session = createSession(config)) {
            return contract(session.store(), runId, args.timeout(), args.outputFile());
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("schemaVersion", 1);
            evidence.put("command", "object-store contract");
            evidence.put("runId", runId);
            evidence.put("headBucketSuccess", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
            printEvidence(createJson(evidence), args.outputFile());
            System.err.println("object-store contract provider creation failed: "
                    + safeMessage(cause));
            return exitCode(cause, AdminExitCode.PROVIDER_ERROR);
        }
    }

    public static AdminExitCode persistenceCreate(
            AdminConfiguration config, CommandLineArguments args) {
        return runPersistenceCommand(config, args, PersistenceAction.CREATE);
    }

    public static AdminExitCode persistenceVerify(
            AdminConfiguration config, CommandLineArguments args) {
        return runPersistenceCommand(config, args, PersistenceAction.VERIFY);
    }

    public static AdminExitCode persistenceCleanup(
            AdminConfiguration config, CommandLineArguments args) {
        return runPersistenceCommand(config, args, PersistenceAction.CLEANUP);
    }

    private static AdminExitCode runPersistenceCommand(
            AdminConfiguration config,
            CommandLineArguments args,
            PersistenceAction action) {
        String runId = args.runId().orElseThrow();
        try (ProviderSession session = createSession(config)) {
            return switch (action) {
                case CREATE -> persistenceCreate(
                        session.store(), runId, args.timeout(), args.outputFile());
                case VERIFY -> persistenceVerify(
                        session.store(), runId, args.timeout(), args.outputFile());
                case CLEANUP -> persistenceCleanup(
                        session.store(), runId, args.timeout(), args.outputFile());
            };
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            Map<String, Object> evidence = persistenceEvidence(action, runId);
            evidence.put("headBucketSuccess", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
            printEvidence(createJson(evidence), args.outputFile());
            System.err.println("object-store persistence " + action.commandName()
                    + " provider creation failed: " + safeMessage(cause));
            return exitCode(cause, AdminExitCode.PROVIDER_ERROR);
        }
    }

    static AdminExitCode persistenceCreate(
            ObjectStore store,
            String runId,
            Duration timeout,
            Optional<java.nio.file.Path> outputFile) {
        Map<String, Object> evidence = persistenceEvidence(PersistenceAction.CREATE, runId);
        evidence.put("headBucketSuccess", true);
        Duration opTimeout = operationTimeout(timeout);
        byte[] payload = persistencePayload(runId);
        Checksum checksum = computeCrc32c(payload);
        ObjectKey key = persistenceKey(runId);
        AdminExitCode failureExitCode = AdminExitCode.CONDITION_FAILED;
        try {
            PutObjectResult put = await(
                    store.putObject(
                            key,
                            ByteBuffer.wrap(payload),
                            new PutObjectOptions(
                                    "application/octet-stream",
                                    checksum,
                                    true,
                                    persistenceMetadata(runId),
                                    opTimeout)),
                    opTimeout);
            HeadObjectResult head = await(
                    store.headObject(key, new HeadObjectOptions(opTimeout)),
                    opTimeout);
            boolean identityExact = head.objectLength() == payload.length
                    && head.checksum().equals(checksum)
                    && head.etag().filter(put.etag()::equals).isPresent()
                    && runId.equals(head.metadata().get("restart-gate-run-id"));
            evidence.put("conditionalCreateSucceeded", true);
            evidence.put("createdIdentityExact", identityExact);
            evidence.put("objectKey", key.value());
            evidence.put("etag", put.etag());
            evidence.put("overallSuccess", identityExact);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            failureExitCode = exitCode(cause, AdminExitCode.CONDITION_FAILED);
            evidence.put("conditionalCreateSucceeded", false);
            evidence.put("createdIdentityExact", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
        }
        printEvidence(createJson(evidence), outputFile);
        return Boolean.TRUE.equals(evidence.get("overallSuccess"))
                ? AdminExitCode.SUCCESS
                : failureExitCode;
    }

    static AdminExitCode persistenceVerify(
            ObjectStore store,
            String runId,
            Duration timeout,
            Optional<java.nio.file.Path> outputFile) {
        Map<String, Object> evidence = persistenceEvidence(PersistenceAction.VERIFY, runId);
        evidence.put("headBucketSuccess", true);
        Duration opTimeout = operationTimeout(timeout);
        byte[] expected = persistencePayload(runId);
        Checksum checksum = computeCrc32c(expected);
        ObjectKey key = persistenceKey(runId);
        AdminExitCode failureExitCode = AdminExitCode.CONDITION_FAILED;
        try {
            HeadObjectResult head = await(
                    store.headObject(key, new HeadObjectOptions(opTimeout)),
                    opTimeout);
            RangeReadResult read = await(
                    store.readRange(
                            key,
                            0,
                            expected.length,
                            new RangeReadOptions(Optional.of(checksum), opTimeout)),
                    opTimeout);
            boolean headExact = head.objectLength() == expected.length
                    && head.checksum().equals(checksum)
                    && head.etag().isPresent()
                    && runId.equals(head.metadata().get("restart-gate-run-id"));
            boolean payloadExact = read.offset() == 0
                    && read.length() == expected.length
                    && read.checksum().filter(checksum::equals).isPresent()
                    && Arrays.equals(bytes(read.payload()), expected);
            evidence.put("headIdentityExact", headExact);
            evidence.put("payloadExact", payloadExact);
            evidence.put("restartPersistenceVerified", headExact && payloadExact);
            evidence.put("objectKey", key.value());
            evidence.put("overallSuccess", headExact && payloadExact);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            failureExitCode = exitCode(cause, AdminExitCode.CONDITION_FAILED);
            evidence.put("headIdentityExact", false);
            evidence.put("payloadExact", false);
            evidence.put("restartPersistenceVerified", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
        }
        printEvidence(createJson(evidence), outputFile);
        return Boolean.TRUE.equals(evidence.get("overallSuccess"))
                ? AdminExitCode.SUCCESS
                : failureExitCode;
    }

    static AdminExitCode persistenceCleanup(
            ObjectStore store,
            String runId,
            Duration timeout,
            Optional<java.nio.file.Path> outputFile) {
        Map<String, Object> evidence = persistenceEvidence(PersistenceAction.CLEANUP, runId);
        evidence.put("headBucketSuccess", true);
        Duration opTimeout = operationTimeout(timeout);
        ObjectKey key = persistenceKey(runId);
        AdminExitCode failureExitCode = AdminExitCode.CONDITION_FAILED;
        try {
            HeadObjectResult head = await(
                    store.headObject(key, new HeadObjectOptions(opTimeout)),
                    opTimeout);
            await(
                    store.deleteObject(
                            key,
                            new DeleteObjectOptions(
                                    head.objectLength(),
                                    head.checksum(),
                                    head.etag(),
                                    opTimeout)),
                    opTimeout);
            boolean absent;
            try {
                await(store.headObject(key, new HeadObjectOptions(opTimeout)), opTimeout);
                absent = false;
            } catch (Throwable expected) {
                Throwable cause = unwrap(expected);
                absent = isNotFound(cause);
                if (!absent) {
                    failureExitCode =
                            exitCode(cause, AdminExitCode.CONDITION_FAILED);
                }
            }
            evidence.put("conditionalDeleteSucceeded", absent);
            evidence.put("postDeleteHeadNotFound", absent);
            evidence.put("overallSuccess", absent);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            failureExitCode = exitCode(cause, AdminExitCode.CONDITION_FAILED);
            evidence.put("conditionalDeleteSucceeded", false);
            evidence.put("postDeleteHeadNotFound", false);
            evidence.put("overallSuccess", false);
            evidence.put("error", safeMessage(cause));
        }
        printEvidence(createJson(evidence), outputFile);
        return Boolean.TRUE.equals(evidence.get("overallSuccess"))
                ? AdminExitCode.SUCCESS
                : failureExitCode;
    }

    static AdminExitCode contract(
            ObjectStore store,
            String runId,
            Duration timeout,
            Optional<java.nio.file.Path> outputFile) {
        String contractPrefix = "__nereus_contract/v1/" + runId + "/";
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("schemaVersion", 1);
        results.put("command", "object-store contract");
        results.put("runId", runId);
        results.put("headBucketSuccess", true);
        Duration opTimeout = operationTimeout(timeout);

        byte[] testData = ("nereus-object-store-contract-" + runId)
                .getBytes(StandardCharsets.UTF_8);
        Checksum testChecksum = computeCrc32c(testData);
        ObjectKey keyA = new ObjectKey(contractPrefix + "object-a");
        ObjectKey keyB = new ObjectKey(contractPrefix + "object-b");

        try {
            runConditionalCreate(
                    store, keyA, testData, testChecksum, runId, opTimeout, results);
            stopAfterTimeout(results);
            runHeadObject(
                    store, keyA, testData, testChecksum, runId, opTimeout, results);
            stopAfterTimeout(results);
            runRangeReads(store, keyA, testData, opTimeout, results);
            stopAfterTimeout(results);
            runDuplicatePut(store, keyA, testData, testChecksum, opTimeout, results);
            stopAfterTimeout(results);
            runConditionalCreateRace(
                    store, keyB, testData, testChecksum, opTimeout, results);
            stopAfterTimeout(results);
            Set<String> expectedKeys = runListObjectCreates(
                    store, contractPrefix, keyA, keyB, opTimeout, results);
            stopAfterTimeout(results);
            runPaginatedList(
                    store, contractPrefix, expectedKeys, opTimeout, results);
            stopAfterTimeout(results);
            runDeleteWithWrongEtag(store, keyA, opTimeout, results);
            stopAfterTimeout(results);
            runDeleteWithCorrectEtag(store, keyA, opTimeout, results);
            stopAfterTimeout(results);
        } catch (Throwable failure) {
            recordFailure(results, "contractExecutionError", failure);
        } finally {
            if (Boolean.TRUE.equals(results.get("timeoutObserved"))) {
                results.put("contractPrefixCleanedUp", false);
                results.put("cleanupSkippedAfterTimeout", true);
                results.put("cleanupFailureCount", 0);
            } else {
                cleanupPrefix(store, contractPrefix, opTimeout, results);
            }
        }

        boolean overallSuccess = !Boolean.TRUE.equals(results.get("timeoutObserved"))
                && evaluateResults(results);
        results.put("overallSuccess", overallSuccess);
        printEvidence(createJson(results), outputFile);
        if (overallSuccess) {
            return AdminExitCode.SUCCESS;
        }
        return Boolean.TRUE.equals(results.get("timeoutObserved"))
                ? AdminExitCode.TIMEOUT
                : AdminExitCode.CONDITION_FAILED;
    }

    private static void runConditionalCreate(
            ObjectStore store,
            ObjectKey key,
            byte[] data,
            Checksum checksum,
            String runId,
            Duration timeout,
            Map<String, Object> results) {
        try {
            PutObjectResult result = await(
                    store.putObject(
                            key,
                            ByteBuffer.wrap(data),
                            new PutObjectOptions(
                                    "application/octet-stream",
                                    checksum,
                                    true,
                                    Map.of("contract-marker", runId),
                                    timeout)),
                    timeout);
            results.put("conditionalCreateSingleWinner", true);
            results.put("conditionalCreateEtag", result.etag());
        } catch (Throwable failure) {
            results.put("conditionalCreateSingleWinner", false);
            recordFailure(results, "conditionalCreateError", failure);
        }
    }

    private static void runHeadObject(
            ObjectStore store,
            ObjectKey key,
            byte[] expectedData,
            Checksum expectedChecksum,
            String runId,
            Duration timeout,
            Map<String, Object> results) {
        try {
            HeadObjectResult result = await(
                    store.headObject(key, new HeadObjectOptions(timeout)), timeout);
            results.put(
                    "metadataRoundTrip",
                    runId.equals(result.metadata().get("contract-marker")));
            results.put("etagPresent", result.etag().isPresent());
            results.put(
                    "headIdentityExact",
                    result.objectLength() == expectedData.length
                            && result.checksum().equals(expectedChecksum));
        } catch (Throwable failure) {
            results.put("metadataRoundTrip", false);
            results.put("etagPresent", false);
            results.put("headIdentityExact", false);
            recordFailure(results, "headObjectError", failure);
        }
    }

    private static void runRangeReads(
            ObjectStore store,
            ObjectKey key,
            byte[] data,
            Duration timeout,
            Map<String, Object> results) {
        try {
            Checksum fullChecksum = computeCrc32c(data);
            RangeReadResult full = await(
                    store.readRange(
                            key,
                            0,
                            data.length,
                            new RangeReadOptions(Optional.of(fullChecksum), timeout)),
                    timeout);
            boolean fullExact = Arrays.equals(bytes(full.payload()), data)
                    && full.offset() == 0
                    && full.length() == data.length
                    && full.checksum().filter(fullChecksum::equals).isPresent();

            int middleOffset = Math.min(3, data.length - 1);
            int middleLength = Math.min(7, data.length - middleOffset);
            byte[] expectedMiddle = Arrays.copyOfRange(
                    data, middleOffset, middleOffset + middleLength);
            Checksum middleChecksum = computeCrc32c(expectedMiddle);
            RangeReadResult middle = await(
                    store.readRange(
                            key,
                            middleOffset,
                            middleLength,
                            new RangeReadOptions(Optional.of(middleChecksum), timeout)),
                    timeout);
            boolean middleExact = Arrays.equals(bytes(middle.payload()), expectedMiddle)
                    && middle.offset() == middleOffset
                    && middle.length() == middleLength
                    && middle.checksum().filter(middleChecksum::equals).isPresent();

            results.put("rangeStatusSemantics", fullExact && middleExact);
            results.put("rangeChecksumValid", fullExact && middleExact);
        } catch (Throwable failure) {
            results.put("rangeStatusSemantics", false);
            results.put("rangeChecksumValid", false);
            recordFailure(results, "rangeReadError", failure);
        }
    }

    private static void runDuplicatePut(
            ObjectStore store,
            ObjectKey key,
            byte[] data,
            Checksum checksum,
            Duration timeout,
            Map<String, Object> results) {
        try {
            await(
                    store.putObject(
                            key,
                            ByteBuffer.wrap(data),
                            new PutObjectOptions(
                                    "application/octet-stream",
                                    checksum,
                                    true,
                                    Map.of(),
                                    timeout)),
                    timeout);
            results.put("duplicatePutRejected", false);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            results.put(
                    "duplicatePutRejected",
                    cause instanceof ObjectAlreadyExistsException);
            if (!(cause instanceof ObjectAlreadyExistsException)) {
                recordFailure(results, "duplicatePutError", cause);
            }
        }
    }

    private static void runConditionalCreateRace(
            ObjectStore store,
            ObjectKey key,
            byte[] data,
            Checksum checksum,
            Duration timeout,
            Map<String, Object> results) {
        PutObjectOptions options = new PutObjectOptions(
                "application/octet-stream", checksum, true, Map.of(), timeout);
        CompletableFuture<PutObjectResult> first =
                store.putObject(key, ByteBuffer.wrap(data), options);
        CompletableFuture<PutObjectResult> second =
                store.putObject(key, ByteBuffer.wrap(data), options);

        int successes = 0;
        int alreadyExists = 0;
        for (CompletableFuture<PutObjectResult> attempt : List.of(first, second)) {
            try {
                await(attempt, timeout);
                successes++;
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                if (cause instanceof ObjectAlreadyExistsException) {
                    alreadyExists++;
                } else {
                    recordFailure(
                            results, "conditionalCreateRaceError", cause);
                }
            }
        }
        results.put(
                "concurrentConditionalCreateSingleWinner",
                successes == 1 && alreadyExists == 1);
    }

    private static Set<String> runListObjectCreates(
            ObjectStore store,
            String prefix,
            ObjectKey keyA,
            ObjectKey keyB,
            Duration timeout,
            Map<String, Object> results) {
        Set<String> expected = new LinkedHashSet<>();
        expected.add(keyA.value());
        expected.add(keyB.value());
        try {
            for (int i = 0; i < LIST_OBJECT_COUNT; i++) {
                byte[] data = ("list-test-" + i).getBytes(StandardCharsets.UTF_8);
                ObjectKey key = new ObjectKey(prefix + String.format("list-%02d", i));
                await(
                        store.putObject(
                                key,
                                ByteBuffer.wrap(data),
                                new PutObjectOptions(
                                        "application/octet-stream",
                                        computeCrc32c(data),
                                        true,
                                        Map.of(),
                                        timeout)),
                        timeout);
                expected.add(key.value());
            }
            results.put("listObjectsCreated", true);
        } catch (Throwable failure) {
            results.put("listObjectsCreated", false);
            recordFailure(results, "listCreateError", failure);
        }
        return expected;
    }

    private static void runPaginatedList(
            ObjectStore store,
            String prefix,
            Set<String> expectedKeys,
            Duration timeout,
            Map<String, Object> results) {
        try {
            List<String> listed = new ArrayList<>();
            Set<String> seenTokens = new HashSet<>();
            Optional<String> continuation = Optional.empty();
            int pages = 0;
            do {
                ListObjectsResult page = await(
                        store.listObjects(
                                new ObjectKeyPrefix(prefix),
                                continuation,
                                new ListObjectsOptions(LIST_PAGE_SIZE, timeout)),
                        timeout);
                pages++;
                if (pages > MAX_LIST_PAGES) {
                    throw new IllegalStateException("list pagination exceeded page guard");
                }
                listed.addAll(page.objects().stream()
                        .map(ListedObject::key)
                        .map(ObjectKey::value)
                        .toList());
                continuation = page.continuationToken();
                continuation.ifPresent(token -> {
                    if (!seenTokens.add(token)) {
                        throw new IllegalStateException(
                                "list pagination repeated a continuation token");
                    }
                });
            } while (continuation.isPresent());

            Set<String> unique = new LinkedHashSet<>(listed);
            results.put("listPaginationObserved", pages > 1);
            results.put("listNoDuplicates", unique.size() == listed.size());
            results.put(
                    "listNoMissing",
                    unique.equals(new LinkedHashSet<>(expectedKeys)));
        } catch (Throwable failure) {
            results.put("listPaginationObserved", false);
            results.put("listNoDuplicates", false);
            results.put("listNoMissing", false);
            recordFailure(results, "listError", failure);
        }
    }

    private static void runDeleteWithWrongEtag(
            ObjectStore store,
            ObjectKey key,
            Duration timeout,
            Map<String, Object> results) {
        try {
            HeadObjectResult before = await(
                    store.headObject(key, new HeadObjectOptions(timeout)), timeout);
            String wrongEtag = before.etag()
                    .map(value -> value + "-wrong")
                    .orElse("nereus-contract-wrong-etag");
            try {
                await(
                        store.deleteObject(
                                key,
                                new DeleteObjectOptions(
                                        before.objectLength(),
                                        before.checksum(),
                                        Optional.of(wrongEtag),
                                        timeout)),
                        timeout);
                results.put("wrongEtagDeletePreservesObject", false);
                return;
            } catch (Throwable expected) {
                // The authoritative postcondition is that the exact object still exists.
                Throwable cause = unwrap(expected);
                if (exitCode(cause, AdminExitCode.CONDITION_FAILED)
                        == AdminExitCode.TIMEOUT) {
                    recordFailure(
                            results, "wrongEtagDeleteAttemptError", cause);
                }
            }
            HeadObjectResult after = await(
                    store.headObject(key, new HeadObjectOptions(timeout)), timeout);
            results.put(
                    "wrongEtagDeletePreservesObject",
                    sameIdentity(before, after));
        } catch (Throwable failure) {
            results.put("wrongEtagDeletePreservesObject", false);
            recordFailure(results, "wrongEtagDeleteError", failure);
        }
    }

    private static void runDeleteWithCorrectEtag(
            ObjectStore store,
            ObjectKey key,
            Duration timeout,
            Map<String, Object> results) {
        try {
            HeadObjectResult head = await(
                    store.headObject(key, new HeadObjectOptions(timeout)), timeout);
            await(
                    store.deleteObject(
                            key,
                            new DeleteObjectOptions(
                                    head.objectLength(),
                                    head.checksum(),
                                    head.etag(),
                                    timeout)),
                    timeout);
            boolean absent;
            try {
                await(store.headObject(key, new HeadObjectOptions(timeout)), timeout);
                absent = false;
            } catch (Throwable expected) {
                Throwable cause = unwrap(expected);
                absent = isNotFound(cause);
                if (!absent) {
                    recordFailure(results, "postDeleteHeadError", cause);
                }
            }
            results.put("correctEtagDeleteRemovesObject", absent);
            results.put("postDeleteHeadNotFound", absent);
        } catch (Throwable failure) {
            results.put("correctEtagDeleteRemovesObject", false);
            results.put("postDeleteHeadNotFound", false);
            recordFailure(results, "deleteError", failure);
        }
    }

    private static void cleanupPrefix(
            ObjectStore store,
            String prefix,
            Duration timeout,
            Map<String, Object> results) {
        List<String> failures = new ArrayList<>();
        try {
            for (int pageGuard = 0; pageGuard < MAX_LIST_PAGES; pageGuard++) {
                ListObjectsResult page = await(
                        store.listObjects(
                                new ObjectKeyPrefix(prefix),
                                Optional.empty(),
                                new ListObjectsOptions(100, timeout)),
                        timeout);
                if (page.objects().isEmpty()) {
                    results.put("contractPrefixCleanedUp", failures.isEmpty());
                    results.put("cleanupFailureCount", failures.size());
                    return;
                }
                for (ListedObject object : page.objects()) {
                    try {
                        HeadObjectResult head = await(
                                store.headObject(
                                        object.key(), new HeadObjectOptions(timeout)),
                                timeout);
                        await(
                                store.deleteObject(
                                        object.key(),
                                        new DeleteObjectOptions(
                                                head.objectLength(),
                                                head.checksum(),
                                                head.etag(),
                                                timeout)),
                                timeout);
                    } catch (Throwable failure) {
                        failures.add(object.key().value() + ": "
                                + safeMessage(unwrap(failure)));
                        recordFailure(results, "cleanupObjectError", failure);
                    }
                }
                if (!failures.isEmpty()) {
                    break;
                }
            }
        } catch (Throwable failure) {
            failures.add(safeMessage(unwrap(failure)));
            recordFailure(results, "cleanupListError", failure);
        }
        results.put("contractPrefixCleanedUp", false);
        results.put("cleanupFailureCount", failures.size());
        if (!failures.isEmpty()) {
            results.put("cleanupError", failures.get(0));
        }
    }

    private static boolean evaluateResults(Map<String, Object> results) {
        return allTrue(
                results,
                "headBucketSuccess",
                "conditionalCreateSingleWinner",
                "metadataRoundTrip",
                "etagPresent",
                "headIdentityExact",
                "rangeStatusSemantics",
                "rangeChecksumValid",
                "duplicatePutRejected",
                "concurrentConditionalCreateSingleWinner",
                "listObjectsCreated",
                "listPaginationObserved",
                "listNoDuplicates",
                "listNoMissing",
                "wrongEtagDeletePreservesObject",
                "correctEtagDeleteRemovesObject",
                "postDeleteHeadNotFound",
                "contractPrefixCleanedUp");
    }

    private static boolean allTrue(Map<String, Object> values, String... names) {
        for (String name : names) {
            if (!Boolean.TRUE.equals(values.get(name))) {
                return false;
            }
        }
        return true;
    }

    private static ProviderSession createSession(AdminConfiguration config) throws Exception {
        Class<?> resolverType = Class.forName(
                config.objectStoreSecretResolverClassName());
        if (!ObjectStoreSecretResolver.class.isAssignableFrom(resolverType)) {
            throw new IllegalArgumentException(
                    "configured object-store resolver does not implement ObjectStoreSecretResolver");
        }
        ObjectStoreSecretResolver resolver = (ObjectStoreSecretResolver) resolverType
                .getDeclaredConstructor()
                .newInstance();

        Class<?> providerType = Class.forName(config.objectStore().providerClassName());
        if (!ObjectStoreProvider.class.isAssignableFrom(providerType)) {
            throw new IllegalArgumentException(
                    "configured object-store provider does not implement ObjectStoreProvider");
        }
        ObjectStoreProvider provider = (ObjectStoreProvider) providerType
                .getDeclaredConstructor()
                .newInstance();
        try {
            return new ProviderSession(
                    provider,
                    provider.create(config.objectStore(), resolver));
        } catch (Throwable failure) {
            provider.close();
            throw failure;
        }
    }

    private static Duration operationTimeout(Duration total) {
        long seconds = Math.max(1, total.toSeconds() / 50);
        return Duration.ofSeconds(Math.min(seconds, 30));
    }

    private static <T> T await(
            CompletableFuture<T> future,
            Duration timeout) throws Exception {
        return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static boolean sameIdentity(
            HeadObjectResult first,
            HeadObjectResult second) {
        return first.key().equals(second.key())
                && first.objectLength() == second.objectLength()
                && first.checksum().equals(second.checksum())
                && first.etag().equals(second.etag());
    }

    private static boolean isNotFound(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.code() == ErrorCode.OBJECT_NOT_FOUND;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer exact = buffer.asReadOnlyBuffer();
        byte[] bytes = new byte[exact.remaining()];
        exact.get(bytes);
        return bytes;
    }

    private static Checksum computeCrc32c(byte[] data) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(data);
        return new Checksum(
                ChecksumType.CRC32C,
                String.format("%08x", crc.getValue()));
    }

    private static ObjectKey persistenceKey(String runId) {
        return new ObjectKey(PERSISTENCE_PREFIX + runId + "/marker");
    }

    private static byte[] persistencePayload(String runId) {
        return ("nereus-object-store-restart-gate-" + runId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, String> persistenceMetadata(String runId) {
        return Map.of(
                "restart-gate-run-id", runId,
                "restart-gate-schema", "v1");
    }

    private static Map<String, Object> persistenceEvidence(
            PersistenceAction action, String runId) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("schemaVersion", 1);
        evidence.put(
                "command",
                "object-store persistence " + action.commandName());
        evidence.put("runId", runId);
        return evidence;
    }

    private static Throwable unwrap(Throwable failure) {
        return AdminFailureClassifier.unwrap(failure);
    }

    private static AdminExitCode exitCode(
            Throwable failure,
            AdminExitCode fallback) {
        return AdminFailureClassifier.classify(failure, fallback);
    }

    private static String safeMessage(Throwable failure) {
        return AdminFailureClassifier.safeMessage(failure);
    }

    private static void recordFailure(
            Map<String, Object> evidence,
            String field,
            Throwable failure) {
        Throwable cause = unwrap(failure);
        evidence.put(field, safeMessage(cause));
        if (exitCode(cause, AdminExitCode.CONDITION_FAILED)
                == AdminExitCode.TIMEOUT) {
            evidence.put("timeoutObserved", true);
        }
    }

    private static void stopAfterTimeout(
            Map<String, Object> evidence) throws java.util.concurrent.TimeoutException {
        if (Boolean.TRUE.equals(evidence.get("timeoutObserved"))) {
            throw new java.util.concurrent.TimeoutException(
                    "object-store contract operation timed out");
        }
    }

    private static String createJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(",\n");
            }
            first = false;
            sb.append("  \"");
            sb.append(entry.getKey());
            sb.append("\": ");
            appendJsonValue(sb, entry.getValue());
        }
        sb.append("\n}");
        return sb.toString();
    }

    private static void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String string) {
            sb.append('"').append(escape(string)).append('"');
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else {
            sb.append('"').append(escape(String.valueOf(value))).append('"');
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static void printEvidence(
            String evidence,
            Optional<java.nio.file.Path> outputFile) {
        AdminEvidenceWriter.printEvidence(evidence);
        outputFile.ifPresent(path ->
                AdminEvidenceWriter.writeEvidence(path, evidence));
    }

    private record ProviderSession(
            ObjectStoreProvider provider,
            ObjectStore store) implements AutoCloseable {
        private ProviderSession {
            java.util.Objects.requireNonNull(provider, "provider");
            java.util.Objects.requireNonNull(store, "store");
        }

        @Override
        public void close() {
            provider.close();
        }
    }

    private enum PersistenceAction {
        CREATE("create"),
        VERIFY("verify"),
        CLEANUP("cleanup");

        private final String commandName;

        PersistenceAction(String commandName) {
            this.commandName = commandName;
        }

        String commandName() {
            return commandName;
        }
    }
}
