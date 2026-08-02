/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.junit.jupiter.api.Test;

class ManagedCursorPublicSurfaceClassificationTest {
    private enum Classification {
        DURABLE,
        LOCAL,
        READ,
        UNSUPPORTED
    }

    private static final Map<String, Classification> CLASSIFICATION = Stream.of(
                    names(
                            Classification.DURABLE,
                            "getName",
                            "getProperties",
                            "getCursorProperties",
                            "putCursorProperty",
                            "setCursorProperties",
                            "removeCursorProperty",
                            "putProperty",
                            "removeProperty",
                            "markDelete",
                            "asyncMarkDelete",
                            "delete",
                            "asyncDelete",
                            "getMarkDeletedPosition",
                            "getPersistentMarkDeletedPosition",
                            "clearBacklog",
                            "asyncClearBacklog",
                            "skipEntries",
                            "asyncSkipEntries",
                            "resetCursor",
                            "asyncResetCursor",
                            "close",
                            "asyncClose",
                            "isDurable",
                            "getTotalNonContiguousDeletedMessagesRange",
                            "getNonContiguousDeletedMessagesRangeSerializedSize",
                            "getLastIndividualDeletedRange",
                            "trimDeletedEntries",
                            "getDeletedBatchIndexesAsLongArray",
                            "isCursorDataFullyPersistable",
                            "isMessageDeleted",
                            "duplicateNonDurableCursor",
                            "getBatchPositionAckSet"),
                    names(
                            Classification.LOCAL,
                            "getLastActive",
                            "updateLastActive",
                            "getReadPosition",
                            "rewind",
                            "seek",
                            "setActive",
                            "setInactive",
                            "setAlwaysInactive",
                            "isActive",
                            "getThrottleMarkDelete",
                            "setThrottleMarkDelete",
                            "getManagedLedger",
                            "getStats",
                            "checkAndUpdateReadPositionChanged",
                            "isClosed",
                            "getManagedCursorAttributes",
                            "getCursorStats",
                            "applyMaxSizeCap",
                            "updateReadStats"),
                    names(
                            Classification.READ,
                            "readEntries",
                            "asyncReadEntries",
                            "asyncReadEntriesWithSkip",
                            "getNthEntry",
                            "asyncGetNthEntry",
                            "readEntriesOrWait",
                            "asyncReadEntriesOrWait",
                            "asyncReadEntriesWithSkipOrWait",
                            "cancelPendingReadRequest",
                            "hasMoreEntries",
                            "getNumberOfEntries",
                            "getNumberOfEntriesInBacklog",
                            "findNewestMatching",
                            "scan",
                            "asyncFindNewestMatching",
                            "replayEntries",
                            "asyncReplayEntries",
                            "getFirstPosition",
                            "getNumberOfEntriesSinceFirstNotAckedMessage",
                            "getEstimatedSizeSinceMarkDeletePosition"),
                    names(Classification.UNSUPPORTED, "skipNonRecoverableLedger", "periodicRollover"))
            .flatMap(Arrays::stream)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    private static final Map<String, Classification> OPTIONAL_HISTORICAL_CLASSIFICATION =
            Map.of("hasBacklog", Classification.READ);

    private static final Map<String, Long> OVERLOAD_COUNTS = Map.ofEntries(
            Map.entry("asyncReadEntries", 2L),
            Map.entry("readEntriesOrWait", 2L),
            Map.entry("asyncReadEntriesOrWait", 2L),
            Map.entry("asyncReadEntriesWithSkipOrWait", 2L),
            Map.entry("markDelete", 2L),
            Map.entry("asyncMarkDelete", 2L),
            Map.entry("delete", 2L),
            Map.entry("asyncDelete", 2L),
            Map.entry("rewind", 2L),
            Map.entry("seek", 2L),
            Map.entry("hasBacklog", 2L),
            Map.entry("findNewestMatching", 2L),
            Map.entry("asyncFindNewestMatching", 3L),
            Map.entry("asyncReplayEntries", 2L));

    @Test
    void everyLockedManagedCursorMethodIsClassifiedAndImplemented() throws Exception {
        Method[] methods = Arrays.stream(ManagedCursor.class.getMethods())
                .filter(method -> method.getDeclaringClass() == ManagedCursor.class)
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .toArray(Method[]::new);
        Map<String, Long> actualCounts =
                Arrays.stream(methods).collect(Collectors.groupingBy(Method::getName, Collectors.counting()));
        Map<String, Classification> expectedClassification = Stream.concat(
                        CLASSIFICATION.entrySet().stream(),
                        OPTIONAL_HISTORICAL_CLASSIFICATION.entrySet().stream()
                                .filter(entry -> actualCounts.containsKey(entry.getKey())))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        assertThat(actualCounts.keySet()).containsExactlyInAnyOrderElementsOf(expectedClassification.keySet());
        actualCounts.forEach(
                (name, count) -> assertThat(count).as(name).isEqualTo(OVERLOAD_COUNTS.getOrDefault(name, 1L)));
        assertThat(expectedClassification.values()).containsAll(Set.of(Classification.values()));

        for (Method method : methods) {
            if (method.getName().equals("hasBacklog")) {
                assertThat(isExactHistoricalHasBacklogDefault(method))
                        .as(signature(method))
                        .isTrue();
            }
            Method implementation = NereusManagedCursor.class.getMethod(method.getName(), method.getParameterTypes());
            if (isIntentionallyInheritedDefault(method)) {
                assertThat(method.isDefault()).as(signature(method)).isTrue();
            } else {
                assertThat(implementation.getDeclaringClass())
                        .as(signature(method))
                        .isEqualTo(NereusManagedCursor.class);
            }
        }
    }

    private static boolean isIntentionallyInheritedDefault(Method method) {
        if (method.getName().equals("seek")
                && Arrays.equals(method.getParameterTypes(), new Class<?>[] {Position.class})) {
            return true;
        }
        return isExactHistoricalHasBacklogDefault(method);
    }

    private static boolean isExactHistoricalHasBacklogDefault(Method method) {
        return method.getName().equals("hasBacklog")
                && method.isDefault()
                && method.getReturnType() == boolean.class
                && (method.getParameterCount() == 0
                        || Arrays.equals(method.getParameterTypes(), new Class<?>[] {boolean.class}));
    }

    private static String signature(Method method) {
        return method.getName() + Arrays.toString(method.getParameterTypes());
    }

    private static Map.Entry<String, Classification>[] names(Classification classification, String... names) {
        @SuppressWarnings("unchecked")
        Map.Entry<String, Classification>[] entries = Arrays.stream(names)
                .map(name -> Map.entry(name, classification))
                .toArray(Map.Entry[]::new);
        return entries;
    }
}
