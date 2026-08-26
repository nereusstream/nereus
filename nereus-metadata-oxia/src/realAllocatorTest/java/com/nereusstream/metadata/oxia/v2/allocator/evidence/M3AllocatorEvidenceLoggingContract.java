/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;

/** Fail-closed runtime proof for the one exact native-harness cleanup-warning filter. */
final class M3AllocatorEvidenceLoggingContract {
    static final String LOGGER_NAME = "org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl";
    static final String EXPECTED_CLEANUP_WARNING = "Ledger was already deleted";
    static final String OTHER_WARNING = "A different ManagedLedger warning must remain visible";

    private M3AllocatorEvidenceLoggingContract() {}

    static void requireInstalled() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        LoggerConfig logger = context.getConfiguration().getLoggerConfig(LOGGER_NAME);
        if (!LOGGER_NAME.equals(logger.getName())) {
            throw new IllegalStateException("formal allocator ManagedLedger logger filter is not installed exactly");
        }
        Filter filter = logger.getFilter();
        if (filter == null) {
            throw new IllegalStateException("formal allocator ManagedLedger logger has no exact cleanup filter");
        }
        requireResult(
                filter,
                Level.WARN,
                EXPECTED_CLEANUP_WARNING,
                Filter.Result.DENY,
                "exact expected cleanup WARN");
        requireResult(
                filter,
                Level.WARN,
                OTHER_WARNING,
                Filter.Result.NEUTRAL,
                "different ManagedLedger WARN");
        requireResult(
                filter,
                Level.ERROR,
                EXPECTED_CLEANUP_WARNING,
                Filter.Result.ACCEPT,
                "same-message ManagedLedger ERROR");
    }

    private static void requireResult(
            Filter filter, Level level, String message, Filter.Result expected, String description) {
        Filter.Result actual = filter.filter(Log4jLogEvent.newBuilder()
                .setLoggerName(LOGGER_NAME)
                .setLevel(level)
                .setMessage(new SimpleMessage(message))
                .build());
        if (actual != expected) {
            throw new IllegalStateException(
                    description + " logger result was " + actual + " instead of " + expected);
        }
    }
}
