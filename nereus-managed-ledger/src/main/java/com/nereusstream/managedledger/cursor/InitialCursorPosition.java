/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

/** Internal non-null initial durable cursor position selected by the ManagedLedger facade. */
public sealed interface InitialCursorPosition {
    record Earliest() implements InitialCursorPosition {
    }

    record Latest() implements InitialCursorPosition {
    }

    record AtOffset(long nextReadOffset) implements InitialCursorPosition {
        public AtOffset {
            if (nextReadOffset < 0) {
                throw new IllegalArgumentException("nextReadOffset must be non-negative");
            }
        }
    }
}
