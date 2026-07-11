/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api.target;

/** Protocol-neutral physical selection for one committed logical range. */
public sealed interface ReadTarget permits ObjectSliceReadTarget, BookKeeperEntryRangeReadTarget {
    ReadTargetType type();

    int version();
}
