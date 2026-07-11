/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api.target;

/** Durable physical target discriminator. A value does not imply that an IO adapter is installed. */
public enum ReadTargetType {
    OBJECT_SLICE,
    BOOKKEEPER_ENTRY_RANGE
}
