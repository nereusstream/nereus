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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import java.util.Objects;

/** Typed NWKCP1 parser failure; callers never branch on message text. */
public final class Nwkcp1DecodingException extends IllegalArgumentException {
    private final Nwkcp1RejectionV1 rejection;

    public Nwkcp1DecodingException(Nwkcp1RejectionV1 rejection, String message) {
        super(message);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public Nwkcp1DecodingException(Nwkcp1RejectionV1 rejection, String message, Throwable cause) {
        super(message, cause);
        this.rejection = Objects.requireNonNull(rejection, "rejection");
    }

    public Nwkcp1RejectionV1 rejection() {
        return rejection;
    }
}
