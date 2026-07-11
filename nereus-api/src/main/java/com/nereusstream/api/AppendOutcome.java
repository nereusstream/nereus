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

package com.nereusstream.api;

/** Commit certainty carried by an append failure. */
public enum AppendOutcome {
    /** The failed attempt is known not to have advanced the stream head. */
    KNOWN_NOT_COMMITTED,

    /** The stream-head write may have committed and the caller must resolve the original attempt before retrying. */
    MAY_HAVE_COMMITTED,

    /** The stream head is known to contain the append, even though the requested operation did not return success. */
    KNOWN_COMMITTED
}
