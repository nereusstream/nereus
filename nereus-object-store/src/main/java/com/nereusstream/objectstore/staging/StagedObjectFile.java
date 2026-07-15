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

package com.nereusstream.objectstore.staging;

import com.nereusstream.api.Checksum;
import com.nereusstream.objectstore.ReplayableObjectUpload;

/** Sealed, close-owned private file that can be replayed for provider upload attempts. */
public interface StagedObjectFile extends ReplayableObjectUpload {
    long sealedLength();

    Checksum storageCrc32c();

    Checksum contentSha256();

    @Override
    default long contentLength() {
        return sealedLength();
    }
}
