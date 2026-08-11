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

package com.nereusstream.domain.aggregate;

import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.util.Objects;

/** The immutable Topic Binding projection of a logical aggregate. */
public record TopicBindingV1(
        ProtocolKindV1 protocolKind,
        TopicBindingId bindingId,
        ProtocolCellIdentity cellIdentity,
        TopicIncarnationIdentity incarnationIdentity) {
    public TopicBindingV1 {
        Objects.requireNonNull(protocolKind, "protocolKind");
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(cellIdentity, "cellIdentity");
        Objects.requireNonNull(incarnationIdentity, "incarnationIdentity");
    }
}
