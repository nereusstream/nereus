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

package com.nereusstream.storage.object.evidence;

import java.nio.file.Path;

/** Production entry point for resolving an already-published exact-source M3 Final receipt. */
public final class M3FinalReceiptCli {
    private M3FinalReceiptCli() {}

    public static void main(String[] arguments) {
        if (arguments.length != 3 || !"validate".equals(arguments[0])) {
            throw new IllegalArgumentException("usage: validate <repository-root> <canonical-m3-final-receipt>");
        }
        M3FinalResolverV1.Resolution resolution =
                M3FinalResolverV1.resolve(Path.of(arguments[1]), Path.of(arguments[2]));
        System.out.println("M3 Final receipt PASS: source="
                + resolution.sourceTuple().nereusCommit()
                + " scenarios="
                + resolution.promotedScenarios().size()
                + " children="
                + resolution.childReceipts()
                + " attachments="
                + resolution.attachments()
                + " tests="
                + resolution.tests());
    }
}
