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

import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.AttachmentRef;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ChildReceiptRef;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.ReceiptRejectedException;
import com.nereusstream.storage.object.evidence.M3FinalReceiptV1.RejectionCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Resolves one already-published M3 Final and every explicitly bound child and attachment. */
public final class M3FinalResolverV1 {
    public static final long MAX_SINGLE_EVIDENCE_BYTES = 67_108_864L;
    public static final long MAX_TOTAL_EVIDENCE_BYTES = 268_435_456L;

    private M3FinalResolverV1() {}

    public record Resolution(
            M3FinalReceiptV1.RootSourceTuple sourceTuple,
            Set<String> promotedScenarios,
            long childReceipts,
            long attachments,
            long tests,
            long verifiedBytes) {
        public Resolution {
            promotedScenarios = Set.copyOf(promotedScenarios);
        }
    }

    public static Resolution resolve(Path repositoryRoot, Path receiptFile) {
        Objects.requireNonNull(repositoryRoot, "repositoryRoot");
        Objects.requireNonNull(receiptFile, "receiptFile");
        Path root = verifiedRoot(repositoryRoot);
        M3FinalReceiptV1.Receipt receipt = M3FinalReceiptV1.parseCanonicalFile(receiptFile);
        verifySourceLocks(root, receipt.sourceTuple().sourceLocksSha256());
        long attachments = 0;
        long tests = 0;
        long verifiedBytes = 0;
        for (ChildReceiptRef child : receipt.childReceipts()) {
            byte[] childBytes = readVerified(root, child.path(), child.bytes(), child.sha256());
            verifiedBytes = checkedTotal(verifiedBytes, childBytes.length);
            M3FinalReceiptV1.validateChildReceiptCanonical(childBytes, child, receipt.sourceTuple());
            tests = checkedAdd(tests, child.tests());
            for (AttachmentRef attachment : child.attachments()) {
                byte[] attachmentBytes = readVerified(root, attachment.path(), attachment.bytes(), attachment.sha256());
                verifiedBytes = checkedTotal(verifiedBytes, attachmentBytes.length);
                attachments++;
            }
        }
        return new Resolution(
                receipt.sourceTuple(),
                new LinkedHashSet<>(receipt.scenarios()),
                receipt.childReceipts().size(),
                attachments,
                tests,
                verifiedBytes);
    }

    private static byte[] readVerified(Path root, String path, long expectedBytes, String expectedSha256) {
        if (expectedBytes > MAX_SINGLE_EVIDENCE_BYTES) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "single evidence file exceeds resolver cap");
        }
        Path current = root;
        for (String segment : M3FinalReceiptV1.validatePath(path)) {
            current = current.resolve(segment);
            BasicFileAttributes attributes = attributes(current);
            if (attributes.isSymbolicLink() || Files.isSymbolicLink(current)) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence path contains a symlink");
            }
        }
        Path normalized = current.normalize();
        if (!normalized.startsWith(root)) {
            throw reject(RejectionCode.PATH_INVALID, "evidence path leaves repository root");
        }
        BasicFileAttributes before = attributes(normalized);
        if (!before.isRegularFile() || before.size() != expectedBytes) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence file length differs");
        }
        byte[] bytes = readBounded(normalized, expectedBytes);
        BasicFileAttributes after = attributes(normalized);
        if (!after.isRegularFile()
                || before.size() != after.size()
                || (before.fileKey() != null
                        && after.fileKey() != null
                        && !before.fileKey().equals(after.fileKey()))) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence file changed while reading");
        }
        if (!sha256(bytes).equals(expectedSha256)) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence file SHA-256 differs");
        }
        return bytes;
    }

    private static void verifySourceLocks(Path root, String expectedSha256) {
        Path current = root;
        for (String segment : new String[] {"docs", "v2", "source-locks.json"}) {
            current = current.resolve(segment);
            BasicFileAttributes attributes = attributes(current);
            if (attributes.isSymbolicLink() || Files.isSymbolicLink(current)) {
                throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source-lock path contains a symlink");
            }
        }
        BasicFileAttributes before = attributes(current);
        if (!before.isRegularFile() || before.size() <= 0 || before.size() > 1_048_576) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source-lock bytes outside cap");
        }
        byte[] raw = readBounded(current, before.size());
        if (!sha256(raw).equals(expectedSha256)) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source-lock SHA-256 differs from Final source tuple");
        }
    }

    private static byte[] readBounded(Path path, long expectedBytes) {
        try (InputStream input = java.nio.channels.Channels.newInputStream(Files.newByteChannel(
                        path, Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)));
                ByteArrayOutputStream output = new ByteArrayOutputStream(Math.toIntExact(expectedBytes))) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                if (checkedAdd(output.size(), count) > expectedBytes) {
                    throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence file grew while reading");
                }
                output.write(buffer, 0, count);
            }
            if (output.size() != expectedBytes) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence file shortened while reading");
            }
            return output.toByteArray();
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException | ArithmeticException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_SET_INVALID, "cannot read bounded evidence file", failure);
        }
    }

    private static Path verifiedRoot(Path repositoryRoot) {
        try {
            if (Files.isSymbolicLink(repositoryRoot)) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "repository root is a symlink");
            }
            Path root = repositoryRoot.toRealPath(LinkOption.NOFOLLOW_LINKS);
            BasicFileAttributes attributes = attributes(root);
            if (!attributes.isDirectory() || attributes.isSymbolicLink()) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "repository root is not a directory");
            }
            return root;
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_SET_INVALID, "cannot resolve repository root", failure);
        }
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_SET_INVALID, "cannot read evidence attributes", failure);
        }
    }

    private static long checkedTotal(long total, long value) {
        long next = checkedAdd(total, value);
        if (next > MAX_TOTAL_EVIDENCE_BYTES) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "total evidence bytes exceed resolver cap");
        }
        return next;
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_SET_INVALID, "checked evidence arithmetic overflow", failure);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private static ReceiptRejectedException reject(RejectionCode code, String detail) {
        return new ReceiptRejectedException(code, detail);
    }
}
