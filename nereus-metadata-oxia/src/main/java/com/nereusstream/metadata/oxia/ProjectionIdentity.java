/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ProjectionIdentity {
    private ProjectionIdentity() { }

    public static String encode(Optional<ProjectionRef> projection) {
        Optional<ProjectionRef> exact =
                Objects.requireNonNull(projection, "projection");
        StringBuilder result = new StringBuilder();
        append(result, "projectionRef");
        if (exact.isEmpty()) {
            append(result, "absent");
            return result.toString();
        }
        ProjectionRef value = exact.orElseThrow();
        append(result, "present");
        append(result, value.type().name());
        append(result, value.value());
        return result.toString();
    }

    public static Optional<ProjectionRef> decode(String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        List<String> values = new ArrayList<>();
        int cursor = 0;
        while (cursor < encoded.length()) {
            int colon = encoded.indexOf(':', cursor);
            if (colon < 0) throw new IllegalArgumentException("malformed projection identity");
            int bytes = Integer.parseInt(encoded.substring(cursor, colon));
            cursor = colon + 1;
            int end = cursor;
            int used = 0;
            while (end < encoded.length() && used < bytes) {
                int cp = encoded.codePointAt(end);
                used += new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8).length;
                end += Character.charCount(cp);
            }
            if (used != bytes) throw new IllegalArgumentException("malformed projection identity length");
            values.add(encoded.substring(cursor, end));
            cursor = end;
        }
        if (values.size() == 2 && values.get(0).equals("projectionRef") && values.get(1).equals("absent")) {
            return Optional.empty();
        }
        if (values.size() == 4 && values.get(0).equals("projectionRef") && values.get(1).equals("present")) {
            return Optional.of(new ProjectionRef(ProjectionType.valueOf(values.get(2)), values.get(3)));
        }
        throw new IllegalArgumentException("unsupported projection identity");
    }

    private static void append(StringBuilder target, String value) {
        target.append(value.getBytes(StandardCharsets.UTF_8).length)
                .append(':')
                .append(value);
    }
}
