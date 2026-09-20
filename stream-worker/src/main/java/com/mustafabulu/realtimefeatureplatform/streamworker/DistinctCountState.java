package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

final class DistinctCountState {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final Set<String> values;

    private DistinctCountState(Set<String> values) {
        this.values = values;
    }

    static DistinctCountState empty() {
        return new DistinctCountState(new TreeSet<>());
    }

    static DistinctCountState parse(String value) {
        if (value == null || value.isBlank()) {
            return empty();
        }
        Set<String> decoded = java.util.Arrays.stream(value.split(",", -1))
                .map(DistinctCountState::decode)
                .collect(Collectors.toCollection(TreeSet::new));
        return new DistinctCountState(decoded);
    }

    DistinctCountState add(Object value) {
        values.add(value.toString());
        return this;
    }

    long count() {
        return values.size();
    }

    String serialize() {
        return values.stream()
                .map(DistinctCountState::encode)
                .collect(Collectors.joining(","));
    }

    private static String encode(String value) {
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(DECODER.decode(value), StandardCharsets.UTF_8);
    }
}
