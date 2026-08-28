package com.mustafabulu.realtimefeatureplatform.eventmodel;

import java.util.Objects;

public record EntityRef(String type, String id) {

    public EntityRef {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(id, "id must not be null");

        if (type.isBlank()) {
            throw new IllegalArgumentException("type must not be blank");
        }
        if (id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
    }
}
