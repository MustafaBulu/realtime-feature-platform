package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FeatureDurationParser {

    private static final Pattern COMPACT_DURATION = Pattern.compile("([1-9][0-9]*)(ms|s|m|h|d)");

    private FeatureDurationParser() {
    }

    public static Duration parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("duration must not be blank");
        }

        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith("pt") || normalized.startsWith("p")) {
            return requirePositive(Duration.parse(normalized.toUpperCase(Locale.ROOT)));
        }

        Matcher matcher = COMPACT_DURATION.matcher(normalized);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("duration must be ISO-8601 or compact duration like 10m");
        }

        long amount = Long.parseLong(matcher.group(1));
        Duration duration = switch (matcher.group(2)) {
            case "ms" -> Duration.ofMillis(amount);
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            case "h" -> Duration.ofHours(amount);
            case "d" -> Duration.ofDays(amount);
            default -> throw new IllegalArgumentException("unsupported duration unit");
        };
        return requirePositive(duration);
    }

    private static Duration requirePositive(Duration duration) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("duration must be positive");
        }
        return duration;
    }
}
