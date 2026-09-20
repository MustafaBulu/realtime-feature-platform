package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter;
import java.util.Optional;

final class FeatureFilterEvaluator {

    private FeatureFilterEvaluator() {
    }

    static boolean matches(PlatformEvent event, FeatureFilter filter) {
        if (filter == null) {
            return true;
        }

        Optional<Object> fieldValue = EventFieldResolver.resolve(event, filter.field());
        return switch (filter.operator()) {
            case EXISTS -> fieldValue.isPresent();
            case EQ -> fieldValue.map(value -> compare(value, filter.value()) == 0).orElse(false);
            case NE -> fieldValue.map(value -> compare(value, filter.value()) != 0).orElse(false);
            case GT -> fieldValue.map(value -> compare(value, filter.value()) > 0).orElse(false);
            case GTE -> fieldValue.map(value -> compare(value, filter.value()) >= 0).orElse(false);
            case LT -> fieldValue.map(value -> compare(value, filter.value()) < 0).orElse(false);
            case LTE -> fieldValue.map(value -> compare(value, filter.value()) <= 0).orElse(false);
        };
    }

    private static int compare(Object left, String right) {
        if (left instanceof Number number) {
            return Double.compare(number.doubleValue(), Double.parseDouble(right));
        }
        return left.toString().compareTo(right);
    }
}
