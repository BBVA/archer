package com.bbva.ddd.domain.aggregates.util;

import com.bbva.ddd.domain.aggregates.annotations.Aggregate;

public final class AggregatesUtil {

    public static String getBaseName(final Class aggregateClass) {
        return Aggregate.class.cast(aggregateClass.getAnnotation(Aggregate.class)).baseName();
    }

}
