package com.bbva.ddd.domain.aggregates.annotations;

import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateParent {
    Class<? extends AbstractAggregateBase> value();
}
