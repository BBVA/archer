package com.bbva.ddd.domain.changelogs.repository.aggregates.annotations;

import com.bbva.ddd.domain.changelogs.repository.aggregates.AbstractAggregate;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface AggregateParent {
    Class<? extends AbstractAggregate> value();
}
