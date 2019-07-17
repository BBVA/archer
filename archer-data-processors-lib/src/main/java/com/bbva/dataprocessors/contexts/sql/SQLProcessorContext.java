package com.bbva.dataprocessors.contexts.sql;

import com.bbva.dataprocessors.contexts.ProcessorContext;
import io.confluent.ksql.KsqlContext;

public interface SQLProcessorContext extends ProcessorContext {

    KsqlContext ksqlContext();

    void printDataSources();
}
