package com.bbva.dataprocessors.contexts.sql;

import com.bbva.dataprocessors.contexts.ProcessorContext;
import io.confluent.ksql.KsqlContext;

/**
 * SQL processor context interface
 */
public interface SQLProcessorContext extends ProcessorContext {

    /**
     * Get ksql context
     *
     * @return context
     */
    KsqlContext ksqlContext();

    /**
     * Print processor datasources
     */
    void printDataSources();
}
