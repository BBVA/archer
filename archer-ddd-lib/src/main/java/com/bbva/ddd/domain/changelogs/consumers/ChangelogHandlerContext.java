package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;

/**
 * Changelog consumed context to handle events before consume.
 */
public class ChangelogHandlerContext extends HandlerContextImpl {

    /**
     * Constructor
     *
     * @param record   consumed record
     * @param producer producer instance
     * @param isReplay flag of replay
     */
    public ChangelogHandlerContext(final CRecord record, final Producer producer, final Boolean isReplay) {
        super(new ChangelogRecord(record), producer, isReplay);
    }

    @Override
    public ChangelogRecord consumedRecord() {
        return (ChangelogRecord) consumedRecord;
    }
}
