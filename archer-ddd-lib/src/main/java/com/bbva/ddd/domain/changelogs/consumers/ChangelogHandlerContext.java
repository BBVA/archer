package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;

public class ChangelogHandlerContext extends HandlerContextImpl {

    public ChangelogHandlerContext(final CRecord consumedRecord, final Producer producer, final Boolean isReplay) {
        super(new ChangelogRecord(consumedRecord), producer, isReplay);
    }

    @Override
    public ChangelogRecord consumedRecord() {
        return (ChangelogRecord) consumedRecord;
    }
}
