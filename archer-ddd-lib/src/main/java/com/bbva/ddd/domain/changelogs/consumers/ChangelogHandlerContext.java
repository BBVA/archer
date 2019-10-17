package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;

public class ChangelogHandlerContext extends HandlerContextImpl {

    public ChangelogHandlerContext(final Producer producer, final CRecord consumedRecord) {
        super(producer, new ChangelogRecord(consumedRecord));
    }

    @Override
    public ChangelogRecord consumedRecord() {
        return (ChangelogRecord) consumedRecord;
    }
}
