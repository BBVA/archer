package com.bbva.ddd.domain.changelogs.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;

public class ChangelogHandlerContext extends HandlerContextImpl {

    public ChangelogHandlerContext(final CRecord consumedRecord) {
        super(new ChangelogRecord(consumedRecord));
    }

    @Override
    public ChangelogRecord consumedRecord() {
        return (ChangelogRecord) consumedRecord;
    }
}
