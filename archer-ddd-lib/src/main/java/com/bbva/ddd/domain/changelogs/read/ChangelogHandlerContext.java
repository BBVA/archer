package com.bbva.ddd.domain.changelogs.read;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.consumers.HandlerContextImpl;

public class ChangelogHandlerContext extends HandlerContextImpl {

    public ChangelogHandlerContext(final CRecord consumedRecord) {
        super(new ChangelogRecord(consumedRecord));
    }

    @Override
    public ChangelogRecord consumedRecord() {
        return (ChangelogRecord) consumedRecord;
    }
}
