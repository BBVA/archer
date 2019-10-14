package com.bbva.common.consumers.contexts;

import com.bbva.common.consumers.record.CRecord;

/**
 * Consumer context
 */
public interface ConsumerContext {

    CRecord consumedRecord();

}
