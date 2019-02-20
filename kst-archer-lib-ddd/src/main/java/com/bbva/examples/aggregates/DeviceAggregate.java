package com.bbva.examples.aggregates;

import com.bbva.avro.Devices;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;

@Aggregate(baseName = "test_devices")
public class DeviceAggregate extends SpecificAggregate<String, Devices> {

    public DeviceAggregate(final String id, final Devices devices) {
        super(id, devices);
    }

    public static String baseName() {
        return "test_devices";
    }

}
