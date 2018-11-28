package com.bbva.examples.aggregates;

import com.bbva.avro.Devices;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;

import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

@Aggregate(baseName = "test_devices")
public class DeviceAggregate extends AbstractAggregateBase<String, Devices> {

    public DeviceAggregate(String id, Devices devices) {
        super(id, devices);
    }

    public void update(Devices modifiedData, CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            System.out.println("change data from " + getData() + " to " + modifiedData);
        });
    }

    public void delete(CommandRecord commandMessage) {
        try {
            apply("delete", commandMessage, (id, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                System.out.println("remove device " + getId());
            });
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public static String baseName() {
        return "test_devices";
    }

}
