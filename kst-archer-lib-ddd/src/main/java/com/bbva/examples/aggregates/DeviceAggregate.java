package com.bbva.examples.aggregates;

import com.bbva.avro.Devices;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;

import java.lang.reflect.InvocationTargetException;

@Aggregate(baseName = "test_devices")
public class DeviceAggregate extends AbstractAggregateBase<String, Devices> {

    public DeviceAggregate(final String id, final Devices devices) {
        super(id, devices);
    }

    public void update(final Devices modifiedData, final CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            System.out.println("change data from " + getData() + " to " + modifiedData);
        });
    }

    public void delete(final CommandRecord commandMessage) {
        try {
            apply("delete", commandMessage, (id, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                System.out.println("remove device " + getId());
            });
        } catch (final NoSuchMethodException e) {
            e.printStackTrace();
        } catch (final InstantiationException e) {
            e.printStackTrace();
        } catch (final IllegalAccessException e) {
            e.printStackTrace();
        } catch (final InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public static String baseName() {
        return "test_devices";
    }

}
