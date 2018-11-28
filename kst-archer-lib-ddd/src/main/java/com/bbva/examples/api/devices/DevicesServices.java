package com.bbva.examples.api.devices;

import com.bbva.avro.Devices;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.commands.write.CommandRecordMetadata;
import com.bbva.examples.Application;
import com.bbva.examples.MainHandler;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.aggregates.DeviceAggregate;
import org.apache.avro.generic.GenericRecord;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

@Path("/devices")
public class DevicesServices {

    private final ApplicationServices app;

    public DevicesServices(ApplicationServices app) {
        this.app = app;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean createDevice(Devices device) throws InterruptedException, ExecutionException {
        ResultsBean result;
        ReadableStore<String, String> publicUuidStore = null;

        try {
            publicUuidStore = app.getStore(Application.PUBLIC_UUID_STORE_BASENAME);

        } catch (NullPointerException e) {
        }

        try {
            if (publicUuidStore == null || !publicUuidStore.exists(device.getPublicUuid())) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                CommandRecordMetadata metadata = app.persistsCommandTo(DeviceAggregate.baseName()).create(device,
                        optionalHeaders, (key, e) -> {
                            if (e != null)
                                e.printStackTrace();
                        });
                result = new ResultsBean(202, "Accepted", "{\"entityId\":\"" + metadata.entityId() + "\"}");

            } else {
                result = new ResultsBean(409, "Conflict");
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

    @POST
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean updateDevice(@PathParam("id") final String id, Devices devices)
            throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Devices> getStore(DeviceAggregate.baseName()).exists(id)) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                app.persistsCommandTo(DeviceAggregate.baseName()).processAction(MainHandler.UPDATE_DEVICE_ACTION, id,
                        devices, optionalHeaders, (key, e) -> {
                            if (e != null)
                                e.printStackTrace();
                        });
                result = new ResultsBean(202, "Accepted");
            } else {
                result = new ResultsBean(404, "Not Found");
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean getDevice(@PathParam("id") final String id) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            // GenericRecord test = app.<String,
            // GenericRecord>getStore(Application.TEST_QUERY_STORE_BASENAME).findById(id);
            GenericRecord test2 = app.<String, GenericRecord> getStore(Application.TEST_QUERY_STORE_BASENAME + "2")
                    .findById(id);
            // Devices devices = app.<String, Devices>getStore(DeviceAggregate.baseName()).findById(id);
            if (/* test != null && */test2 != null) {
                result = new ResultsBean(202, "Accepted", /* test.toString() + */test2.toString());
            } else {
                result = new ResultsBean(404, "Not Found");
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean deleteDevice(@PathParam("id") final String id) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Devices> getStore(DeviceAggregate.baseName()).exists(id)) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                app.persistsCommandTo(DeviceAggregate.baseName()).delete(id, Devices.class, optionalHeaders,
                        (key, e) -> {
                            if (e != null)
                                e.printStackTrace();
                        });
                result = new ResultsBean(202, "Accepted");
            } else {
                result = new ResultsBean(404, "Not Found");
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }
}
