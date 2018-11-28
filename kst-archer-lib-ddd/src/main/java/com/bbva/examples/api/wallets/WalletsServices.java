package com.bbva.examples.api.wallets;

import com.bbva.avro.Wallets;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.commands.write.CommandRecordMetadata;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.aggregates.WalletsAggregate;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

@Path("/wallets")
public class WalletsServices {

    private final ApplicationServices app;

    public WalletsServices(ApplicationServices app) {
        this.app = app;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean create(Wallets wallet) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                    .addAck("adsgfawghah");

            CommandRecordMetadata metadata = app.persistsCommandTo(WalletsAggregate.baseName()).create(wallet,
                    optionalHeaders, (key, e) -> {
                        if (e != null)
                            e.printStackTrace();
                    });
            result = new ResultsBean(202, "Accepted", "{\"entityId\":\"" + metadata.entityId() + "\"}");

        } catch (Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

    // @GET
    // @Path("/{id}")
    // @Produces(MediaType.APPLICATION_JSON)
    // public ResultsBean getDevice(@PathParam("id") final String id) throws InterruptedException, ExecutionException {
    // ResultsBean result;
    // try {
    //// GenericRecord test = app.<String, GenericRecord>getStore(Application.TEST_QUERY_STORE_BASENAME).findById(id);
    // GenericRecord test2 = app.<String, GenericRecord>getStore(Application.TEST_QUERY_STORE_BASENAME +
    // "2").findById(id);
    //// Devices devices = app.<String, Devices>getStore(DeviceAggregate.baseName()).findById(id);
    // if (/*test != null && */test2 != null) {
    // result = new ResultsBean(202, "Accepted", /*test.toString() + */test2.toString());
    // } else {
    // result = new ResultsBean(404, "Not Found");
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // result = new ResultsBean(500, "Internal Server Error");
    // }
    // return result;
    // }

}
