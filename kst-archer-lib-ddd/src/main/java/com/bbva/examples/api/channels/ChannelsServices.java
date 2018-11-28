package com.bbva.examples.api.channels;

import com.bbva.avro.Channels;
import com.bbva.avro.Devices;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.commands.write.CommandRecordMetadata;
import com.bbva.examples.MainHandler;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.aggregates.ChannelsAggregate;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

@Path("/channels")
public class ChannelsServices {

    private final ApplicationServices app;

    public ChannelsServices(ApplicationServices app) {
        this.app = app;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean create(Channels channels) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                    .addAck("adsgfawghah");

            CommandRecordMetadata metadata = app.persistsCommandTo(ChannelsAggregate.baseName()).create(channels,
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

    @POST
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean update(@PathParam("id") final String id, Channels channels)
            throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Devices> getStore(ChannelsAggregate.baseName()).exists(id)) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                app.persistsCommandTo(ChannelsAggregate.baseName()).processAction(MainHandler.UPDATE_CHANNEL_ACTION, id,
                        channels, optionalHeaders, (key, e) -> {
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
