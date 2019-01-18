package com.bbva.examples.api.wallets;

import com.bbva.avro.Wallets;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.ddd.HelperDomain;
import com.bbva.ddd.domain.commands.write.CommandRecordMetadata;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.aggregates.WalletsAggregate;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/wallets")
public class WalletsServices {

    private final HelperDomain app;

    public WalletsServices(final HelperDomain app) {
        this.app = app;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean create(final Wallets wallet) {
        ResultsBean result;
        try {
            final OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                    .addAck("adsgfawghah");

            final CommandRecordMetadata metadata = app.persistsCommandTo(WalletsAggregate.baseName()).create(wallet,
                    optionalHeaders, (key, e) -> {
                        if (e != null) {
                            e.printStackTrace();
                        }
                    });
            result = new ResultsBean(202, "Accepted", "{\"entityId\":\"" + metadata.entityId() + "\"}");

        } catch (final Exception e) {
            e.printStackTrace();
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

}
