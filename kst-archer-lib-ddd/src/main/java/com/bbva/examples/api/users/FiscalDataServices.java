package com.bbva.examples.api.users;

import com.bbva.avro.Users;
import com.bbva.avro.users.FiscalData;
import com.bbva.common.utils.OptionalRecordHeaders;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.commands.write.CommandRecordMetadata;
import com.bbva.examples.Application;
import com.bbva.examples.MainHandler;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.aggregates.UserAggregate;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.bbva.dataprocessors.ReadableStore;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

@Path("/users")
public class FiscalDataServices {

    private final ApplicationServices app;

    public FiscalDataServices(ApplicationServices app) {
        this.app = app;
    }

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean createUser(FiscalData fiscalData) throws InterruptedException, ExecutionException {
        ResultsBean result;
        ReadableStore<String, String> emailStore = null;

        try {
            emailStore = app.getStore(Application.EMAIL_STORE_BASENAME);

        } catch (NullPointerException e) {
        }

        try {
            if (emailStore == null || !emailStore.exists(fiscalData.getEmail())) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                CommandRecordMetadata metadata = app.persistsCommandTo(FiscalDataAggregate.baseName())
                        .create(fiscalData, optionalHeaders, (key, e) -> {
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
    public ResultsBean updateUser(@PathParam("id") final String id, FiscalData fiscalData)
            throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Users> getStore(UserAggregate.baseName()).exists(id)) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                app.persistsCommandTo(FiscalDataAggregate.baseName()).processAction(MainHandler.ADD_FISCAL_DATA_ACTION,
                        id, fiscalData, optionalHeaders, (key, e) -> {
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
    public ResultsBean getUser(@PathParam("id") final String id) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            Users user = app.<String, Users> getStore(UserAggregate.baseName()).findById(id);
            if (user != null) {
                result = new ResultsBean(200, "Accepted", user.toString());
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
    public ResultsBean deleteUser(@PathParam("id") final String id) throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Users> getStore(UserAggregate.baseName()).exists(id)) {
                OptionalRecordHeaders optionalHeaders = new OptionalRecordHeaders().addOrigin("aegewy445y")
                        .addAck("adsgfawghah");

                app.persistsCommandTo(UserAggregate.baseName()).delete(id, Users.class, optionalHeaders, (key, e) -> {
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
