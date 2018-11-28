package com.bbva.examples.api.users;

import com.bbva.avro.Users;
import com.bbva.avro.users.Settings;
import com.bbva.ddd.ApplicationServices;
import com.bbva.examples.ResultsBean;
import com.bbva.examples.MainHandler;
import com.bbva.examples.aggregates.user.SettingsAggregate;
import com.bbva.examples.aggregates.UserAggregate;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

@Path("/users")
public class SettingsServices {

    private final ApplicationServices app;

    public SettingsServices(ApplicationServices app) {
        this.app = app;
    }

    @POST
    @Path("/{id}/settings")
    @Produces(MediaType.APPLICATION_JSON)
    public ResultsBean createUser(@PathParam("id") final String id, Settings settings)
            throws InterruptedException, ExecutionException {
        ResultsBean result;
        try {
            if (app.<String, Users> getStore(UserAggregate.baseName()).exists(id)) {
                app.sendCommandTo(SettingsAggregate.baseName()).processAction(MainHandler.ADD_SETTINGS_ACTION, id,
                        settings, (key, e) -> {
                            if (e != null)
                                e.printStackTrace();
                        });
                result = new ResultsBean(202, "Accepted");
            } else {
                result = new ResultsBean(404, "Not Found");
            }
        } catch (Exception e) {
            result = new ResultsBean(500, "Internal Server Error");
        }
        return result;
    }

}
