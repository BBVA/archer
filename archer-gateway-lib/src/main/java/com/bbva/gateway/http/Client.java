package com.bbva.gateway.http;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.Map;

/**
 * Http client for calls
 */
public interface Client {

    /**
     * POST method
     *
     * @param body        body object
     * @param headers     headers
     * @param queryParams query params
     * @return callable
     */
    @POST("./")
    Call<ResponseBody> post(@Body Object body, @HeaderMap Map<String, String> headers, @QueryMap Map<String, String> queryParams);

    /**
     * GET method
     *
     * @param headers     headers
     * @param queryParams query params
     * @return callable
     */
    @GET("./")
    Call<ResponseBody> get(@HeaderMap Map<String, String> headers, @QueryMap Map<String, String> queryParams);
}
