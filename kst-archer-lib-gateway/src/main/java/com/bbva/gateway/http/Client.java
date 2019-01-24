package com.bbva.gateway.http;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.Map;

public interface Client {

    @POST("./")
    Call<ResponseBody> post(@Body Object body, @HeaderMap Map<String, String> headers, @QueryMap Map<String, String> queryParams);

    @GET("./")
    Call<ResponseBody> get(@HeaderMap Map<String, String> headers, @QueryMap Map<String, String> queryParams);
}
