package com.bbva.gateway.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RetrofitClient {

    private static final LoggerGen logger = LoggerGenesis.getLogger(RetrofitClient.class.getName());

    public static Retrofit build(final String url) {
        final Gson gson = new GsonBuilder().setLenient().create();

        final OkHttpClient okHttpClient = new OkHttpClient.Builder().retryOnConnectionFailure(true).connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS).build();

        return new Retrofit.Builder().baseUrl(url).client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson)).build();
    }

    public static Response call(final Retrofit retrofitClient, final HttpRequest request, Map<String, String> queryParams) {
        final Client client = retrofitClient.create(Client.class);
        final Call<ResponseBody> call;
        queryParams = queryParams != null ? queryParams : new HashMap<>();
        switch (request.getMethod()) {
            case HttpMethod.GET:
                call = client.get(request.getHeaders(), queryParams);
                break;
            case HttpMethod.POST:
                call = client.post(request.getBody(), request.getHeaders(), queryParams);
                break;
            default:
                call = client.post(request.getBody(), request.getHeaders(), queryParams);
                break;
        }

        try {
            return call.execute();
        } catch (final IOException e) {
            logger.error("Cannot connect with the external endpoint", e);
        }
        return null;
    }
}
