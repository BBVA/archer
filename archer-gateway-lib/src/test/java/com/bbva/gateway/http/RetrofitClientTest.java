package com.bbva.gateway.http;

import com.bbva.gateway.util.PowermockExtension;
import okhttp3.ResponseBody;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;

import javax.ws.rs.HttpMethod;
import java.util.HashMap;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({Retrofit.class, RetrofitClient.class, Response.class})
@PowerMockIgnore("javax.net.ssl.*")
public class RetrofitClientTest {

    @DisplayName("Create retrofit client ok")
    @Test
    public void createClientOk() {
        final RetrofitClient retrofitClient = new RetrofitClient();
        final Retrofit client = RetrofitClient.build("http://localhost");

        Assertions.assertAll("Retrofit",
                () -> Assertions.assertNotNull(client),
                () -> Assertions.assertNotNull(retrofitClient)
        );
    }

    @DisplayName("Retrofit call to get ok")
    @Test
    public void getOk() throws Exception {
        final Client client = PowerMockito.mock(Client.class);
        final Call<ResponseBody> call = PowerMockito.mock(Call.class);
        final Response responseMock = PowerMockito.mock(Response.class);
        final Retrofit retrofitClient = PowerMockito.mock(Retrofit.class);
        PowerMockito.when(retrofitClient, "create", Mockito.any(Client.class)).thenReturn(client);
        PowerMockito.when(client, "get", Mockito.any(), Mockito.any()).thenReturn(call);
        PowerMockito.when(call, "execute").thenReturn(responseMock);

        final HttpRequest request = new HttpRequest();
        request.setMethod(HttpMethod.GET);
        request.setHeaders(new HashMap<>());
        final Response response = RetrofitClient.call(retrofitClient, request, new HashMap<>());
        Assertions.assertNotNull(response);

        Assertions.assertAll("Retrofit",
                () -> Assertions.assertNotNull(response),
                () -> Assertions.assertEquals(responseMock, response)
        );
    }


    @DisplayName("Retrofit call to post ok")
    @Test
    public void postOk() throws Exception {
        final Client client = PowerMockito.mock(Client.class);
        final Call<ResponseBody> call = PowerMockito.mock(Call.class);
        final Response responseMock = PowerMockito.mock(Response.class);
        final Retrofit retrofitClient = PowerMockito.mock(Retrofit.class);
        PowerMockito.when(retrofitClient, "create", Mockito.any(Client.class)).thenReturn(client);
        PowerMockito.when(client, "post", Mockito.any(), Mockito.any(), Mockito.any()).thenReturn(call);
        PowerMockito.when(call, "execute").thenReturn(responseMock);

        final HttpRequest request = new HttpRequest();
        request.setMethod(HttpMethod.POST);
        request.setHeaders(new HashMap<>());
        final Response response = RetrofitClient.call(retrofitClient, request, new HashMap<>());
        Assertions.assertNotNull(response);

        Assertions.assertAll("Retrofit",
                () -> Assertions.assertNotNull(response),
                () -> Assertions.assertEquals(responseMock, response)
        );
    }

    @DisplayName("Retrofit call to patch ok")
    @Test
    public void patchOk() throws Exception {
        final Client client = PowerMockito.mock(Client.class);
        final Call<ResponseBody> call = PowerMockito.mock(Call.class);
        final Response responseMock = PowerMockito.mock(Response.class);
        final Retrofit retrofitClient = PowerMockito.mock(Retrofit.class);
        PowerMockito.when(retrofitClient, "create", Mockito.any(Client.class)).thenReturn(client);
        PowerMockito.when(client, "post", Mockito.any(), Mockito.any(), Mockito.any()).thenReturn(call);
        PowerMockito.when(call, "execute").thenReturn(responseMock);

        final HttpRequest request = new HttpRequest();
        request.setMethod(HttpMethod.PATCH);
        request.setHeaders(new HashMap<>());
        final Response response = RetrofitClient.call(retrofitClient, request, new HashMap<>());
        Assertions.assertNotNull(response);

        Assertions.assertAll("Retrofit",
                () -> Assertions.assertNotNull(response),
                () -> Assertions.assertEquals(responseMock, response)
        );
    }
}
