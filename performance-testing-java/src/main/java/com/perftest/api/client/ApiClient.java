package com.perftest.api.client;

import com.perftest.api.auth.TokenManager;
import com.perftest.api.config.ApiTestConfig;
import com.perftest.api.model.ApiResponse;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared HTTP client for API endpoint testing.
 * Wraps OkHttp with:
 * - Automatic Bearer token attachment via TokenManager
 * - GET and POST helper methods
 * - Auto-retry on 401 with token refresh
 * - Latency measurement
 * - Response parsing into ApiResponse
 */
public class ApiClient implements AutoCloseable {

    private static final Gson GSON = new Gson();
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final ApiTestConfig config;
    private final OkHttpClient httpClient;
    private final TokenManager tokenManager;

    /**
     * Create an ApiClient with the given configuration.
     * Initialises OkHttp client and fetches an initial auth token.
     */
    public ApiClient(ApiTestConfig config) throws IOException {
        this.config = config;

        // Build OkHttp client
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(200);
        dispatcher.setMaxRequestsPerHost(config.getMaxConnections());

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getTimeoutSeconds(), TimeUnit.SECONDS)
                .readTimeout(config.getTimeoutSeconds(), TimeUnit.SECONDS)
                .writeTimeout(config.getTimeoutSeconds(), TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(config.getMaxConnections(), 5, TimeUnit.MINUTES))
                .dispatcher(dispatcher)
                .build();

        // Initialise token manager
        this.tokenManager = new TokenManager(
                config.getBaseUrl(),
                "api/v1/token",
                config.getAuthConfigPath(),
                httpClient,
                config.isVerbose());
    }

    /**
     * Execute an authenticated GET request.
     *
     * @param path        Relative path (e.g., "api/v1/puzzles")
     * @param queryParams Query parameters (may be null or empty)
     * @return ApiResponse with status, body, latency
     */
    public ApiResponse get(String path, Map<String, String> queryParams) throws IOException {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(config.getBaseUrl() + path).newBuilder();

        if (queryParams != null) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (entry.getValue() != null) {
                    urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
                }
            }
        }

        HttpUrl url = urlBuilder.build();

        for (int attempt = 0; attempt < 2; attempt++) {
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "Bearer " + tokenManager.getAccessToken())
                    .get()
                    .build();

            long startMs = System.currentTimeMillis();
            try (Response response = httpClient.newCall(request).execute()) {
                long latencyMs = System.currentTimeMillis() - startMs;

                if (response.code() == 401 && attempt == 0) {
                    if (tokenManager.handleUnauthorized()) {
                        log("Token refreshed after 401, retrying GET " + path);
                        continue;
                    }
                }

                return toApiResponse(response, latencyMs);
            }
        }
        throw new IOException("GET " + path + " failed after retry");
    }

    /**
     * Convenience: GET with no query params.
     */
    public ApiResponse get(String path) throws IOException {
        return get(path, null);
    }

    /**
     * Execute an authenticated POST request with a JSON body.
     *
     * @param path    Relative path (e.g., "api/v1/plays")
     * @param payload JSON object to send as request body
     * @return ApiResponse with status, body, latency
     */
    public ApiResponse post(String path, JsonObject payload) throws IOException {
        String url = config.getBaseUrl() + path;
        RequestBody body = RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE);

        for (int attempt = 0; attempt < 2; attempt++) {
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Content-Type", "application/json")
                    .addHeader("Authorization", "Bearer " + tokenManager.getAccessToken())
                    .post(body)
                    .build();

            long startMs = System.currentTimeMillis();
            try (Response response = httpClient.newCall(request).execute()) {
                long latencyMs = System.currentTimeMillis() - startMs;

                if (response.code() == 401 && attempt == 0) {
                    if (tokenManager.handleUnauthorized()) {
                        log("Token refreshed after 401, retrying POST " + path);
                        continue;
                    }
                }

                return toApiResponse(response, latencyMs);
            }
        }
        throw new IOException("POST " + path + " failed after retry");
    }

    /**
     * Execute an authenticated POST request with query parameters and a JSON body.
     *
     * @param path        Relative path
     * @param queryParams Query parameters (may be null)
     * @param payload     JSON body (may be null for empty body)
     * @return ApiResponse
     */
    public ApiResponse post(String path, Map<String, String> queryParams, JsonObject payload) throws IOException {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(config.getBaseUrl() + path).newBuilder();

        if (queryParams != null) {
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (entry.getValue() != null) {
                    urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
                }
            }
        }

        String url = urlBuilder.build().toString();
        RequestBody body = payload != null
                ? RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE)
                : RequestBody.create("", JSON_MEDIA_TYPE);

        for (int attempt = 0; attempt < 2; attempt++) {
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Content-Type", "application/json")
                    .addHeader("Authorization", "Bearer " + tokenManager.getAccessToken())
                    .post(body)
                    .build();

            long startMs = System.currentTimeMillis();
            try (Response response = httpClient.newCall(request).execute()) {
                long latencyMs = System.currentTimeMillis() - startMs;

                if (response.code() == 401 && attempt == 0) {
                    if (tokenManager.handleUnauthorized()) {
                        continue;
                    }
                }

                return toApiResponse(response, latencyMs);
            }
        }
        throw new IOException("POST " + path + " failed after retry");
    }

    /**
     * Convert an OkHttp Response into an ApiResponse.
     */
    private ApiResponse toApiResponse(Response response, long latencyMs) throws IOException {
        String rawBody = response.body() != null ? response.body().string() : "";

        Map<String, String> headers = new HashMap<>();
        for (String name : response.headers().names()) {
            headers.put(name, response.header(name));
        }

        return new ApiResponse(response.code(), rawBody, latencyMs, headers);
    }

    /**
     * Get the underlying config for endpoint classes to reference.
     */
    public ApiTestConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private void log(String msg) {
        if (config.isVerbose()) {
            System.out.println("[ApiClient] " + msg);
        }
    }
}
