package com.perftest.api.auth;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OAuth2 token lifecycle manager.
 * Handles:
 * - Loading client credentials from auth_config.json
 * - POST /api/v1/token to obtain access_token
 * - 55-minute validity with automatic thread-safe refresh
 * - Force refresh on 401 responses
 *
 * Extracted from PplmagReplayExecutor's auth logic so it can be
 * reused across all API endpoint tests.
 */
public class TokenManager {

    private final String baseUrl;
    private final String authEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final OkHttpClient httpClient;
    private final boolean verbose;

    private volatile String accessToken;
    private volatile long tokenExpiryTime; // System.currentTimeMillis() when token expires
    private final ReentrantLock tokenRefreshLock = new ReentrantLock();

    /** Token is refreshed 5 minutes before actual 1-hour expiry. */
    private static final long TOKEN_VALIDITY_MS = 55 * 60 * 1000;

    /**
     * Create a TokenManager by loading credentials from a JSON config file.
     *
     * @param baseUrl        API base URL (e.g.,
     *                       "https://cdn-test.amuselabs.com/pmm/")
     * @param authEndpoint   Token endpoint path (e.g., "api/v1/token")
     * @param configFilePath Path to JSON file with client_id and client_secret
     * @param httpClient     OkHttp client instance to reuse
     * @param verbose        Whether to print log messages
     */
    public TokenManager(String baseUrl, String authEndpoint, String configFilePath,
            OkHttpClient httpClient, boolean verbose) throws IOException {
        this.baseUrl = baseUrl;
        this.authEndpoint = authEndpoint;
        this.httpClient = httpClient;
        this.verbose = verbose;

        // Load credentials
        JsonObject config = loadAuthConfig(configFilePath);
        this.clientId = config.get("client_id").getAsString();
        this.clientSecret = config.get("client_secret").getAsString();

        // Fetch initial token
        refreshToken();
    }

    /**
     * Load auth configuration from a JSON file.
     * Expected format: {"client_id": "...", "client_secret": "..."}
     */
    private JsonObject loadAuthConfig(String configFilePath) throws IOException {
        File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            throw new IOException("Auth config file not found: " + configFilePath +
                    "\nExpected format: {\"client_id\": \"...\", \"client_secret\": \"...\"}");
        }
        String content = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
        return JsonParser.parseString(content).getAsJsonObject();
    }

    /**
     * Get the current access token, refreshing if expired or missing.
     * Thread-safe.
     */
    public String getAccessToken() throws IOException {
        if (accessToken == null || System.currentTimeMillis() >= tokenExpiryTime) {
            refreshToken();
        }
        return accessToken;
    }

    /**
     * Force-refresh the token. Called after a 401 response.
     * Thread-safe: only one thread refreshes at a time; others get the new token.
     */
    public void refreshToken() throws IOException {
        tokenRefreshLock.lock();
        try {
            // Double-check: another thread may have already refreshed
            if (accessToken != null && System.currentTimeMillis() < tokenExpiryTime) {
                return;
            }

            log("Fetching new access token...");

            RequestBody formBody = new FormBody.Builder()
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret)
                    .build();

            Request request = new Request.Builder()
                    .url(baseUrl + authEndpoint)
                    .addHeader("Accept", "application/json")
                    .post(formBody)
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("Failed to get access token: HTTP " + response.code());
                }

                String body = response.body().string();
                JsonObject json = JsonParser.parseString(body).getAsJsonObject();

                if (!json.has("access_token")) {
                    throw new IOException("No access_token in response: " + body);
                }

                this.accessToken = json.get("access_token").getAsString();
                this.tokenExpiryTime = System.currentTimeMillis() + TOKEN_VALIDITY_MS;

                log("Access token obtained, valid for 55 minutes");
            }
        } finally {
            tokenRefreshLock.unlock();
        }
    }

    /**
     * Invalidate the current token and refresh.
     * Call this when a 401 is received.
     *
     * @return true if a new token was obtained successfully
     */
    public boolean handleUnauthorized() {
        try {
            tokenRefreshLock.lock();
            try {
                tokenExpiryTime = 0; // Force expiry
            } finally {
                tokenRefreshLock.unlock();
            }
            refreshToken();
            return true;
        } catch (IOException e) {
            log("Failed to refresh token after 401: " + e.getMessage());
            return false;
        }
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[TokenManager] " + msg);
        }
    }
}
