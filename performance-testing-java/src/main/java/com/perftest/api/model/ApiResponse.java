package com.perftest.api.model;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;

/**
 * Wrapper around an HTTP response.
 * Captures status code, parsed JSON body, raw body string, latency, and
 * headers.
 * This is the currency of flow chaining — extract values from one ApiResponse
 * and feed them into the next endpoint call.
 */
public class ApiResponse {

    private final int statusCode;
    private final String rawBody;
    private final JsonObject body;
    private final long latencyMs;
    private final Map<String, String> headers;

    public ApiResponse(int statusCode, String rawBody, long latencyMs, Map<String, String> headers) {
        this.statusCode = statusCode;
        this.rawBody = rawBody;
        this.latencyMs = latencyMs;
        this.headers = headers;

        // Try to parse as JSON; null if not valid JSON
        JsonObject parsed = null;
        if (rawBody != null && !rawBody.isEmpty()) {
            try {
                parsed = JsonParser.parseString(rawBody).getAsJsonObject();
            } catch (Exception ignored) {
                // Not JSON or not an object — that's fine
            }
        }
        this.body = parsed;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300;
    }

    public String getRawBody() {
        return rawBody;
    }

    /**
     * Parsed JSON body. May be null if the response was not valid JSON.
     */
    public JsonObject getBody() {
        return body;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Convenience: extract a string value from the JSON body by key.
     * Returns null if the body is null or the key doesn't exist.
     */
    public String getString(String key) {
        if (body != null && body.has(key)) {
            return body.get(key).getAsString();
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("ApiResponse{status=%d, latency=%dms, bodyLength=%d}",
                statusCode, latencyMs, rawBody != null ? rawBody.length() : 0);
    }
}
