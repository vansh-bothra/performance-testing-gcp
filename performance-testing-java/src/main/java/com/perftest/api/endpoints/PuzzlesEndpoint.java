package com.perftest.api.endpoints;

import com.perftest.api.client.ApiClient;
import com.perftest.api.model.ApiResponse;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * /api/v1/puzzles endpoint.
 *
 * GET /api/v1/puzzles — List puzzles for a series.
 *
 * Query parameters:
 * series (required) — Puzzle series identifier (e.g., "gandalf")
 * limit (optional) — Max results to return (default: 14)
 * offset (optional) — Pagination offset (default: 0)
 */
public class PuzzlesEndpoint {

    private static final String PATH = "api/v1/puzzles";

    private String series;
    private int limit = 14;
    private int offset = 0;

    public PuzzlesEndpoint(String series) {
        this.series = series;
    }

    public PuzzlesEndpoint series(String series) {
        this.series = series;
        return this;
    }

    public PuzzlesEndpoint limit(int limit) {
        this.limit = limit;
        return this;
    }

    public PuzzlesEndpoint offset(int offset) {
        this.offset = offset;
        return this;
    }

    /**
     * GET /api/v1/puzzles?series=X&limit=N&offset=N
     */
    public ApiResponse get(ApiClient client) throws IOException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("series", series);
        params.put("limit", String.valueOf(limit));
        params.put("offset", String.valueOf(offset));
        return client.get(PATH, params);
    }

    /**
     * Describe this endpoint's current configuration.
     */
    public String describe() {
        return String.format("GET /%s?series=%s&limit=%d&offset=%d", PATH, series, limit, offset);
    }
}
