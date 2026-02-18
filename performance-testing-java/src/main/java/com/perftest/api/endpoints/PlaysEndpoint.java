package com.perftest.api.endpoints;

import com.perftest.api.client.ApiClient;
import com.perftest.api.model.ApiResponse;

import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * /api/v1/plays endpoint.
 *
 * GET /api/v1/plays — List play records for a series.
 * POST /api/v1/plays — Submit play data.
 *
 * GET query parameters:
 * series (required) — Puzzle series identifier (e.g., "gandalf")
 * limit (optional) — Max results to return (default: 14)
 * offset (optional) — Pagination offset (default: 0)
 * puzzleIds (optional) — Comma-separated puzzle IDs to filter by
 *
 * POST body fields (all in JSON):
 * loadToken, updatePlayTable, updateLoadTable, series, id (puzzleId),
 * playId, userId, playState, timeTaken, score, primaryState, secondaryState,
 * ...
 */
public class PlaysEndpoint {

    private static final String PATH = "api/v1/plays";

    // GET parameters
    private String series;
    private int limit = 14;
    private int offset = 0;
    private String puzzleIds; // Comma-separated, optional

    public PlaysEndpoint(String series) {
        this.series = series;
    }

    public PlaysEndpoint series(String series) {
        this.series = series;
        return this;
    }

    public PlaysEndpoint limit(int limit) {
        this.limit = limit;
        return this;
    }

    public PlaysEndpoint offset(int offset) {
        this.offset = offset;
        return this;
    }

    public PlaysEndpoint puzzleIds(String puzzleIds) {
        this.puzzleIds = puzzleIds;
        return this;
    }

    /**
     * GET /api/v1/plays?series=X&limit=N&offset=N[&puzzleIds=...]
     */
    public ApiResponse get(ApiClient client) throws IOException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("series", series);
        params.put("limit", String.valueOf(limit));
        params.put("offset", String.valueOf(offset));
        if (puzzleIds != null && !puzzleIds.isEmpty()) {
            params.put("puzzleIds", puzzleIds);
        }
        return client.get(PATH, params);
    }

    /**
     * POST /api/v1/plays with a complete payload.
     *
     * @param payload Full JSON body to send. At minimum should include:
     *                loadToken, series, id (puzzleId), userId, playState.
     */
    public ApiResponse post(ApiClient client, JsonObject payload) throws IOException {
        return client.post(PATH, payload);
    }

    /**
     * Build a standard play POST payload with required fields.
     * Caller can add more fields to the returned object before posting.
     *
     * @param loadToken Session load token from crossword page
     * @param puzzleId  Puzzle ID
     * @param userId    User ID
     * @param series    Series name
     * @param playId    Play ID (from crossword session, can be empty)
     * @return JsonObject payload ready for post()
     */
    public static JsonObject buildPayload(String loadToken, String puzzleId, String userId,
            String series, String playId) {
        long ts = System.currentTimeMillis();
        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", loadToken);
        payload.addProperty("updatePlayTable", true);
        payload.addProperty("updateLoadTable", false);
        payload.addProperty("series", series);
        payload.addProperty("id", puzzleId);
        payload.addProperty("playId", playId != null ? playId : "");
        payload.addProperty("userId", userId);
        payload.addProperty("browser", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36");
        payload.addProperty("streakLength", 0);
        payload.addProperty("getProgressFromBackend", true);
        payload.addProperty("fromPicker", "date-picker");
        payload.addProperty("inContestMode", false);
        payload.addProperty("timestamp", ts);
        payload.addProperty("updatedTimestamp", ts);
        payload.addProperty("playState", 2);
        payload.addProperty("timeTaken", 10);
        payload.addProperty("score", 0);
        payload.addProperty("timeOnPage", 5000);
        payload.addProperty("nPrints", 0);
        payload.addProperty("nPrintsEmpty", 0);
        payload.addProperty("nPrintsFilled", 0);
        payload.addProperty("nPrintsSol", 0);
        payload.addProperty("nClearClicks", 0);
        payload.addProperty("nSettingsClicks", 0);
        payload.addProperty("nHelpClicks", 0);
        payload.addProperty("nResizes", 0);
        payload.addProperty("nExceptions", 0);
        payload.addProperty("postScoreReason", "AUTOSAVE");
        payload.addProperty("primaryState", "000000000000000");
        payload.addProperty("secondaryState", "000000000000000");
        return payload;
    }

    /**
     * Describe this endpoint's current GET configuration.
     */
    public String describe() {
        String desc = String.format("GET /%s?series=%s&limit=%d&offset=%d", PATH, series, limit, offset);
        if (puzzleIds != null && !puzzleIds.isEmpty()) {
            desc += "&puzzleIds=" + puzzleIds;
        }
        return desc;
    }
}
