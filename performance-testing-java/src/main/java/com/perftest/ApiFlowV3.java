package com.perftest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * API Flow V3 - Same as V2 but with additional CDN resource fetches.
 * 
 * Step 1 fetches:
 * 1. date-picker HTML
 * 2. date-picker-min.css
 * 3. picker-min.js
 * 4. font-awesome CSS
 * 5. font-awesome woff2 font
 * 
 * Step 3 fetches:
 * 1. crossword HTML
 * 2. crossword-player-min.css
 * 3. c-min.js
 */
public class ApiFlowV3 {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
    private static final Random RANDOM = new Random();
    private static final String CHARS = "abcdefghijklmnopqrstuvwxyz";
    private static final Gson GSON = new Gson();

    // CDN resources to fetch after date-picker (Step 1)
    private static final String[] STEP1_CDN_RESOURCES = {
            "https://cdn-test.amuselabs.com/pmm/dd97891/css/date-picker-min.css?v=6aee5d1bf087693e360c8e38dac76fecc9ad81a90abc9ed19cb26a97e1759919",
            "https://cdn-test.amuselabs.com/pmm/dd97891/js/picker-min.js?v=6aee5d1bf087693e360c8e38dac76fecc9ad81a90abc9ed19cb26a97e1759919",
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/css/all.min.css",
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/webfonts/fa-solid-900.woff2"
    };

    // CDN resources to fetch after crossword page (Step 3)
    private static final String[] STEP3_CDN_RESOURCES = {
            "https://cdn-test.amuselabs.com/pmm/dd97891/css/crossword-player-min.css?v=6aee5d1bf087693e360c8e38dac76fecc9ad81a90abc9ed19cb26a97e1759919",
            "https://cdn-test.amuselabs.com/pmm/dd97891/js/c-min.js?v=6aee5d1bf087693e360c8e38dac76fecc9ad81a90abc9ed19cb26a97e1759919"
    };

    private final ApiConfig config;
    private final boolean verbose;
    private final OkHttpClient client;

    // Shared state between steps
    private final Map<String, Object> context = new HashMap<>();

    public ApiFlowV3(ApiConfig config, boolean verbose) {
        this.config = config;
        this.verbose = verbose;
        this.client = new OkHttpClient.Builder()
                .connectTimeout(config.getTimeout(), TimeUnit.SECONDS)
                .readTimeout(config.getTimeout(), TimeUnit.SECONDS)
                .writeTimeout(config.getTimeout(), TimeUnit.SECONDS)
                .build();
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    private String b64Decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private JsonObject b64DecodeJson(String encoded) {
        return JsonParser.parseString(b64Decode(encoded)).getAsJsonObject();
    }

    private JsonObject extractParamsFromHtml(String html) {
        String[] patterns = {
                "<script[^>]*type=\"application/json\"[^>]*id=\"params\"[^>]*>(.*?)</script>",
                "<script[^>]*id=\"params\"[^>]*type=\"application/json\"[^>]*>(.*?)</script>"
        };

        for (String pattern : patterns) {
            Pattern p = Pattern.compile(pattern, Pattern.DOTALL);
            Matcher m = p.matcher(html);
            if (m.find()) {
                return JsonParser.parseString(m.group(1).trim()).getAsJsonObject();
            }
        }

        throw new RuntimeException("couldn't find params script tag in response");
    }

    // =========================================================================
    // STEP 1: Hit the date picker page, grab the loadToken, THEN fetch CDN
    // resources
    // =========================================================================
    public Map<String, Object> step1DatePicker() throws IOException {
        long startTimestamp = System.currentTimeMillis();
        String uid = config.getUid();
        context.put("uid", uid);

        log(String.format("Step 1: GET /date-picker (uid=%s) + CDN resources", uid));

        // 1. Fetch date-picker HTML
        HttpUrl url = HttpUrl.parse(config.getBaseUrl() + "date-picker").newBuilder()
                .addQueryParameter("set", config.getSetParam())
                .addQueryParameter("uid", uid)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        long start = System.nanoTime();
        double datePickerLatencyMs;
        try (Response response = client.newCall(request).execute()) {
            datePickerLatencyMs = (System.nanoTime() - start) / 1_000_000.0;

            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response: " + response.code());
            }

            String body = response.body().string();
            JsonObject paramsJson = extractParamsFromHtml(body);

            String rawsps = paramsJson.has("rawsps") ? paramsJson.get("rawsps").getAsString() : null;
            if (rawsps == null) {
                throw new RuntimeException("no rawsps in response");
            }

            JsonObject decoded = b64DecodeJson(rawsps);
            String loadToken = decoded.has("loadToken") ? decoded.get("loadToken").getAsString() : null;
            if (loadToken == null) {
                throw new RuntimeException("no loadToken in rawsps");
            }

            context.put("load_token", loadToken);
            context.put("params_json", paramsJson);

            log(String.format("  date-picker: %.1fms", datePickerLatencyMs));
        }

        // 2. Fetch CDN resources (after decode)
        List<Map<String, Object>> cdnResults = new ArrayList<>();
        double totalCdnLatencyMs = 0;

        for (String cdnUrl : STEP1_CDN_RESOURCES) {
            Request cdnRequest = new Request.Builder()
                    .url(cdnUrl)
                    .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                    .get()
                    .build();

            long cdnStart = System.nanoTime();
            try (Response cdnResponse = client.newCall(cdnRequest).execute()) {
                double cdnLatencyMs = (System.nanoTime() - cdnStart) / 1_000_000.0;
                totalCdnLatencyMs += cdnLatencyMs;

                // Consume the body to complete the request
                if (cdnResponse.body() != null) {
                    cdnResponse.body().bytes();
                }

                Map<String, Object> cdnResult = new HashMap<>();
                cdnResult.put("url", cdnUrl);
                cdnResult.put("status_code", cdnResponse.code());
                cdnResult.put("latency_ms", cdnLatencyMs);
                cdnResult.put("success", cdnResponse.isSuccessful());
                cdnResults.add(cdnResult);

                String shortUrl = cdnUrl.substring(cdnUrl.lastIndexOf('/') + 1);
                if (shortUrl.contains("?")) {
                    shortUrl = shortUrl.substring(0, shortUrl.indexOf('?'));
                }
                log(String.format("  CDN %s: %.1fms", shortUrl, cdnLatencyMs));

            } catch (IOException e) {
                Map<String, Object> cdnResult = new HashMap<>();
                cdnResult.put("url", cdnUrl);
                cdnResult.put("error", e.getMessage());
                cdnResult.put("success", false);
                cdnResults.add(cdnResult);
                log(String.format("  CDN error: %s", e.getMessage()));
            }
        }

        double totalLatencyMs = datePickerLatencyMs + totalCdnLatencyMs;
        log(String.format("  total: %.1fms (date-picker: %.1fms, CDN: %.1fms)",
                totalLatencyMs, datePickerLatencyMs, totalCdnLatencyMs));

        Map<String, Object> result = new HashMap<>();
        result.put("status_code", 200);
        result.put("uid", uid);
        result.put("latency_ms", totalLatencyMs);
        result.put("date_picker_latency_ms", datePickerLatencyMs);
        result.put("cdn_latency_ms", totalCdnLatencyMs);
        result.put("cdn_results", cdnResults);
        result.put("start_timestamp", startTimestamp);
        result.put("end_timestamp", System.currentTimeMillis());
        return result;
    }

    // =========================================================================
    // STEP 2: Post picker status (same as V2)
    // =========================================================================
    public Map<String, Object> step2PostPickerStatus() throws IOException {
        long startTimestamp = System.currentTimeMillis();
        String loadToken = (String) context.get("load_token");
        if (loadToken == null) {
            throw new RuntimeException("need loadToken from step1");
        }

        log("Step 2: POST /postPickerStatus");

        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", loadToken);
        payload.addProperty("isVerified", true);
        payload.addProperty("adDuration", 0);
        payload.addProperty("reason", "displaying puzzle picker");

        Request request = new Request.Builder()
                .url(config.getBaseUrl() + "postPickerStatus")
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE))
                .build();

        long start = System.nanoTime();
        try (Response response = client.newCall(request).execute()) {
            double latencyMs = (System.nanoTime() - start) / 1_000_000.0;

            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response: " + response.code());
            }

            JsonObject data = JsonParser.parseString(response.body().string()).getAsJsonObject();
            if (!data.has("status") || data.get("status").getAsInt() != 0) {
                throw new RuntimeException("postPickerStatus failed: " + data);
            }

            log(String.format("  done in %.1fms", latencyMs));

            Map<String, Object> result = new HashMap<>();
            result.put("status_code", response.code());
            result.put("latency_ms", latencyMs);
            result.put("start_timestamp", startTimestamp);
            result.put("end_timestamp", System.currentTimeMillis());
            return result;
        }
    }

    // =========================================================================
    // STEP 3: Load the crossword puzzle + CDN resources
    // =========================================================================
    public Map<String, Object> step3LoadCrossword() throws IOException {
        long startTimestamp = System.currentTimeMillis();
        String loadToken = (String) context.get("load_token");
        String uid = (String) context.get("uid");

        if (loadToken == null || uid == null) {
            throw new RuntimeException("need context from step1");
        }

        String puzzleId = ApiConfig.PUZZLE_ID;
        context.put("puzzle_id", puzzleId);

        log(String.format("Step 3: GET /crossword (id=%s) + CDN resources", puzzleId));

        String srcUrl = String.format("%sdate-picker?set=%s&uid=%s",
                config.getBaseUrl(), config.getSetParam(), uid);

        HttpUrl url = HttpUrl.parse(config.getBaseUrl() + "crossword").newBuilder()
                .addQueryParameter("id", puzzleId)
                .addQueryParameter("set", config.getSetParam())
                .addQueryParameter("picker", "date-picker")
                .addQueryParameter("src", srcUrl)
                .addQueryParameter("uid", uid)
                .addQueryParameter("loadToken", loadToken)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        long start = System.nanoTime();
        double crosswordLatencyMs;
        try (Response response = client.newCall(request).execute()) {
            crosswordLatencyMs = (System.nanoTime() - start) / 1_000_000.0;

            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response: " + response.code());
            }

            String body = response.body().string();
            JsonObject crosswordParams = extractParamsFromHtml(body);
            context.put("crossword_params", crosswordParams);

            if (crosswordParams.has("rawp")) {
                JsonObject decodedPlay = b64DecodeJson(crosswordParams.get("rawp").getAsString());
                context.put("decoded_play", decodedPlay);
                if (decodedPlay.has("playId")) {
                    context.put("play_id", decodedPlay.get("playId").getAsString());
                }
            }

            log(String.format("  crossword: %.1fms", crosswordLatencyMs));
        }

        // Fetch CDN resources after crossword page
        List<Map<String, Object>> cdnResults = new ArrayList<>();
        double totalCdnLatencyMs = 0;

        for (String cdnUrl : STEP3_CDN_RESOURCES) {
            Request cdnRequest = new Request.Builder()
                    .url(cdnUrl)
                    .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                    .get()
                    .build();

            long cdnStart = System.nanoTime();
            try (Response cdnResponse = client.newCall(cdnRequest).execute()) {
                double cdnLatencyMs = (System.nanoTime() - cdnStart) / 1_000_000.0;
                totalCdnLatencyMs += cdnLatencyMs;

                if (cdnResponse.body() != null) {
                    cdnResponse.body().bytes();
                }

                Map<String, Object> cdnResult = new HashMap<>();
                cdnResult.put("url", cdnUrl);
                cdnResult.put("status_code", cdnResponse.code());
                cdnResult.put("latency_ms", cdnLatencyMs);
                cdnResult.put("success", cdnResponse.isSuccessful());
                cdnResults.add(cdnResult);

                String shortUrl = cdnUrl.substring(cdnUrl.lastIndexOf('/') + 1);
                if (shortUrl.contains("?")) {
                    shortUrl = shortUrl.substring(0, shortUrl.indexOf('?'));
                }
                log(String.format("  CDN %s: %.1fms", shortUrl, cdnLatencyMs));

            } catch (IOException e) {
                Map<String, Object> cdnResult = new HashMap<>();
                cdnResult.put("url", cdnUrl);
                cdnResult.put("error", e.getMessage());
                cdnResult.put("success", false);
                cdnResults.add(cdnResult);
                log(String.format("  CDN error: %s", e.getMessage()));
            }
        }

        double totalLatencyMs = crosswordLatencyMs + totalCdnLatencyMs;
        log(String.format("  total: %.1fms (crossword: %.1fms, CDN: %.1fms)",
                totalLatencyMs, crosswordLatencyMs, totalCdnLatencyMs));

        Map<String, Object> result = new HashMap<>();
        result.put("status_code", 200);
        result.put("puzzle_id", puzzleId);
        result.put("latency_ms", totalLatencyMs);
        result.put("crossword_latency_ms", crosswordLatencyMs);
        result.put("cdn_latency_ms", totalCdnLatencyMs);
        result.put("cdn_results", cdnResults);
        result.put("start_timestamp", startTimestamp);
        result.put("end_timestamp", System.currentTimeMillis());
        return result;
    }

    // =========================================================================
    // STEP 4: Simulate playing - 10 POST calls (same as V2)
    // =========================================================================
    private String[] generateState(int length, double fillRatio) {
        StringBuilder primary = new StringBuilder(length);
        StringBuilder secondary = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            if (RANDOM.nextDouble() < fillRatio) {
                primary.append(CHARS.charAt(RANDOM.nextInt(CHARS.length())));
                secondary.append('1');
            } else {
                primary.append('#');
                secondary.append('0');
            }
        }

        return new String[] { primary.toString(), secondary.toString() };
    }

    private String[] mutateState(String primary, String secondary) {
        char[] p = primary.toCharArray();
        char[] s = secondary.toCharArray();

        int numChanges = RANDOM.nextInt(Math.min(5, p.length)) + 1;
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < p.length; i++)
            positions.add(i);
        Collections.shuffle(positions);

        for (int i = 0; i < numChanges; i++) {
            int pos = positions.get(i);
            if (p[pos] == '#') {
                p[pos] = CHARS.charAt(RANDOM.nextInt(CHARS.length()));
                s[pos] = '1';
            } else {
                p[pos] = '#';
                s[pos] = '0';
            }
        }

        return new String[] { new String(p), new String(s) };
    }

    private String[] completeState(int length) {
        StringBuilder primary = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            primary.append(CHARS.charAt(RANDOM.nextInt(CHARS.length())));
        }
        StringBuilder secondary = new StringBuilder();
        for (int i = 0; i < length; i++)
            secondary.append('1');
        return new String[] { primary.toString(), secondary.toString() };
    }

    public Map<String, Object> step4PostPlays() throws IOException {
        long startTimestamp = System.currentTimeMillis();
        String loadToken = (String) context.get("load_token");
        String uid = (String) context.get("uid");
        String puzzleId = (String) context.get("puzzle_id");
        JsonObject decodedPlay = (JsonObject) context.get("decoded_play");

        if (loadToken == null || uid == null || puzzleId == null) {
            throw new RuntimeException("missing context from earlier steps");
        }

        String playId = context.containsKey("play_id") ? (String) context.get("play_id") : "";
        int stateLen = ApiConfig.STATE_LEN;

        log(String.format("Step 4: POST /api/v1/plays (10 iterations, state_len=%d) [HARDCODED]", stateLen));

        String[] state = generateState(stateLen, 0.1);
        String primary = state[0];
        String secondary = state[1];

        List<Map<String, Object>> results = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            long ts = System.currentTimeMillis();

            int playState;
            String currPrimary = null;
            String currSecondary = null;

            if (i == 0) {
                playState = 1;
            } else if (i == 9) {
                playState = 4;
                String[] completed = completeState(stateLen);
                currPrimary = completed[0];
                currSecondary = completed[1];
            } else {
                playState = 2;
                String[] mutated = mutateState(primary, secondary);
                primary = mutated[0];
                secondary = mutated[1];
                currPrimary = primary;
                currSecondary = secondary;
            }

            JsonObject payload = new JsonObject();
            payload.addProperty("browser",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
            payload.addProperty("fromPicker", "date-picker");
            payload.addProperty("getProgressFromBackend", true);
            payload.addProperty("id", puzzleId);
            payload.addProperty("inContestMode", false);
            payload.addProperty("loadToken", loadToken);
            payload.addProperty("nClearClicks", 0);
            payload.addProperty("nExceptions", 0);
            payload.addProperty("nHelpClicks", 0);
            payload.addProperty("nPrints", 0);
            payload.addProperty("nPrintsEmpty", 0);
            payload.addProperty("nPrintsFilled", 0);
            payload.addProperty("nPrintsSol", 0);
            payload.addProperty("nResizes", 0);
            payload.addProperty("nSettingsClicks", 0);
            payload.addProperty("playId", playId);
            payload.addProperty("playState", playState);
            payload.addProperty("postScoreReason", "BLUR");
            if (currPrimary != null) {
                payload.addProperty("primaryState", currPrimary);
            }
            payload.addProperty("score",
                    decodedPlay != null && decodedPlay.has("score") ? decodedPlay.get("score").getAsInt() : 0);
            if (currSecondary != null) {
                payload.addProperty("secondaryState", currSecondary);
            }
            payload.addProperty("series", config.getSetParam());
            payload.addProperty("streakLength", 0);
            payload.addProperty("timeOnPage",
                    decodedPlay != null && decodedPlay.has("timeOnPage") ? decodedPlay.get("timeOnPage").getAsInt()
                            : 5000);
            payload.addProperty("timeTaken",
                    decodedPlay != null && decodedPlay.has("timeTaken") ? decodedPlay.get("timeTaken").getAsInt() : 5);
            payload.addProperty("timestamp", ts);
            payload.addProperty("updateLoadTable", false);
            payload.addProperty("updatePlayTable", true);
            payload.addProperty("updatedTimestamp", ts);
            payload.addProperty("userId", uid);

            Request request = new Request.Builder()
                    .url(config.getBaseUrl() + "api/v1/plays")
                    .addHeader("Content-Type", "application/json")
                    .post(RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE))
                    .build();

            long start = System.nanoTime();
            try (Response response = client.newCall(request).execute()) {
                double latencyMs = (System.nanoTime() - start) / 1_000_000.0;

                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected response: " + response.code());
                }

                JsonObject data = JsonParser.parseString(response.body().string()).getAsJsonObject();
                if (!data.has("status") || data.get("status").getAsInt() != 0) {
                    throw new RuntimeException("plays API failed at iteration " + (i + 1) + ": " + data);
                }

                log(String.format("  [%d/10] playState=%d - %.1fms", i + 1, playState, latencyMs));

                Map<String, Object> iterResult = new HashMap<>();
                iterResult.put("iteration", i + 1);
                iterResult.put("play_state", playState);
                iterResult.put("latency_ms", latencyMs);
                results.add(iterResult);
            }
        }

        log("  all done");

        Map<String, Object> result = new HashMap<>();
        result.put("iterations", results);
        result.put("success", true);
        result.put("start_timestamp", startTimestamp);
        result.put("end_timestamp", System.currentTimeMillis());
        return result;
    }

    // =========================================================================
    // Main flow
    // =========================================================================
    public Map<String, Object> runSequentialFlow() {
        Map<String, Object> results = new HashMap<>();

        try {
            results.put("step1", step1DatePicker());
            results.put("step2", step2PostPickerStatus());
            results.put("step3", step3LoadCrossword());
            results.put("step4", step4PostPlays());
            results.put("success", true);
        } catch (IOException e) {
            log("request error: " + e.getMessage());
            results.put("success", false);
            results.put("error", e.getMessage());
        } catch (RuntimeException e) {
            log("parse error: " + e.getMessage());
            results.put("success", false);
            results.put("error", e.getMessage());
        }

        return results;
    }

    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }
}
