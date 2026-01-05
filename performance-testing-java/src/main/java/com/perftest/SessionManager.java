package com.perftest;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

/**
 * Session Manager - Handles dynamic fetching of loadToken and playId for
 * replay.
 * 
 * When replaying traffic, the logged loadToken and playId are stale. This class
 * fetches fresh tokens by hitting the date-picker and crossword endpoints,
 * similar to how ApiFlow.step1DatePicker and step3LoadCrossword work.
 */
public class SessionManager {

    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36";

    // Cache: "userId|puzzleId" -> SessionTokens
    private final ConcurrentHashMap<String, SessionTokens> tokenCache = new ConcurrentHashMap<>();

    // Pending initializations to avoid duplicate calls
    private final ConcurrentHashMap<String, CompletableFuture<SessionTokens>> pendingInits = new ConcurrentHashMap<>();

    private final OkHttpClient client;
    private final String baseUrl;
    private final boolean verbose;

    public static class SessionTokens {
        public final String loadToken;
        public final String playId;
        public final String error;

        public SessionTokens(String loadToken, String playId) {
            this.loadToken = loadToken;
            this.playId = playId;
            this.error = null;
        }

        public SessionTokens(String error) {
            this.loadToken = null;
            this.playId = null;
            this.error = error;
        }

        public boolean isValid() {
            return loadToken != null && playId != null;
        }
    }

    public SessionManager(OkHttpClient client, String baseUrl, boolean verbose) {
        this.client = client;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.verbose = verbose;
    }

    /**
     * Get or create a session for the given userId and puzzleId.
     * This method is thread-safe and deduplicates concurrent requests.
     */
    public SessionTokens getOrCreateSession(String userId, String puzzleId, String series) {
        String cacheKey = userId + "|" + puzzleId;

        // Check cache first
        SessionTokens cached = tokenCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        // Use CompletableFuture to deduplicate concurrent init calls
        CompletableFuture<SessionTokens> future = pendingInits.computeIfAbsent(cacheKey, k -> {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return initializeSession(userId, puzzleId, series);
                } catch (Exception e) {
                    return new SessionTokens("Init failed: " + e.getMessage());
                }
            });
        });

        try {
            SessionTokens tokens = future.get(30, TimeUnit.SECONDS);
            tokenCache.put(cacheKey, tokens);
            pendingInits.remove(cacheKey);
            return tokens;
        } catch (Exception e) {
            pendingInits.remove(cacheKey);
            return new SessionTokens("Timeout: " + e.getMessage());
        }
    }

    /**
     * Pre-initialize sessions for a list of unique (userId, puzzleId, series)
     * tuples.
     * This is called before replay starts to warm up the cache.
     */
    public void initializeSessions(List<String[]> sessions, int parallelism) {
        if (sessions.isEmpty())
            return;

        log(String.format("[SessionManager] Initializing %d sessions (parallelism=%d)...",
                sessions.size(), parallelism));

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        List<Future<?>> futures = new ArrayList<>();

        for (String[] session : sessions) {
            String userId = session[0];
            String puzzleId = session[1];
            String series = session[2];

            futures.add(executor.submit(() -> {
                getOrCreateSession(userId, puzzleId, series);
            }));
        }

        // Wait for all to complete
        int completed = 0;
        for (Future<?> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
                completed++;
                if (completed % 10 == 0 || completed == sessions.size()) {
                    log(String.format("[SessionManager] Progress: %d/%d sessions initialized",
                            completed, sessions.size()));
                }
            } catch (Exception e) {
                log("[SessionManager] Session init failed: " + e.getMessage());
            }
        }

        executor.shutdown();

        // Report stats
        int valid = 0;
        for (SessionTokens t : tokenCache.values()) {
            if (t.isValid())
                valid++;
        }
        log(String.format("[SessionManager] Done. %d/%d sessions valid.", valid, sessions.size()));
    }

    /**
     * Initialize a single session by hitting date-picker and crossword endpoints.
     */
    private SessionTokens initializeSession(String userId, String puzzleId, String series)
            throws IOException {

        // Step 1: Get loadToken from date-picker
        String loadToken = fetchLoadToken(userId, series);
        if (loadToken == null) {
            return new SessionTokens("Failed to get loadToken");
        }

        // Step 2: Get playId from crossword page
        String playId = fetchPlayId(userId, puzzleId, loadToken, series);
        if (playId == null) {
            return new SessionTokens("Failed to get playId");
        }

        return new SessionTokens(loadToken, playId);
    }

    /**
     * Fetch loadToken by hitting the date-picker endpoint.
     */
    private String fetchLoadToken(String userId, String series) throws IOException {
        HttpUrl url = HttpUrl.parse(baseUrl + "date-picker").newBuilder()
                .addQueryParameter("set", series)
                .addQueryParameter("uid", userId)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html")
                .addHeader("User-Agent", USER_AGENT)
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                log("[SessionManager] date-picker failed: HTTP " + response.code());
                return null;
            }

            String body = response.body().string();
            JsonObject paramsJson = extractParamsFromHtml(body);
            if (paramsJson == null) {
                // Check if it's an error page
                if (body.contains("PuzzleMe Error") || body.contains("error-min.js")) {
                    log("[SessionManager] date-picker returned error page for series=" + series);
                } else {
                    log("[SessionManager] date-picker: no params script tag found");
                }
                return null;
            }

            String rawsps = paramsJson.has("rawsps") ? paramsJson.get("rawsps").getAsString() : null;
            if (rawsps == null) {
                log("[SessionManager] date-picker: no rawsps in params");
                return null;
            }

            JsonObject decoded = b64DecodeJson(rawsps);
            if (decoded == null) {
                log("[SessionManager] date-picker: failed to decode rawsps");
                return null;
            }
            return decoded.has("loadToken") ? decoded.get("loadToken").getAsString() : null;
        }
    }

    /**
     * Fetch playId by hitting the crossword endpoint.
     */
    private String fetchPlayId(String userId, String puzzleId, String loadToken, String series)
            throws IOException {

        String srcUrl = String.format("%sdate-picker?set=%s&uid=%s", baseUrl, series, userId);

        HttpUrl url = HttpUrl.parse(baseUrl + "crossword").newBuilder()
                .addQueryParameter("id", puzzleId)
                .addQueryParameter("set", series)
                .addQueryParameter("picker", "date-picker")
                .addQueryParameter("src", srcUrl)
                .addQueryParameter("uid", userId)
                .addQueryParameter("loadToken", loadToken)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html")
                .addHeader("User-Agent", USER_AGENT)
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                log("[SessionManager] crossword failed: HTTP " + response.code());
                return null;
            }

            String body = response.body().string();
            JsonObject paramsJson = extractParamsFromHtml(body);
            if (paramsJson == null)
                return null;

            if (!paramsJson.has("rawp"))
                return null;

            JsonObject decoded = b64DecodeJson(paramsJson.get("rawp").getAsString());
            return decoded.has("playId") ? decoded.get("playId").getAsString() : null;
        }
    }

    // =========================================================================
    // Utility methods (copied from ApiFlow)
    // =========================================================================

    private String b64Decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private JsonObject b64DecodeJson(String encoded) {
        try {
            return JsonParser.parseString(b64Decode(encoded)).getAsJsonObject();
        } catch (Exception e) {
            return null;
        }
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
                try {
                    return JsonParser.parseString(m.group(1).trim()).getAsJsonObject();
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return null;
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    /**
     * Get cache stats for reporting.
     */
    public int getCacheSize() {
        return tokenCache.size();
    }

    public int getValidCount() {
        int count = 0;
        for (SessionTokens t : tokenCache.values()) {
            if (t.isValid())
                count++;
        }
        return count;
    }
}
