package com.perftest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pplmag Traffic Replay Executor.
 * Replays traffic from JSONL files with OAuth2 authentication.
 * Features:
 * - OAuth2 token from client_id + secret in config file
 * - 1-hour token validity with automatic refresh
 * - Thread-safe token refresh on 401 errors
 */
public class PplmagReplayExecutor {

    private static final String BASE_URL = "https://cdn-test.amuselabs.com/pmm/";
    private static final String AUTH_ENDPOINT = "api/v1/token";
    private static final String CONFIG_FILE = "auth_config.json";

    private static final Gson GSON = new Gson();
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
    private static final MediaType FORM_MEDIA_TYPE = MediaType.get("application/x-www-form-urlencoded");

    // Configuration
    private final String jsonlPath;
    private final double speedFactor;
    private final boolean dryRun;
    private final boolean verbose;
    private final boolean generateHtml;

    // HTTP Client
    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService prewarmExecutor;

    // OAuth2 Authentication
    private final String clientId;
    private final String clientSecret;
    private volatile String accessToken;
    private volatile long tokenExpiryTime; // System.currentTimeMillis() when token expires
    private final ReentrantLock tokenRefreshLock = new ReentrantLock();
    private static final long TOKEN_VALIDITY_MS = 55 * 60 * 1000; // 55 minutes (buffer before 1 hour)

    // Per (userId, puzzleId) combination session state
    private final ConcurrentHashMap<String, UserSession> userSessions = new ConcurrentHashMap<>();

    // Stats
    private final AtomicInteger totalEvents = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);

    // CSV output
    private final List<String[]> csvRows = Collections.synchronizedList(new ArrayList<>());
    private volatile long replayStartTime;
    private final AtomicLong lastResponseTimeMs = new AtomicLong(0);

    /**
     * Per-user session state.
     */
    static class UserSession {
        final String loadToken;
        final String playId;

        UserSession(String loadToken, String playId) {
            this.loadToken = loadToken;
            this.playId = playId;
        }
    }

    /**
     * Traffic event from JSONL file.
     */
    static class TrafficEvent {
        int index; // Event index for CSV
        long ts;
        String userId;
        String endpoint;
        String method;
        String series;
        String puzzleId;
        Integer offset; // Can be null
        long delayMs;
    }

    public PplmagReplayExecutor(String jsonlPath, double speedFactor, boolean dryRun,
            boolean verbose, boolean generateHtml) throws IOException {
        this.jsonlPath = jsonlPath;
        this.speedFactor = speedFactor;
        this.dryRun = dryRun;
        this.verbose = verbose;
        this.generateHtml = generateHtml;

        // Load auth config
        JsonObject config = loadAuthConfig();
        this.clientId = config.get("client_id").getAsString();
        this.clientSecret = config.get("client_secret").getAsString();

        // Configure OkHttp client
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(200);
        dispatcher.setMaxRequestsPerHost(100);

        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(100, 5, TimeUnit.MINUTES))
                .dispatcher(dispatcher)
                .build();

        this.scheduler = Executors.newScheduledThreadPool(20);
        this.prewarmExecutor = Executors.newFixedThreadPool(50);

        // Get initial access token
        refreshAccessToken();
    }

    /**
     * Load auth configuration from JSON file.
     */
    private JsonObject loadAuthConfig() throws IOException {
        File configFile = new File(CONFIG_FILE);
        if (!configFile.exists()) {
            throw new IOException("Auth config file not found: " + CONFIG_FILE +
                    "\nExpected format: {\"client_id\": \"...\", \"client_secret\": \"...\"}");
        }
        String content = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
        return JsonParser.parseString(content).getAsJsonObject();
    }

    /**
     * Refresh the OAuth2 access token.
     * Thread-safe: only one thread will refresh at a time.
     */
    private void refreshAccessToken() throws IOException {
        tokenRefreshLock.lock();
        try {
            // Double-check if token was already refreshed by another thread
            if (accessToken != null && System.currentTimeMillis() < tokenExpiryTime) {
                return;
            }

            log("Fetching new access token...");

            RequestBody formBody = new FormBody.Builder()
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret)
                    .build();

            Request request = new Request.Builder()
                    .url(BASE_URL + AUTH_ENDPOINT)
                    .addHeader("Accept", "application/json")
                    .post(formBody)
                    .build();

            try (Response response = client.newCall(request).execute()) {
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
     * Get the current access token, refreshing if needed.
     */
    private String getAccessToken() throws IOException {
        if (accessToken == null || System.currentTimeMillis() >= tokenExpiryTime) {
            refreshAccessToken();
        }
        return accessToken;
    }

    /**
     * Handle a 401 error by refreshing the token.
     * Returns true if token was refreshed successfully.
     */
    private boolean handleUnauthorized() {
        try {
            // Force token refresh
            tokenRefreshLock.lock();
            try {
                // Invalidate current token
                tokenExpiryTime = 0;
            } finally {
                tokenRefreshLock.unlock();
            }
            refreshAccessToken();
            return true;
        } catch (IOException e) {
            log("Failed to refresh token after 401: " + e.getMessage());
            return false;
        }
    }

    /**
     * Execute an authenticated GET request.
     * Automatically retries once on 401 with token refresh.
     */
    private Response executeAuthenticatedGet(HttpUrl url) throws IOException {
        for (int attempt = 0; attempt < 2; attempt++) {
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "Bearer " + getAccessToken())
                    .get()
                    .build();

            Response response = client.newCall(request).execute();

            if (response.code() == 401 && attempt == 0) {
                response.close();
                if (handleUnauthorized()) {
                    continue; // Retry with new token
                }
            }

            return response;
        }
        throw new IOException("Failed after retry");
    }

    /**
     * Execute an authenticated POST request.
     * Automatically retries once on 401 with token refresh.
     */
    private Response executeAuthenticatedPost(String url, RequestBody body) throws IOException {
        for (int attempt = 0; attempt < 2; attempt++) {
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "Bearer " + getAccessToken())
                    .post(body)
                    .build();

            Response response = client.newCall(request).execute();

            if (response.code() == 401 && attempt == 0) {
                response.close();
                if (handleUnauthorized()) {
                    continue; // Retry with new token
                }
            }

            return response;
        }
        throw new IOException("Failed after retry");
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[PplmagReplay] " + msg);
        }
    }

    // =========================================================================
    // HTML Parsing Utilities
    // =========================================================================

    private static final Pattern PARAMS_PATTERN = Pattern.compile(
            "window\\.rawc\\s*=\\s*\\{([^}]+)\\}",
            Pattern.DOTALL);
    private static final Pattern ATTR_PATTERN = Pattern.compile(
            "(\\w+)\\s*:\\s*\"([^\"]+)\"");
    // Pattern for <script id="params">{JSON}</script>
    private static final Pattern SCRIPT_PARAMS_PATTERN = Pattern.compile(
            "<script[^>]*id=\"params\"[^>]*>([^<]+)</script>",
            Pattern.DOTALL);

    private String b64Decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private JsonObject b64DecodeJson(String encoded) {
        return JsonParser.parseString(b64Decode(encoded)).getAsJsonObject();
    }

    /**
     * Extract parameters from HTML - supports both window.rawc format and
     * script id="params" JSON format.
     */
    private JsonObject extractParamsFromHtml(String html) {
        // First try window.rawc = {...} format
        Matcher matcher = PARAMS_PATTERN.matcher(html);
        if (matcher.find()) {
            String content = matcher.group(1);
            JsonObject result = new JsonObject();
            Matcher attrMatcher = ATTR_PATTERN.matcher(content);
            while (attrMatcher.find()) {
                result.addProperty(attrMatcher.group(1), attrMatcher.group(2));
            }
            return result;
        }

        // Try <script id="params">{JSON}</script> format
        Matcher scriptMatcher = SCRIPT_PARAMS_PATTERN.matcher(html);
        if (scriptMatcher.find()) {
            try {
                return JsonParser.parseString(scriptMatcher.group(1).trim()).getAsJsonObject();
            } catch (Exception e) {
                // Ignore parse errors
            }
        }

        return null;
    }

    // =========================================================================
    // Pre-warm Phase
    // =========================================================================

    private static final String SET_PARAM = "gandalf";
    private static final String FALLBACK_PUZZLE_ID = "ce996e5f";

    /**
     * Pre-warm a (userId, puzzleId) combination by fetching /crossword.
     * URL: /crossword?id={puzzleId}&set=gandalf&uid={userId}
     * If rawL is not found, falls back to a known valid puzzleId.
     */
    private UserSession prewarmUser(String userId, String puzzleId) throws IOException {
        // Try with original puzzleId first
        UserSession session = fetchCrosswordSession(userId, puzzleId);

        // If loadToken is empty and we're not already using fallback, try with fallback
        // puzzleId
        if ((session.loadToken == null || session.loadToken.isEmpty())
                && !puzzleId.equals(FALLBACK_PUZZLE_ID)) {
            session = fetchCrosswordSession(userId, FALLBACK_PUZZLE_ID);
        }

        return session;
    }

    /**
     * Fetch crossword page and extract session tokens.
     */
    private UserSession fetchCrosswordSession(String userId, String puzzleId) throws IOException {
        HttpUrl crosswordUrl = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", puzzleId)
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("uid", userId)
                .build();

        Request request = new Request.Builder()
                .url(crosswordUrl)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        String loadToken = "";
        String playId = "";

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("crossword failed: " + response.code());
            }

            String body = response.body().string();
            JsonObject params = extractParamsFromHtml(body);

            if (params != null) {
                // rawL IS the loadToken directly (no decoding needed)
                if (params.has("rawL")) {
                    loadToken = params.get("rawL").getAsString();
                } else if (params.has("rawl")) {
                    loadToken = params.get("rawl").getAsString();
                }

                // Extract playId from rawp (still needs decoding)
                if (params.has("rawp")) {
                    try {
                        JsonObject decodedPlay = b64DecodeJson(params.get("rawp").getAsString());
                        if (decodedPlay.has("playId")) {
                            playId = decodedPlay.get("playId").getAsString();
                        }
                    } catch (Exception e) {
                        // playId is optional
                    }
                }
            }
        }

        return new UserSession(loadToken, playId);
    }

    /**
     * Create session key from userId and puzzleId.
     */
    private static String sessionKey(String userId, String puzzleId) {
        return userId + ":" + puzzleId;
    }

    // =========================================================================
    // JSONL Parsing
    // =========================================================================

    /**
     * Parse a single JSONL line into a TrafficEvent.
     */
    private TrafficEvent parseEvent(String line) {
        JsonObject json = JsonParser.parseString(line).getAsJsonObject();
        TrafficEvent event = new TrafficEvent();
        event.ts = json.has("ts") ? json.get("ts").getAsLong() : 0;
        event.userId = json.has("userId") ? json.get("userId").getAsString() : "";
        event.endpoint = json.has("endpoint") ? json.get("endpoint").getAsString() : "";
        event.method = json.has("method") ? json.get("method").getAsString() : "GET";
        event.series = json.has("series") ? json.get("series").getAsString() : "";
        event.puzzleId = json.has("puzzleId") ? json.get("puzzleId").getAsString() : "";
        event.offset = json.has("offset") && !json.get("offset").isJsonNull()
                ? json.get("offset").getAsInt()
                : null;
        event.delayMs = json.has("delayMs") ? json.get("delayMs").getAsLong() : 0;
        return event;
    }

    // =========================================================================
    // API Endpoint Methods (async)
    // =========================================================================

    private static final Random RANDOM = new Random();

    /**
     * Generate random digit string for state fields.
     */
    private String randomDigitString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(RANDOM.nextInt(10));
        }
        return sb.toString();
    }

    /**
     * POST /api/v1/plays - Submit play data (async).
     * Uses puzzleId and series from the TrafficEvent.
     */
    private CompletableFuture<Void> firePostPlaysAsync(String userId, UserSession session,
            String puzzleId, String series) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (session == null || session.loadToken == null || session.loadToken.isEmpty()) {
            future.completeExceptionally(new IOException("No session for plays"));
            return future;
        }

        long ts = System.currentTimeMillis();
        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", session.loadToken);
        payload.addProperty("updatePlayTable", true);
        payload.addProperty("updateLoadTable", false);
        payload.addProperty("series", series);
        payload.addProperty("id", puzzleId);
        payload.addProperty("playId", session.playId != null ? session.playId : "");
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
        payload.addProperty("primaryState", randomDigitString(15));
        payload.addProperty("secondaryState", randomDigitString(15));

        Request request = new Request.Builder()
                .url(BASE_URL + "api/v1/plays")
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful()) {
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    } else {
                        future.complete(null);
                    }
                }
            }
        });

        return future;
    }

    /**
     * POST /postPickerStatus - Submit picker status (async).
     * Uses loadToken from the user session.
     */
    private CompletableFuture<Void> firePostPickerStatusAsync(UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (session == null || session.loadToken == null || session.loadToken.isEmpty()) {
            future.completeExceptionally(new IOException("No session for postPickerStatus"));
            return future;
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", session.loadToken);
        payload.addProperty("isVerified", true);
        payload.addProperty("adDuration", 0);
        payload.addProperty("reason", "displaying puzzle picker");

        Request request = new Request.Builder()
                .url(BASE_URL + "postPickerStatus")
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE))
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful()) {
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    } else {
                        future.complete(null);
                    }
                }
            }
        });

        return future;
    }

    /**
     * GET /crossword - Fetch crossword page (async).
     * Simple version with just id (userId) and set=gandalf.
     */
    private CompletableFuture<Void> fireGetCrosswordAsync(String userId) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        HttpUrl url = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", userId)
                .addQueryParameter("set", SET_PARAM) // gandalf
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful()) {
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    } else {
                        future.complete(null);
                    }
                }
            }
        });

        return future;
    }

    /**
     * GET /api/v1/puzzles - Fetch puzzle metadata (async with auth).
     * Uses series=gandalf (hardcoded), limit=14 (hardcoded), offset from JSONL.
     */
    private CompletableFuture<Void> fireGetPuzzlesAsync(Integer offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        HttpUrl url = HttpUrl.parse(BASE_URL + "api/v1/puzzles").newBuilder()
                .addQueryParameter("series", SET_PARAM) // gandalf
                .addQueryParameter("limit", "14")
                .addQueryParameter("offset", offset != null ? String.valueOf(offset) : "0")
                .build();

        try {
            String token = getAccessToken();
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "Bearer " + token)
                    .get()
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    future.completeExceptionally(e);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (response) {
                        if (response.code() == 401) {
                            // Token expired, try refresh and retry once
                            if (handleUnauthorized()) {
                                // Retry with new token
                                try {
                                    Request retryRequest = new Request.Builder()
                                            .url(url)
                                            .addHeader("Accept", "application/json")
                                            .addHeader("Authorization", "Bearer " + accessToken)
                                            .get()
                                            .build();

                                    try (Response retryResponse = client.newCall(retryRequest).execute()) {
                                        if (!retryResponse.isSuccessful()) {
                                            future.completeExceptionally(
                                                    new IOException("HTTP " + retryResponse.code()));
                                        } else {
                                            future.complete(null);
                                        }
                                    }
                                } catch (IOException retryE) {
                                    future.completeExceptionally(retryE);
                                }
                            } else {
                                future.completeExceptionally(new IOException("HTTP 401 - Token refresh failed"));
                            }
                        } else if (!response.isSuccessful()) {
                            future.completeExceptionally(new IOException("HTTP " + response.code()));
                        } else {
                            future.complete(null);
                        }
                    }
                }
            });
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * GET /api/v1/plays - Fetch play data (async with auth).
     * Uses series=gandalf (hardcoded), limit=14 (hardcoded), offset from JSONL.
     * Optional puzzleIds as comma-separated string if available.
     */
    private CompletableFuture<Void> fireGetPlaysAsync(Integer offset, String puzzleIds) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        HttpUrl.Builder urlBuilder = HttpUrl.parse(BASE_URL + "api/v1/plays").newBuilder()
                .addQueryParameter("series", SET_PARAM) // gandalf
                .addQueryParameter("limit", "14")
                .addQueryParameter("offset", offset != null ? String.valueOf(offset) : "0");

        // Add puzzleIds if provided
        if (puzzleIds != null && !puzzleIds.isEmpty()) {
            urlBuilder.addQueryParameter("puzzleIds", puzzleIds);
        }

        HttpUrl url = urlBuilder.build();

        try {
            String token = getAccessToken();
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "Bearer " + token)
                    .get()
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    future.completeExceptionally(e);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (response) {
                        if (response.code() == 401) {
                            // Token expired, try refresh and retry once
                            if (handleUnauthorized()) {
                                try {
                                    Request retryRequest = new Request.Builder()
                                            .url(url)
                                            .addHeader("Accept", "application/json")
                                            .addHeader("Authorization", "Bearer " + accessToken)
                                            .get()
                                            .build();

                                    try (Response retryResponse = client.newCall(retryRequest).execute()) {
                                        if (!retryResponse.isSuccessful()) {
                                            future.completeExceptionally(
                                                    new IOException("HTTP " + retryResponse.code()));
                                        } else {
                                            future.complete(null);
                                        }
                                    }
                                } catch (IOException retryE) {
                                    future.completeExceptionally(retryE);
                                }
                            } else {
                                future.completeExceptionally(new IOException("HTTP 401 - Token refresh failed"));
                            }
                        } else if (!response.isSuccessful()) {
                            future.completeExceptionally(new IOException("HTTP " + response.code()));
                        } else {
                            future.complete(null);
                        }
                    }
                }
            });
        } catch (IOException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Run the replay from JSONL file.
     */
    public void run() throws Exception {
        log("Starting authenticated traffic replay from: " + jsonlPath);
        log(String.format("Speed factor: %.1fx, Dry run: %s", speedFactor, dryRun));

        // Phase 1: Parse JSONL and extract unique (userId, puzzleId) combinations
        log("Phase 1: Parsing JSONL file...");
        List<TrafficEvent> events = new ArrayList<>();
        Set<String> uniqueCombos = new LinkedHashSet<>(); // "userId:puzzleId" keys

        try (BufferedReader reader = new BufferedReader(new FileReader(jsonlPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                try {
                    TrafficEvent event = parseEvent(line);
                    events.add(event);
                    // Only add if both userId and puzzleId are non-empty
                    if (!event.userId.isEmpty() && event.puzzleId != null && !event.puzzleId.isEmpty()) {
                        uniqueCombos.add(sessionKey(event.userId, event.puzzleId));
                    }
                } catch (Exception e) {
                    // Skip malformed lines
                }
            }
        }

        log(String.format("Found %d events and %d unique (userId, puzzleId) combinations",
                events.size(), uniqueCombos.size()));

        // Phase 2: Pre-warm (userId, puzzleId) combinations (fetch loadToken and
        // playId)
        if (!dryRun && !uniqueCombos.isEmpty()) {
            log(String.format("Phase 2: Pre-warming %d combinations with %d threads...",
                    uniqueCombos.size(), 50));
            long prewarmStart = System.currentTimeMillis();

            AtomicInteger warmed = new AtomicInteger(0);
            AtomicInteger failed = new AtomicInteger(0);
            AtomicLong totalPrewarmTimeMs = new AtomicLong(0);
            final int totalCombos = uniqueCombos.size();

            List<Future<?>> futures = new ArrayList<>();
            for (String comboKey : uniqueCombos) {
                // Parse combo key back to userId and puzzleId
                String[] parts = comboKey.split(":", 2);
                String userId = parts[0];
                String puzzleId = parts[1];

                futures.add(prewarmExecutor.submit(() -> {
                    long userStart = System.currentTimeMillis();
                    try {
                        UserSession session = prewarmUser(userId, puzzleId);
                        userSessions.put(comboKey, session);
                        long userDuration = System.currentTimeMillis() - userStart;
                        totalPrewarmTimeMs.addAndGet(userDuration);
                        int count = warmed.incrementAndGet();

                        // Progress every 50 combos
                        if (count % 50 == 0) {
                            double avgMs = (double) totalPrewarmTimeMs.get() / count;
                            int remaining = totalCombos - count - failed.get();
                            double etaSeconds = (remaining * avgMs / 1000.0) / 50;
                            System.out.printf("\r[Prewarm] %d/%d combos | avg: %.0fms | ETA: %.0fs   ",
                                    count, totalCombos, avgMs, etaSeconds);
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        // Individual failures are summarized at end
                    }
                }));
            }

            // Wait for all pre-warm tasks
            for (Future<?> f : futures) {
                try {
                    f.get(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Ignore
                }
            }

            long prewarmDuration = System.currentTimeMillis() - prewarmStart;
            double avgPrewarmMs = warmed.get() > 0 ? (double) totalPrewarmTimeMs.get() / warmed.get() : 0;
            System.out.println();
            log(String.format("Pre-warmed %d combos (%d failed) in %.1f seconds (avg %.0fms/combo)",
                    warmed.get(), failed.get(), prewarmDuration / 1000.0, avgPrewarmMs));
        }

        // Phase 3: Replay traffic - schedule and fire requests
        log(String.format("Phase 3: Starting replay of %d events...", events.size()));
        replayStartTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(events.size());
        totalEvents.set(events.size());

        long cumulativeDelayMs = 0;
        int eventIndex = 0;
        for (TrafficEvent event : events) {
            event.index = eventIndex++;
            cumulativeDelayMs += (long) (event.delayMs / speedFactor);
            final long scheduledDelayMs = cumulativeDelayMs;

            scheduler.schedule(() -> {
                fireRequest(event, latch, scheduledDelayMs);
            }, scheduledDelayMs, TimeUnit.MILLISECONDS);
        }

        // Wait for all requests to complete
        long expectedDurationMs = cumulativeDelayMs + 60_000; // add 60s buffer for responses
        log(String.format("All events scheduled. Waiting for completion (max %.0f seconds)...",
                expectedDurationMs / 1000.0));

        try {
            boolean completed = latch.await(expectedDurationMs, TimeUnit.MILLISECONDS);
            if (!completed) {
                log("Warning: Timeout waiting for all requests to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Print summary
        long totalDuration = System.currentTimeMillis() - replayStartTime;
        log(String.format("Replay complete: %d success, %d failed in %.1f seconds",
                successCount.get(), failCount.get(), totalDuration / 1000.0));

        // Calculate original duration from events
        long originalDurationMs = 0;
        for (TrafficEvent event : events) {
            originalDurationMs += event.delayMs;
        }

        // Generate HTML report from CSV data
        if (generateHtml && !csvRows.isEmpty()) {
            try {
                List<ReplayReportWriter.ReplayEvent> replayEvents = new ArrayList<>();
                for (String[] row : csvRows) {
                    replayEvents.add(new ReplayReportWriter.ReplayEvent(
                            Long.parseLong(row[1]), // scheduledMs
                            Long.parseLong(row[2]), // actualMs
                            Long.parseLong(row[3]), // latencyMs
                            "true".equals(row[5]), // success
                            row[6], // endpoint
                            row[7], // userId
                            row[8] // error
                    ));
                }

                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String baseName = new File(jsonlPath).getName().replace(".jsonl", "");
                String htmlPath = String.format("results/pplmag_replay_%s_%s_%.0fx.html", baseName, timestamp,
                        speedFactor);

                String savedPath = ReplayReportWriter.saveHtml(
                        "Pplmag Replay: " + baseName,
                        replayEvents,
                        speedFactor,
                        totalDuration,
                        originalDurationMs,
                        htmlPath);
                log("HTML report saved to: " + savedPath);
            } catch (IOException e) {
                System.err.println("Failed to save HTML report: " + e.getMessage());
            }
        }
    }

    /**
     * Route and fire a request based on the event's endpoint and method.
     */
    private void fireRequest(TrafficEvent event, CountDownLatch latch, long scheduledDelayMs) {
        long startTime = System.currentTimeMillis();
        long actualTimeOffset = startTime - replayStartTime;
        // Look up session by (userId, puzzleId) combo
        String comboKey = sessionKey(event.userId, event.puzzleId != null ? event.puzzleId : "");
        UserSession session = userSessions.get(comboKey);
        String endpointKey = event.method + " " + event.endpoint;

        CompletableFuture<Void> future;

        try {
            if (event.endpoint.equals("/api/v1/plays") && event.method.equals("POST")) {
                future = firePostPlaysAsync(event.userId, session, event.puzzleId, event.series);
            } else if (event.endpoint.equals("/api/v1/plays") && event.method.equals("GET")) {
                future = fireGetPlaysAsync(event.offset, event.puzzleId);
            } else if (event.endpoint.equals("/api/v1/puzzles") && event.method.equals("GET")) {
                future = fireGetPuzzlesAsync(event.offset);
            } else if (event.endpoint.equals("/crossword") && event.method.equals("GET")) {
                future = fireGetCrosswordAsync(event.userId);
            } else if (event.endpoint.equals("/postPickerStatus") && event.method.equals("POST")) {
                future = firePostPickerStatusAsync(session);
            } else {
                // Unknown endpoint - skip
                if (verbose) {
                    log(String.format("Unknown endpoint: %s %s", event.method, event.endpoint));
                }
                addCsvRow(event.index, scheduledDelayMs, actualTimeOffset, 0, false, endpointKey, event.userId,
                        "Unknown endpoint");
                latch.countDown();
                return;
            }

            future.whenComplete((result, error) -> {
                long latency = System.currentTimeMillis() - startTime;
                long actualOffset = startTime - replayStartTime;
                totalLatencyMs.addAndGet(latency);

                if (error != null) {
                    failCount.incrementAndGet();
                    addCsvRow(event.index, scheduledDelayMs, actualOffset, latency, false, endpointKey, event.userId,
                            error.getMessage());
                    if (verbose) {
                        log(String.format("FAIL %s %s: %s (%.0fms)",
                                event.method, event.endpoint, error.getMessage(), (double) latency));
                    }
                } else {
                    successCount.incrementAndGet();
                    addCsvRow(event.index, scheduledDelayMs, actualOffset, latency, true, endpointKey, event.userId,
                            "");
                    if (verbose) {
                        log(String.format("OK %s %s (%.0fms)", event.method, event.endpoint, (double) latency));
                    }
                }
                latch.countDown();
            });
        } catch (Exception e) {
            failCount.incrementAndGet();
            addCsvRow(event.index, scheduledDelayMs, actualTimeOffset, 0, false, endpointKey, event.userId,
                    e.getMessage());
            if (verbose) {
                log(String.format("ERROR %s %s: %s", event.method, event.endpoint, e.getMessage()));
            }
            latch.countDown();
        }
    }

    /**
     * Add a row to the CSV output.
     */
    private void addCsvRow(int index, long scheduledMs, long actualMs, long latencyMs, boolean success,
            String endpoint, String userId, String error) {
        long responseMs = actualMs + latencyMs;
        csvRows.add(new String[] {
                String.valueOf(index),
                String.valueOf(scheduledMs),
                String.valueOf(actualMs),
                String.valueOf(latencyMs),
                String.valueOf(responseMs),
                success ? "true" : "false",
                endpoint,
                userId != null ? userId : "",
                error
        });
        lastResponseTimeMs.updateAndGet(current -> Math.max(current, responseMs));
    }

    /**
     * Shutdown executors and HTTP client.
     */
    public void shutdown() {
        scheduler.shutdown();
        prewarmExecutor.shutdown();
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
            prewarmExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================
    // Main entry point
    // =========================================================================

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: PplmagReplayExecutor <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --speed <factor>   Speed factor (default: 1.0, e.g., 5.0 = 5x faster)");
            System.out.println("  --dry-run          Don't actually send requests");
            System.out.println("  --html             Generate HTML report (default: on)");
            System.out.println("  --no-html          Disable HTML report generation");
            System.out.println("  -v, --verbose      Verbose output (default: on)");
            System.out.println("  -q, --quiet        Quiet output (only summary)");
            System.out.println();
            System.out.println("Expects auth_config.json in current directory");
            return;
        }

        String jsonlPath = args[0];
        double speedFactor = 1.0;
        boolean verbose = true;
        boolean dryRun = false;
        boolean html = true;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--speed":
                    speedFactor = Double.parseDouble(args[++i]);
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--html":
                    html = true;
                    break;
                case "--no-html":
                    html = false;
                    break;
                case "-v":
                case "--verbose":
                    verbose = true;
                    break;
                case "-q":
                case "--quiet":
                    verbose = false;
                    break;
            }
        }

        PplmagReplayExecutor executor = null;
        try {
            executor = new PplmagReplayExecutor(jsonlPath, speedFactor, dryRun, verbose, html);
            executor.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }
}
