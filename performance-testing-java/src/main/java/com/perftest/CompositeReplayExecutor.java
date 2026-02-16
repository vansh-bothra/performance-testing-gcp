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
 * Composite Traffic Replay Executor.
 * Replays merged traffic from JSONL containing both PMM (app) and Pplmag
 * sources.
 * Routes events to appropriate handlers based on "source" field.
 * 
 * PMM events: Use cookie-based sessions (userId keyed)
 * Pplmag events: Use OAuth + (userId:puzzleId) keyed sessions
 */
public class CompositeReplayExecutor {

    private static final String BASE_URL = "https://cdn-test.amuselabs.com/pmm/";
    private static final String AUTH_ENDPOINT = "api/v1/token";
    private static final String CONFIG_FILE = "auth_config.json";
    private static final String SET_PARAM = "gandalf";
    private static final String FALLBACK_PUZZLE_ID = "ce996e5f";

    private static final Gson GSON = new Gson();
    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    // Configuration
    private final String jsonlPath;
    private final double speedFactor;
    private final boolean dryRun;
    private final boolean verbose;
    private final boolean generateHtml;

    // HTTP Client (shared)
    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService prewarmExecutor;

    // OAuth2 Authentication (for pplmag)
    private final String clientId;
    private final String clientSecret;
    private volatile String accessToken;
    private volatile long tokenExpiryTime;
    private final ReentrantLock tokenRefreshLock = new ReentrantLock();
    private static final long TOKEN_VALIDITY_MS = 55 * 60 * 1000;

    // Session stores - separate for each source
    private final ConcurrentHashMap<String, UserSession> pmmSessions = new ConcurrentHashMap<>(); // keyed by userId
    private final ConcurrentHashMap<String, UserSession> pplmagSessions = new ConcurrentHashMap<>(); // keyed by
                                                                                                     // userId:puzzleId

    // Stats
    private final AtomicInteger totalEvents = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);

    // CSV output
    private final List<String[]> csvRows = Collections.synchronizedList(new ArrayList<>());
    private volatile long replayStartTime;
    private final AtomicLong lastResponseTimeMs = new AtomicLong(0);

    static class UserSession {
        final String loadToken;
        final String playId;

        UserSession(String loadToken, String playId) {
            this.loadToken = loadToken;
            this.playId = playId;
        }
    }

    /**
     * Unified traffic event from JSONL.
     */
    static class TrafficEvent {
        int index;
        long ts;
        String source; // "pmm" or "pplmag"
        String endpoint;
        String method; // pplmag only
        String userId;
        String series; // pplmag only
        String puzzleId; // pplmag only
        Integer offset; // pplmag only
        long delayMs;
        int isLastReq; // pmm only
    }

    public CompositeReplayExecutor(String jsonlPath, double speedFactor, boolean dryRun,
            boolean verbose, boolean generateHtml) throws IOException {
        this.jsonlPath = jsonlPath;
        this.speedFactor = speedFactor;
        this.dryRun = dryRun;
        this.verbose = verbose;
        this.generateHtml = generateHtml;

        // Load pplmag auth config
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
        this.prewarmExecutor = Executors.newFixedThreadPool(100);

        // Get initial access token for pplmag
        refreshAccessToken();
    }

    private JsonObject loadAuthConfig() throws IOException {
        File configFile = new File(CONFIG_FILE);
        if (!configFile.exists()) {
            throw new IOException("Auth config file not found: " + CONFIG_FILE);
        }
        String content = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
        return JsonParser.parseString(content).getAsJsonObject();
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[CompositeReplay] " + msg);
        }
    }

    // =========================================================================
    // OAuth2 (for pplmag)
    // =========================================================================

    private void refreshAccessToken() throws IOException {
        tokenRefreshLock.lock();
        try {
            if (accessToken != null && System.currentTimeMillis() < tokenExpiryTime) {
                return;
            }
            log("Fetching OAuth access token...");
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
                this.accessToken = json.get("access_token").getAsString();
                this.tokenExpiryTime = System.currentTimeMillis() + TOKEN_VALIDITY_MS;
                log("OAuth token obtained, valid for 55 minutes");
            }
        } finally {
            tokenRefreshLock.unlock();
        }
    }

    private String getAccessToken() throws IOException {
        if (accessToken == null || System.currentTimeMillis() >= tokenExpiryTime) {
            refreshAccessToken();
        }
        return accessToken;
    }

    // =========================================================================
    // HTML Parsing Utilities
    // =========================================================================

    private String b64Decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private JsonObject b64DecodeJson(String encoded) {
        return JsonParser.parseString(b64Decode(encoded)).getAsJsonObject();
    }

    private String[] escapeCSV(String[] values) {
        String[] escaped = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            String val = values[i];
            if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
                escaped[i] = "\"" + val.replace("\"", "\"\"") + "\"";
            } else {
                escaped[i] = val;
            }
        }
        return escaped;
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
        return null;
    }

    // =========================================================================
    // PMM Prewarm (per userId)
    // =========================================================================

    private static final String PMM_PUZZLE_ID = "ce996e5f";

    private UserSession prewarmPmmUser(String userId) throws IOException {
        // Step 1: GET date-picker to get loadToken
        HttpUrl datePickerUrl = HttpUrl.parse(BASE_URL + "date-picker").newBuilder()
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("uid", userId)
                .build();

        Request datePickerReq = new Request.Builder()
                .url(datePickerUrl)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        String loadToken;
        try (Response response = client.newCall(datePickerReq).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("date-picker failed: " + response.code());
            }

            String body = response.body().string();
            JsonObject params = extractParamsFromHtml(body);
            if (params == null || !params.has("rawsps")) {
                throw new IOException("No rawsps in date-picker response");
            }

            JsonObject decoded = b64DecodeJson(params.get("rawsps").getAsString());
            loadToken = decoded.has("loadToken") ? decoded.get("loadToken").getAsString() : null;
            if (loadToken == null) {
                throw new IOException("No loadToken in rawsps");
            }
        }

        // Step 2: GET crossword to get playId
        String srcUrl = String.format("%sdate-picker?set=%s&uid=%s", BASE_URL, SET_PARAM, userId);

        HttpUrl crosswordUrl = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", PMM_PUZZLE_ID)
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("picker", "date-picker")
                .addQueryParameter("src", srcUrl)
                .addQueryParameter("uid", userId)
                .addQueryParameter("loadToken", loadToken)
                .build();

        Request crosswordReq = new Request.Builder()
                .url(crosswordUrl)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                .get()
                .build();

        String playId = "";
        try (Response response = client.newCall(crosswordReq).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("crossword failed: " + response.code());
            }

            String body = response.body().string();
            JsonObject params = extractParamsFromHtml(body);
            if (params != null && params.has("rawp")) {
                JsonObject decodedPlay = b64DecodeJson(params.get("rawp").getAsString());
                if (decodedPlay.has("playId")) {
                    playId = decodedPlay.get("playId").getAsString();
                }
            }
        }

        return new UserSession(loadToken, playId);
    }

    // =========================================================================
    // Pplmag Prewarm (per userId:puzzleId)
    // =========================================================================

    private UserSession prewarmPplmagUser(String userId, String puzzleId) throws IOException {
        UserSession session = fetchPplmagCrossword(userId, puzzleId);
        if ((session.loadToken == null || session.loadToken.isEmpty()) && !puzzleId.equals(FALLBACK_PUZZLE_ID)) {
            session = fetchPplmagCrossword(userId, FALLBACK_PUZZLE_ID);
        }
        return session;
    }

    private UserSession fetchPplmagCrossword(String userId, String puzzleId) throws IOException {
        HttpUrl url = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", puzzleId)
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("uid", userId)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html")
                .addHeader("User-Agent", "Mozilla/5.0")
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
                if (params.has("rawL")) {
                    loadToken = params.get("rawL").getAsString();
                } else if (params.has("rawl")) {
                    loadToken = params.get("rawl").getAsString();
                }
                if (params.has("rawp")) {
                    try {
                        JsonObject decoded = b64DecodeJson(params.get("rawp").getAsString());
                        if (decoded.has("playId")) {
                            playId = decoded.get("playId").getAsString();
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }
        return new UserSession(loadToken, playId);
    }

    private static String pplmagSessionKey(String userId, String puzzleId) {
        return userId + ":" + puzzleId;
    }

    // =========================================================================
    // JSONL Parsing
    // =========================================================================

    private TrafficEvent parseEvent(String line) {
        JsonObject json = JsonParser.parseString(line).getAsJsonObject();
        TrafficEvent event = new TrafficEvent();
        event.ts = json.has("ts") ? json.get("ts").getAsLong() : 0;
        event.source = json.has("source") ? json.get("source").getAsString() : "pmm";
        event.endpoint = json.has("endpoint") ? json.get("endpoint").getAsString() : "";
        event.method = json.has("method") ? json.get("method").getAsString() : "GET";
        event.userId = json.has("userId") ? json.get("userId").getAsString() : "";
        event.series = json.has("series") ? json.get("series").getAsString() : "";
        event.puzzleId = json.has("puzzleId") ? json.get("puzzleId").getAsString() : "";
        event.offset = json.has("offset") && !json.get("offset").isJsonNull() ? json.get("offset").getAsInt() : null;
        event.delayMs = json.has("delayMs") ? json.get("delayMs").getAsLong() : 0;
        event.isLastReq = json.has("isLastReq") ? json.get("isLastReq").getAsInt() : 0;
        return event;
    }

    // =========================================================================
    // Main Run Method
    // =========================================================================

    public void run() throws Exception {
        log("Starting composite traffic replay from: " + jsonlPath);
        log(String.format("Speed factor: %.1fx, Dry run: %s", speedFactor, dryRun));

        // Phase 1: Parse JSONL and extract unique prewarm targets
        log("Phase 1: Parsing JSONL file...");
        List<TrafficEvent> events = new ArrayList<>();
        Set<String> pmmUsers = new LinkedHashSet<>();
        Set<String> pplmagCombos = new LinkedHashSet<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(jsonlPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#"))
                    continue;
                try {
                    TrafficEvent event = parseEvent(line);
                    events.add(event);
                    if ("pmm".equals(event.source) && !event.userId.isEmpty()) {
                        pmmUsers.add(event.userId);
                    } else if ("pplmag".equals(event.source) && !event.userId.isEmpty()
                            && event.puzzleId != null && !event.puzzleId.isEmpty()) {
                        pplmagCombos.add(pplmagSessionKey(event.userId, event.puzzleId));
                    }
                } catch (Exception e) {
                }
            }
        }

        log(String.format("Found %d events: %d PMM users, %d Pplmag (user,puzzle) combos",
                events.size(), pmmUsers.size(), pplmagCombos.size()));

        // Phase 2: Pre-warm both sources
        if (!dryRun) {
            // 2a: PMM prewarm
            if (!pmmUsers.isEmpty()) {
                log(String.format("Phase 2a: Pre-warming %d PMM users...", pmmUsers.size()));
                long start = System.currentTimeMillis();
                AtomicInteger warmed = new AtomicInteger(0);
                AtomicInteger failed = new AtomicInteger(0);
                List<Future<?>> futures = new ArrayList<>();

                for (String userId : pmmUsers) {
                    futures.add(prewarmExecutor.submit(() -> {
                        try {
                            UserSession session = prewarmPmmUser(userId);
                            pmmSessions.put(userId, session);
                            warmed.incrementAndGet();
                        } catch (Exception e) {
                            failed.incrementAndGet();
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                    }
                }
                log(String.format("PMM: Pre-warmed %d users (%d failed) in %.1fs",
                        warmed.get(), failed.get(), (System.currentTimeMillis() - start) / 1000.0));
            }

            // 2b: Pplmag prewarm
            if (!pplmagCombos.isEmpty()) {
                log(String.format("Phase 2b: Pre-warming %d Pplmag combos...", pplmagCombos.size()));
                long start = System.currentTimeMillis();
                AtomicInteger warmed = new AtomicInteger(0);
                AtomicInteger failed = new AtomicInteger(0);
                List<Future<?>> futures = new ArrayList<>();

                for (String comboKey : pplmagCombos) {
                    String[] parts = comboKey.split(":", 2);
                    String userId = parts[0];
                    String puzzleId = parts[1];
                    futures.add(prewarmExecutor.submit(() -> {
                        try {
                            UserSession session = prewarmPplmagUser(userId, puzzleId);
                            pplmagSessions.put(comboKey, session);
                            warmed.incrementAndGet();
                        } catch (Exception e) {
                            failed.incrementAndGet();
                        }
                    }));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                    }
                }
                log(String.format("Pplmag: Pre-warmed %d combos (%d failed) in %.1fs",
                        warmed.get(), failed.get(), (System.currentTimeMillis() - start) / 1000.0));
            }
        }

        // Phase 3: Replay
        log(String.format("Phase 3: Starting replay of %d events...", events.size()));
        replayStartTime = System.currentTimeMillis();
        totalEvents.set(events.size());
        CountDownLatch latch = new CountDownLatch(events.size());

        long cumulativeDelayMs = 0;
        int eventIndex = 0;
        for (TrafficEvent event : events) {
            event.index = eventIndex++;
            cumulativeDelayMs += (long) (event.delayMs / speedFactor);
            final long scheduledDelayMs = cumulativeDelayMs;

            scheduler.schedule(() -> fireRequest(event, latch, scheduledDelayMs),
                    scheduledDelayMs, TimeUnit.MILLISECONDS);
        }

        long expectedDurationMs = cumulativeDelayMs + 60_000;
        log(String.format("All events scheduled. Waiting for completion (max %.0f seconds)...",
                expectedDurationMs / 1000.0));

        try {
            latch.await(expectedDurationMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long totalDuration = System.currentTimeMillis() - replayStartTime;
        log(String.format("Replay complete: %d success, %d failed in %.1f seconds",
                successCount.get(), failCount.get(), totalDuration / 1000.0));

        // Wait a bit for late async callbacks to complete before generating reports
        log("Waiting 3 seconds for remaining async callbacks...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Take a snapshot to avoid ConcurrentModificationException
        List<String[]> csvSnapshot;
        synchronized (csvRows) {
            csvSnapshot = new ArrayList<>(csvRows);
        }

        // Generate CSV report
        if (!csvSnapshot.isEmpty()) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String baseName = new File(jsonlPath).getName().replace(".jsonl", "");
            String csvPath = String.format("results/composite_replay_%s_%s_%.0fx.csv",
                    baseName, timestamp, speedFactor);
            try {
                new File("results").mkdirs();
                try (java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter(csvPath))) {
                    pw.println(
                            "eventIndex,scheduledDelayMs,actualTimeOffset,latencyMs,source,success,method,endpoint,body");
                    for (String[] row : csvSnapshot) {
                        pw.println(String.join(",", escapeCSV(row)));
                    }
                }
                log("CSV report saved to: " + csvPath);
            } catch (IOException e) {
                System.err.println("Failed to save CSV report: " + e.getMessage());
            }
        }

        // Generate HTML report
        if (generateHtml && !csvSnapshot.isEmpty()) {
            try {
                List<ReplayReportWriter.ReplayEvent> replayEvents = new ArrayList<>();
                for (String[] row : csvSnapshot) {
                    replayEvents.add(new ReplayReportWriter.ReplayEvent(
                            Long.parseLong(row[1]), Long.parseLong(row[2]), Long.parseLong(row[3]),
                            "true".equals(row[5]), row[6], row[7], row[8]));
                }
                long originalDurationMs = events.stream().mapToLong(e -> e.delayMs).sum();
                String htmlTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String htmlBaseName = new File(jsonlPath).getName().replace(".jsonl", "");
                String htmlPath = String.format("results/composite_replay_%s_%s_%.0fx.html",
                        htmlBaseName, htmlTimestamp, speedFactor);
                String savedPath = ReplayReportWriter.saveHtml("Composite Replay: " + htmlBaseName,
                        replayEvents, speedFactor, totalDuration, originalDurationMs, htmlPath);
                log("HTML report saved to: " + savedPath);
            } catch (IOException e) {
                System.err.println("Failed to save HTML report: " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // Request Routing
    // =========================================================================

    private void fireRequest(TrafficEvent event, CountDownLatch latch, long scheduledDelayMs) {
        long startTime = System.currentTimeMillis();
        long actualTimeOffset = startTime - replayStartTime;
        String endpointKey = event.method + " " + event.endpoint;

        CompletableFuture<Void> future;
        try {
            if ("pmm".equals(event.source)) {
                future = firePmmRequest(event);
            } else if ("pplmag".equals(event.source)) {
                future = firePplmagRequest(event);
            } else {
                if (verbose)
                    log("Unknown source: " + event.source);
                addCsvRow(event.index, scheduledDelayMs, actualTimeOffset, 0, false, endpointKey, event.userId,
                        "Unknown source");
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
                    if (verbose)
                        log(String.format("FAIL [%s] %s %s: %s (%.0fms)",
                                event.source, event.method, event.endpoint, error.getMessage(), (double) latency));
                } else {
                    successCount.incrementAndGet();
                    addCsvRow(event.index, scheduledDelayMs, actualOffset, latency, true, endpointKey, event.userId,
                            "");
                    if (verbose)
                        log(String.format("OK [%s] %s %s (%.0fms)",
                                event.source, event.method, event.endpoint, (double) latency));
                }
                latch.countDown();
            });
        } catch (Exception e) {
            failCount.incrementAndGet();
            addCsvRow(event.index, scheduledDelayMs, actualTimeOffset, 0, false, endpointKey, event.userId,
                    e.getMessage());
            if (verbose)
                log(String.format("ERROR [%s] %s %s: %s", event.source, event.method, event.endpoint, e.getMessage()));
            latch.countDown();
        }
    }

    // =========================================================================
    // PMM Request Handlers
    // =========================================================================

    private CompletableFuture<Void> firePmmRequest(TrafficEvent event) {
        UserSession session = pmmSessions.get(event.userId);
        return switch (event.endpoint) {
            case "/date-picker" -> firePmmDatePickerAsync(event.userId);
            case "/postPickerStatus" -> firePmmPostPickerStatusAsync(session);
            case "/crossword" -> firePmmCrosswordAsync(event.userId, session);
            case "/api/v1/plays" -> firePmmPostPlaysAsync(event.userId, session);
            default -> {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new IOException("Unknown PMM endpoint: " + event.endpoint));
                yield f;
            }
        };
    }

    private CompletableFuture<Void> firePmmDatePickerAsync(String userId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        HttpUrl url = HttpUrl.parse(BASE_URL + "date-picker").newBuilder()
                .addQueryParameter("id", userId)
                .addQueryParameter("set", SET_PARAM)
                .build();
        Request request = new Request.Builder().url(url).addHeader("User-Agent", "Mozilla/5.0").get().build();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> firePmmPostPickerStatusAsync(UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (session == null || session.loadToken == null || session.loadToken.isEmpty()) {
            future.completeExceptionally(new IOException("No session"));
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
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> firePmmCrosswordAsync(String userId, UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        HttpUrl url = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", userId)
                .addQueryParameter("set", SET_PARAM)
                .build();
        Request request = new Request.Builder().url(url).addHeader("User-Agent", "Mozilla/5.0").get().build();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> firePmmPostPlaysAsync(String userId, UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (session == null || session.loadToken == null || session.loadToken.isEmpty()) {
            future.completeExceptionally(new IOException("No session"));
            return future;
        }
        long ts = System.currentTimeMillis();
        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", session.loadToken);
        payload.addProperty("updatePlayTable", true);
        payload.addProperty("updateLoadTable", false);
        payload.addProperty("series", SET_PARAM);
        payload.addProperty("playId", session.playId != null ? session.playId : "");
        payload.addProperty("userId", userId);
        payload.addProperty("timestamp", ts);
        payload.addProperty("playState", 2);
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
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    // =========================================================================
    // Pplmag Request Handlers
    // =========================================================================

    private CompletableFuture<Void> firePplmagRequest(TrafficEvent event) {
        String comboKey = pplmagSessionKey(event.userId, event.puzzleId != null ? event.puzzleId : "");
        UserSession session = pplmagSessions.get(comboKey);

        if (event.endpoint.equals("/api/v1/plays") && "POST".equals(event.method)) {
            return firePplmagPostPlaysAsync(event.userId, session, event.puzzleId, event.series);
        } else if (event.endpoint.equals("/api/v1/plays") && "GET".equals(event.method)) {
            return firePplmagGetPlaysAsync(event.offset, event.puzzleId);
        } else if (event.endpoint.equals("/api/v1/puzzles") && "GET".equals(event.method)) {
            return firePplmagGetPuzzlesAsync(event.offset);
        } else if (event.endpoint.equals("/crossword")) {
            return firePplmagCrosswordAsync(event.userId);
        } else if (event.endpoint.equals("/postPickerStatus")) {
            return firePplmagPostPickerStatusAsync(session);
        } else {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(new IOException("Unknown Pplmag endpoint: " + event.endpoint));
            return f;
        }
    }

    private static final Random RANDOM = new Random();

    private String randomDigitString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            sb.append(RANDOM.nextInt(10));
        return sb.toString();
    }

    private CompletableFuture<Void> firePplmagPostPlaysAsync(String userId, UserSession session, String puzzleId,
            String series) {
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
        payload.addProperty("timestamp", ts);
        payload.addProperty("playState", 2);
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
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> firePplmagGetPlaysAsync(Integer offset, String puzzleIds) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            HttpUrl.Builder urlBuilder = HttpUrl.parse(BASE_URL + "api/v1/plays").newBuilder()
                    .addQueryParameter("series", SET_PARAM)
                    .addQueryParameter("limit", "14")
                    .addQueryParameter("offset", offset != null ? String.valueOf(offset) : "0");
            if (puzzleIds != null && !puzzleIds.isEmpty()) {
                urlBuilder.addQueryParameter("puzzleIds", puzzleIds);
            }
            Request request = new Request.Builder()
                    .url(urlBuilder.build())
                    .addHeader("Authorization", "Bearer " + getAccessToken())
                    .get().build();
            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    future.completeExceptionally(e);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (response) {
                        if (!response.isSuccessful())
                            future.completeExceptionally(new IOException("HTTP " + response.code()));
                        else
                            future.complete(null);
                    }
                }
            });
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<Void> firePplmagGetPuzzlesAsync(Integer offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            HttpUrl url = HttpUrl.parse(BASE_URL + "api/v1/puzzles").newBuilder()
                    .addQueryParameter("series", SET_PARAM)
                    .addQueryParameter("limit", "14")
                    .addQueryParameter("offset", offset != null ? String.valueOf(offset) : "0")
                    .build();
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", "Bearer " + getAccessToken())
                    .get().build();
            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    future.completeExceptionally(e);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (response) {
                        if (!response.isSuccessful())
                            future.completeExceptionally(new IOException("HTTP " + response.code()));
                        else
                            future.complete(null);
                    }
                }
            });
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<Void> firePplmagCrosswordAsync(String userId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        HttpUrl url = HttpUrl.parse(BASE_URL + "crossword").newBuilder()
                .addQueryParameter("id", userId)
                .addQueryParameter("set", SET_PARAM)
                .build();
        Request request = new Request.Builder().url(url).addHeader("User-Agent", "Mozilla/5.0").get().build();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    private CompletableFuture<Void> firePplmagPostPickerStatusAsync(UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (session == null || session.loadToken == null || session.loadToken.isEmpty()) {
            future.completeExceptionally(new IOException("No session"));
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
                    if (!response.isSuccessful())
                        future.completeExceptionally(new IOException("HTTP " + response.code()));
                    else
                        future.complete(null);
                }
            }
        });
        return future;
    }

    // =========================================================================
    // CSV Output
    // =========================================================================

    private void addCsvRow(int index, long scheduledMs, long actualMs, long latencyMs, boolean success,
            String endpoint, String userId, String error) {
        long responseMs = actualMs + latencyMs;
        csvRows.add(new String[] {
                String.valueOf(index), String.valueOf(scheduledMs), String.valueOf(actualMs),
                String.valueOf(latencyMs), String.valueOf(responseMs), success ? "true" : "false",
                endpoint, userId != null ? userId : "", error
        });
        lastResponseTimeMs.updateAndGet(current -> Math.max(current, responseMs));
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

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
    // Main
    // =========================================================================

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: CompositeReplayExecutor <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --speed <factor>   Speed factor (default: 1.0)");
            System.out.println("  --dry-run          Don't actually send requests");
            System.out.println("  --html             Generate HTML report (default: on)");
            System.out.println("  --no-html          Disable HTML report");
            System.out.println("  -v, --verbose      Verbose output (default: on)");
            System.out.println("  -q, --quiet        Quiet output");
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
                case "--speed" -> speedFactor = Double.parseDouble(args[++i]);
                case "--dry-run" -> dryRun = true;
                case "--html" -> html = true;
                case "--no-html" -> html = false;
                case "-v", "--verbose" -> verbose = true;
                case "-q", "--quiet" -> verbose = false;
            }
        }

        CompositeReplayExecutor executor = null;
        try {
            executor = new CompositeReplayExecutor(jsonlPath, speedFactor, dryRun, verbose, html);
            executor.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (executor != null)
                executor.shutdown();
        }
    }
}
