package com.perftest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import okhttp3.*;

import javax.net.ssl.*;
import java.io.*;
import java.lang.reflect.Type;
import java.security.cert.X509Certificate;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Traffic Replay Executor - Replays production traffic from JSONL file.
 * 
 * Features:
 * - Parallel pre-warm (10 threads) for faster session setup
 * - CSV output for detailed results
 * - HTML report generation from CSV
 * - Accurate per-request timing
 * 
 * Usage:
 * java -jar traffic-replay.jar <jsonl-file> --speed 5 --html -v
 */
public class TrafficReplayExecutor {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
    private static final Gson GSON = new Gson();

    // Configurable via CLI
    private final String baseUrl;

    // Hardcoded config (matching ApiConfig)
    private static final String SET_PARAM = "malhar-1";
    private static final String PUZZLE_ID = "7a3720d7";

    private static final int PREWARM_THREADS = 100;

    // CDN resource paths (relative to cdnBaseUrl/cdnPath/cdnCommit/)
    private static final String[] DATE_PICKER_CDN_PATHS = {
            "css/date-picker-min.css",
            "js/picker-min.js"
    };

    private static final String[] CROSSWORD_CDN_PATHS = {
            "css/crossword-player-min.css",
            "js/c-min.js"
    };

    // External CDN resources (fixed URLs, e.g., font-awesome)
    private static final String[] EXTERNAL_CDN_URLS = {
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/css/all.min.css",
            "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.0/webfonts/fa-solid-900.woff2"
    };

    private final String jsonlPath;
    private final double speedFactor;
    private final boolean verbose;
    private final boolean dryRun;
    private final boolean generateHtml;
    private final boolean saveSessions;
    private final boolean loadSessions;
    private final boolean noSsl;
    private final String sessionsFile; // Computed from input filename

    // CDN configuration
    private final boolean fetchCdn;
    private final String cdnEnv; // e.g., "cdn-test" or "cdn-prod"
    private final String cdnPath;
    private final String cdnCommit;

    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService prewarmExecutor;

    // Per-user session state
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
    private static class UserSession {
        String loadToken;
        String playId;

        UserSession(String loadToken, String playId) {
            this.loadToken = loadToken;
            this.playId = playId;
        }
    }

    /**
     * Traffic event from JSONL.
     */
    private static class TrafficEvent {
        int index;
        long ts;
        String endpoint;
        String userId;
        long delayMs;
        int isLastReq;
    }

    public TrafficReplayExecutor(String jsonlPath, String baseUrl, double speedFactor, boolean verbose, boolean dryRun,
            boolean generateHtml, boolean saveSessions, boolean loadSessions, boolean noSsl,
            boolean fetchCdn, String cdnEnv, String cdnPath, String cdnCommit) {
        this.jsonlPath = jsonlPath;
        this.baseUrl = baseUrl;
        this.speedFactor = speedFactor;
        this.verbose = verbose;
        this.dryRun = dryRun;
        this.generateHtml = generateHtml;
        this.saveSessions = saveSessions;
        this.loadSessions = loadSessions;
        this.noSsl = noSsl;

        // CDN configuration
        this.fetchCdn = fetchCdn;
        this.cdnEnv = cdnEnv;
        this.cdnPath = cdnPath;
        this.cdnCommit = cdnCommit;

        // Compute sessions filename from input file: traffic_final_10min.jsonl ->
        // sessions_traffic_final_10min.json
        String baseName = Paths.get(jsonlPath).getFileName().toString().replaceFirst("\\.jsonl?$", "");
        this.sessionsFile = "sessions_" + baseName + ".json";

        // Configure dispatcher to allow more concurrent requests per host
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(200);
        dispatcher.setMaxRequestsPerHost(100);

        this.client = noSsl ? createInsecureClient(dispatcher) : createSecureClient(dispatcher);

        this.scheduler = Executors.newScheduledThreadPool(20);
        this.prewarmExecutor = Executors.newFixedThreadPool(PREWARM_THREADS);
    }

    /**
     * Create a secure OkHttpClient with normal SSL certificate validation.
     */
    private OkHttpClient createSecureClient(Dispatcher dispatcher) {
        return new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(100, 5, TimeUnit.MINUTES))
                .dispatcher(dispatcher)
                .build();
    }

    /**
     * Create an insecure OkHttpClient that trusts all SSL certificates.
     * WARNING: Only use for testing with self-signed certificates!
     */
    private OkHttpClient createInsecureClient(Dispatcher dispatcher) {
        try {
            // Create a trust manager that trusts all certificates
            final TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                        // Trust all clients
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                        // Trust all servers
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            return new OkHttpClient.Builder()
                    .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                    .hostnameVerifier((hostname, session) -> true) // Trust all hostnames
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .connectionPool(new ConnectionPool(100, 5, TimeUnit.MINUTES))
                    .dispatcher(dispatcher)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create insecure SSL client", e);
        }
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[Replay] " + msg);
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
        return null;
    }

    /**
     * Pre-warm: Fetch loadToken and playId for a user.
     */
    private UserSession prewarmUser(String userId) throws IOException {
        // Step 1: GET date-picker to get loadToken
        HttpUrl datePickerUrl = HttpUrl.parse(baseUrl + "date-picker").newBuilder()
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

        // Step 3: GET crossword to get playId
        String srcUrl = String.format("%sdate-picker?set=%s&uid=%s", baseUrl, SET_PARAM, userId);

        HttpUrl crosswordUrl = HttpUrl.parse(baseUrl + "crossword").newBuilder()
                .addQueryParameter("id", PUZZLE_ID)
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

    /**
     * Fire a request based on the traffic event (async).
     * The latch is decremented when the async request completes.
     */
    private void fireRequest(TrafficEvent event, long scheduledTimeMs, CountDownLatch latch) {
        totalEvents.incrementAndGet();
        long actualStartTime = System.currentTimeMillis();
        long actualTimeOffset = actualStartTime - replayStartTime;
        String endpoint = event.endpoint;
        String userId = event.userId;

        UserSession session = userId != null ? userSessions.get(userId) : null;

        if (dryRun) {
            log(String.format("[DRY-RUN] %s for user %s", endpoint, userId));
            successCount.incrementAndGet();
            addCsvRow(event.index, scheduledTimeMs, actualTimeOffset, 1, true, endpoint, userId, "");
            latch.countDown();
            return;
        }

        long startNanos = System.nanoTime();

        // Get the appropriate async method
        CompletableFuture<Void> requestFuture;
        try {
            requestFuture = switch (endpoint) {
                case "/date-picker" -> {
                    CompletableFuture<Void> main = fireGetDatePickerAsync(userId);
                    if (fetchCdn) {
                        yield main.thenCompose(v -> fetchCdnResourcesAsync(DATE_PICKER_CDN_PATHS, EXTERNAL_CDN_URLS));
                    }
                    yield main;
                }
                case "/postPickerStatus" -> firePostPickerStatusAsync(session);
                case "/crossword" -> {
                    CompletableFuture<Void> main = fireGetCrosswordAsync(userId, session);
                    if (fetchCdn) {
                        yield main.thenCompose(v -> fetchCdnResourcesAsync(CROSSWORD_CDN_PATHS, null));
                    }
                    yield main;
                }
                case "/api/v1/plays" -> firePostPlaysAsync(userId, session);
                default -> {
                    log("Unknown endpoint: " + endpoint);
                    yield CompletableFuture.completedFuture(null);
                }
            };
        } catch (Exception e) {
            // Handle immediate exceptions (e.g., null session)
            long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
            failCount.incrementAndGet();
            String error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            log(String.format("FAIL %s user=%s: %s", endpoint, userId, error));
            addCsvRow(event.index, scheduledTimeMs, actualTimeOffset, latencyMs, false, endpoint, userId, error);
            latch.countDown();
            return;
        }

        // Handle async completion
        requestFuture.whenComplete((result, ex) -> {
            long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
            boolean success = (ex == null);
            String error = "";

            if (success) {
                successCount.incrementAndGet();
                totalLatencyMs.addAndGet(latencyMs);
                if (verbose) {
                    log(String.format("OK %s user=%s latency=%dms", endpoint, userId, latencyMs));
                }
            } else {
                failCount.incrementAndGet();
                error = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
                log(String.format("FAIL %s user=%s: %s", endpoint, userId, error));
            }

            addCsvRow(event.index, scheduledTimeMs, actualTimeOffset, latencyMs, success, endpoint, userId, error);
            latch.countDown();
        });
    }

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

        // Track the latest response completion time
        lastResponseTimeMs.updateAndGet(current -> Math.max(current, responseMs));
    }

    private CompletableFuture<Void> fireGetDatePickerAsync(String userId) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        HttpUrl url = HttpUrl.parse(baseUrl + "date-picker").newBuilder()
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("uid", userId)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0")
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
     * Build a CDN URL from the configurable components.
     * Format: https://{cdnEnv}.amuselabs.com/{cdnPath}/{cdnCommit}/{path}
     */
    private String buildCdnUrl(String path) {
        return String.format("https://%s.amuselabs.com/%s/%s/%s", cdnEnv, cdnPath, cdnCommit, path);
    }

    /**
     * Fetch CDN resources asynchronously in parallel.
     * 
     * @param paths        Relative paths to fetch (will be prefixed with CDN base)
     * @param externalUrls External URLs to fetch as-is
     * @return CompletableFuture that completes when all CDN fetches are done
     */
    private CompletableFuture<Void> fetchCdnResourcesAsync(String[] paths, String[] externalUrls) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Fetch relative CDN paths
        for (String path : paths) {
            String url = buildCdnUrl(path);
            futures.add(fetchSingleCdnResourceAsync(url));
        }

        // Fetch external URLs
        if (externalUrls != null) {
            for (String url : externalUrls) {
                futures.add(fetchSingleCdnResourceAsync(url));
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Fetch a single CDN resource asynchronously.
     */
    private CompletableFuture<Void> fetchSingleCdnResourceAsync(String url) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        long startNanos = System.nanoTime();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("User-Agent", "Mozilla/5.0")
                .get()
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
                if (verbose) {
                    log(String.format("CDN FAIL %s: %s (%.0fms)", url, e.getMessage(), (double) latencyMs));
                }
                // Don't fail the main request if CDN fails
                future.complete(null);
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
                    // Consume body to complete transfer
                    if (response.body() != null) {
                        response.body().bytes();
                    }
                    if (verbose) {
                        String shortUrl = url.substring(url.lastIndexOf('/') + 1);
                        if (shortUrl.contains("?")) {
                            shortUrl = shortUrl.substring(0, shortUrl.indexOf('?'));
                        }
                        log(String.format("CDN OK %s: %dms", shortUrl, latencyMs));
                    }
                    future.complete(null);
                } catch (IOException e) {
                    future.complete(null);
                }
            }
        });

        return future;
    }

    private CompletableFuture<Void> firePostPickerStatusAsync(UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (session == null || session.loadToken == null) {
            future.completeExceptionally(new IOException("No session for postPickerStatus"));
            return future;
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", session.loadToken);
        payload.addProperty("isVerified", true);
        payload.addProperty("adDuration", 0);
        payload.addProperty("reason", "displaying puzzle picker");

        Request request = new Request.Builder()
                .url(baseUrl + "postPickerStatus")
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

    private CompletableFuture<Void> fireGetCrosswordAsync(String userId, UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        String loadToken = session != null ? session.loadToken : "";
        String srcUrl = String.format("%sdate-picker?set=%s&uid=%s", baseUrl, SET_PARAM, userId);

        HttpUrl url = HttpUrl.parse(baseUrl + "crossword").newBuilder()
                .addQueryParameter("id", PUZZLE_ID)
                .addQueryParameter("set", SET_PARAM)
                .addQueryParameter("picker", "date-picker")
                .addQueryParameter("src", srcUrl)
                .addQueryParameter("uid", userId)
                .addQueryParameter("loadToken", loadToken)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .addHeader("User-Agent", "Mozilla/5.0")
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

    private CompletableFuture<Void> firePostPlaysAsync(String userId, UserSession session) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (session == null || session.loadToken == null) {
            future.completeExceptionally(new IOException("No session for plays"));
            return future;
        }

        long ts = System.currentTimeMillis();
        JsonObject payload = new JsonObject();
        payload.addProperty("loadToken", session.loadToken);
        payload.addProperty("updatePlayTable", true);
        payload.addProperty("updateLoadTable", false);
        payload.addProperty("series", SET_PARAM);
        payload.addProperty("id", PUZZLE_ID);
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
                .url(baseUrl + "api/v1/plays")
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

    private String randomDigitString(int length) {
        StringBuilder sb = new StringBuilder(length);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }

    /**
     * Save user sessions to JSON file for later reuse.
     */
    private void saveSessionsToFile() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(sessionsFile))) {
            Map<String, Map<String, String>> sessionsMap = new HashMap<>();
            for (Map.Entry<String, UserSession> entry : userSessions.entrySet()) {
                Map<String, String> sessionData = new HashMap<>();
                sessionData.put("loadToken", entry.getValue().loadToken);
                sessionData.put("playId", entry.getValue().playId);
                sessionsMap.put(entry.getKey(), sessionData);
            }
            writer.write(GSON.toJson(sessionsMap));
        } catch (IOException e) {
            System.err.println("Failed to save sessions: " + e.getMessage());
        }
    }

    /**
     * Load user sessions from JSON file.
     * 
     * @return true if sessions were loaded successfully
     */
    private boolean loadSessionsFromFile() {
        File sessionFile = new File(sessionsFile);
        if (!sessionFile.exists()) {
            return false;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(sessionFile))) {
            Type type = new TypeToken<Map<String, Map<String, String>>>() {
            }.getType();
            Map<String, Map<String, String>> sessionsMap = GSON.fromJson(reader, type);
            if (sessionsMap == null || sessionsMap.isEmpty()) {
                return false;
            }
            for (Map.Entry<String, Map<String, String>> entry : sessionsMap.entrySet()) {
                String userId = entry.getKey();
                Map<String, String> sessionData = entry.getValue();
                userSessions.put(userId, new UserSession(
                        sessionData.get("loadToken"),
                        sessionData.get("playId")));
            }
            return true;
        } catch (Exception e) {
            System.err.println("Failed to load sessions: " + e.getMessage());
            return false;
        }
    }

    /**
     * Save results to CSV file.
     */
    private String saveCsv(String baseName) throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String csvPath = String.format("results/replay_%s_%s.csv", baseName, timestamp);

        try (PrintWriter writer = new PrintWriter(new FileWriter(csvPath))) {
            writer.println("index,scheduledMs,actualMs,latencyMs,responseMs,success,endpoint,userId,error");
            for (String[] row : csvRows) {
                writer.println(String.join(",", row));
            }
        }

        return csvPath;
    }

    /**
     * Main execution.
     */
    public Map<String, Object> execute() throws IOException {
        log("Starting traffic replay from: " + jsonlPath);
        log(String.format("Speed factor: %.1fx, Dry run: %s, HTML: %s", speedFactor, dryRun, generateHtml));

        // Phase 1: Scan JSONL for unique users
        log("Phase 1: Scanning for unique users...");
        Set<String> uniqueUsers = ConcurrentHashMap.newKeySet();
        List<TrafficEvent> events = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonlPath))) {
            String line;
            int index = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty())
                    continue;
                try {
                    JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                    TrafficEvent event = new TrafficEvent();
                    event.index = index++;
                    event.ts = obj.get("ts").getAsLong();
                    event.endpoint = obj.get("endpoint").getAsString();
                    event.userId = obj.has("userId") && !obj.get("userId").isJsonNull()
                            ? obj.get("userId").getAsString()
                            : null;
                    event.delayMs = obj.get("delayMs").getAsLong();
                    event.isLastReq = obj.has("isLastReq") ? obj.get("isLastReq").getAsInt() : 0;

                    events.add(event);
                    if (event.userId != null) {
                        uniqueUsers.add(event.userId);
                    }
                } catch (Exception e) {
                    // Skip malformed lines
                }
            }
        }

        log(String.format("Found %d events and %d unique users", events.size(), uniqueUsers.size()));

        // Phase 2: Load cached sessions or pre-warm users
        if (!dryRun && !uniqueUsers.isEmpty()) {
            if (loadSessions && loadSessionsFromFile()) {
                log(String.format("Loaded %d cached sessions from %s", userSessions.size(), sessionsFile));
            } else {
                // Pre-warm users in parallel
                log(String.format("Phase 2: Pre-warming %d users with %d threads...", uniqueUsers.size(),
                        PREWARM_THREADS));
                long prewarmStart = System.currentTimeMillis();

                AtomicInteger warmed = new AtomicInteger(0);
                AtomicInteger failed = new AtomicInteger(0);
                AtomicLong totalPrewarmTimeMs = new AtomicLong(0);

                List<Future<?>> futures = new ArrayList<>();
                for (String userId : uniqueUsers) {
                    futures.add(prewarmExecutor.submit(() -> {
                        long userStart = System.currentTimeMillis();
                        try {
                            UserSession session = prewarmUser(userId);
                            userSessions.put(userId, session);
                            long userDuration = System.currentTimeMillis() - userStart;
                            totalPrewarmTimeMs.addAndGet(userDuration);
                            int count = warmed.incrementAndGet();

                            // Progress with analytics every 50 users
                            if (count % 50 == 0) {
                                double avgMs = (double) totalPrewarmTimeMs.get() / count;
                                int remaining = uniqueUsers.size() - count - failed.get();
                                double etaSeconds = (remaining * avgMs / 1000.0) / PREWARM_THREADS;
                                System.out.printf("\r[Prewarm] %d/%d users | avg: %.0fms/user | ETA: %.0fs   ",
                                        count, uniqueUsers.size(), avgMs, etaSeconds);
                            }
                        } catch (Exception e) {
                            failed.incrementAndGet();
                            if (verbose) {
                                log(String.format("Failed to pre-warm user %s: %s", userId, e.getMessage()));
                            }
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
                log(String.format("Pre-warmed %d users (%d failed) in %.1f seconds (avg %.0fms/user)",
                        warmed.get(), failed.get(), prewarmDuration / 1000.0, avgPrewarmMs));

                // Save sessions if requested
                if (saveSessions) {
                    saveSessionsToFile();
                    log(String.format("Saved %d sessions to %s", userSessions.size(), sessionsFile));
                }
            }
        }

        // Phase 3: Schedule and fire requests
        log("Phase 3: Starting replay...");
        replayStartTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(events.size());

        long cumulativeDelayMs = 0;
        for (TrafficEvent event : events) {
            long scaledDelay = (long) (cumulativeDelayMs / speedFactor);

            final long scheduledTime = scaledDelay;
            // Latch is now passed to fireRequest() and decremented in async callback
            scheduler.schedule(() -> fireRequest(event, scheduledTime, latch), scaledDelay, TimeUnit.MILLISECONDS);

            cumulativeDelayMs += event.delayMs;
        }

        // Store original traffic duration (before speed adjustment)
        final long originalDurationMs = cumulativeDelayMs;

        // Wait for completion
        // Add extra time: scheduled duration + buffer for in-flight requests (avg
        // latency * concurrent requests factor)
        long scheduledDurationMs = (long) (cumulativeDelayMs / speedFactor);
        // At high speeds, requests may take longer than the schedule gap, so add
        // substantial buffer
        long maxLatencyBuffer = 120_000; // 2 min buffer for in-flight requests to complete
        long expectedDurationMs = scheduledDurationMs + maxLatencyBuffer;

        log(String.format("Waiting up to %.1f seconds for completion (scheduled: %.1fs + buffer: %.1fs)...",
                expectedDurationMs / 1000.0, scheduledDurationMs / 1000.0, maxLatencyBuffer / 1000.0));

        try {
            boolean completed = latch.await(expectedDurationMs, TimeUnit.MILLISECONDS);
            if (!completed) {
                int remaining = (int) latch.getCount();
                System.err.println(
                        String.format("Warning: Timeout! %d/%d requests did not complete.", remaining, events.size()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long replayEndTime = System.currentTimeMillis();
        long actualDurationMs = replayEndTime - replayStartTime;

        // Print summary
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TRAFFIC REPLAY SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("  Source file:      %s%n", jsonlPath);
        System.out.printf("  Speed factor:     %.1fx%n", speedFactor);
        System.out.printf("  Original traffic: %.1f seconds%n", originalDurationMs / 1000.0);
        System.out.printf("  Scheduled replay: %.1f seconds (expected at %.1fx)%n", scheduledDurationMs / 1000.0,
                speedFactor);
        System.out.printf("  Actual execution: %.1f seconds (wall-clock)%n", actualDurationMs / 1000.0);
        System.out.println("-".repeat(60));
        System.out.printf("  Total events:     %d%n", totalEvents.get());
        System.out.printf("  Successful:       %d%n", successCount.get());
        System.out.printf("  Failed:           %d%n", failCount.get());
        System.out.printf("  Avg latency:      %.1f ms%n",
                successCount.get() > 0 ? (double) totalLatencyMs.get() / successCount.get() : 0);
        System.out.println("=".repeat(60));

        // Save CSV
        String baseName = Paths.get(jsonlPath).getFileName().toString().replace(".jsonl", "");
        String csvPath = null;
        try {
            csvPath = saveCsv(baseName);
            System.out.println("\nCSV results saved to: " + csvPath);
        } catch (IOException e) {
            System.err.println("Failed to save CSV: " + e.getMessage());
        }

        // Generate HTML report from CSV data
        if (generateHtml && !csvRows.isEmpty()) {
            try {
                // Convert CSV rows to ReplayEvents
                // CSV columns:
                // index,scheduledMs,actualMs,latencyMs,responseMs,success,endpoint,userId,error
                List<ReplayReportWriter.ReplayEvent> replayEvents = new ArrayList<>();
                for (String[] row : csvRows) {
                    replayEvents.add(new ReplayReportWriter.ReplayEvent(
                            Long.parseLong(row[1]), // scheduledMs
                            Long.parseLong(row[2]), // actualMs
                            Long.parseLong(row[3]), // latencyMs
                            // row[4] is responseMs (already computed, skip for ReplayEvent)
                            "true".equals(row[5]), // success
                            row[6], // endpoint
                            row[7], // userId
                            row[8] // error
                    ));
                }

                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                // If CSV was saved, try to reuse its timestamp to match
                if (csvPath != null && csvPath.contains("_")) {
                    // Extract timestamp from csvPath: results/replay_base_TIMESTAMP.csv
                    // Simple regex or string manipulation
                    int lastUnderscore = csvPath.lastIndexOf('_');
                    int dotCsv = csvPath.lastIndexOf(".csv");
                    if (lastUnderscore > 0 && dotCsv > lastUnderscore) {
                        timestamp = csvPath.substring(lastUnderscore + 1, dotCsv);
                    }
                }

                String htmlPath = String.format("results/replay_%s_%s_%.0fx.html", baseName, timestamp, speedFactor);
                String savedPath = ReplayReportWriter.saveHtml(
                        "Traffic Replay: " + baseName,
                        replayEvents,
                        speedFactor,
                        actualDurationMs,
                        originalDurationMs,
                        htmlPath);
                System.out.println("HTML report saved to: " + savedPath);
            } catch (IOException e) {
                System.err.println("Failed to save HTML report: " + e.getMessage());
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("total_events", totalEvents.get());
        result.put("success_count", successCount.get());
        result.put("fail_count", failCount.get());
        result.put("duration_ms", actualDurationMs);
        result.put("csv_path", csvPath);
        return result;
    }

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

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: TrafficReplayExecutor <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --base-url <url>       Base URL for the server (default: https://cdn-test.amuselabs.com/pmm/)");
            System.out.println("  --speed <factor>       Speed factor (default: 1.0, e.g., 5.0 = 5x faster)");
            System.out.println("  --dry-run              Don't actually send requests");
            System.out.println("  --html                 Generate HTML report");
            System.out.println("  --save-sessions        Pre-warm users and save sessions to sessions.json");
            System.out.println("  --load-sessions        Load sessions from sessions.json (skip pre-warming)");
            System.out.println("  --no-ssl               Disable SSL certificate validation (for self-signed certs)");
            System.out.println("  --fetch-cdn            Fetch CDN resources after date-picker and crossword");
            System.out.println("  --cdn-env <env>        CDN environment prefix (default: cdn-test)");
            System.out.println("  --cdn-path <path>      CDN path prefix (default: pmm)");
            System.out.println("  --cdn-commit <id>      CDN commit ID (default: dd97891)");
            System.out.println("  -v, --verbose          Verbose output");
            return;
        }

        String jsonlPath = args[0];
        String baseUrl = "https://cdn-test.amuselabs.com/pmm/"; // default
        double speedFactor = 1.0;
        boolean verbose = false;
        boolean dryRun = false;
        boolean html = false;
        boolean saveSessions = false;
        boolean loadSessions = false;
        boolean noSsl = false;
        boolean fetchCdn = false;
        String cdnEnv = "cdn-test";
        String cdnPath = "pmm";
        String cdnCommit = "dd97891";

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--base-url":
                    baseUrl = args[++i];
                    break;
                case "--speed":
                    speedFactor = Double.parseDouble(args[++i]);
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--html":
                    html = true;
                    break;
                case "--save-sessions":
                    saveSessions = true;
                    break;
                case "--load-sessions":
                    loadSessions = true;
                    break;
                case "--no-ssl":
                    noSsl = true;
                    break;
                case "--fetch-cdn":
                    fetchCdn = true;
                    break;
                case "--cdn-env":
                    cdnEnv = args[++i];
                    break;
                case "--cdn-path":
                    cdnPath = args[++i];
                    break;
                case "--cdn-commit":
                    cdnCommit = args[++i];
                    break;
                case "-v":
                case "--verbose":
                    verbose = true;
                    break;
            }
        }

        TrafficReplayExecutor executor = new TrafficReplayExecutor(jsonlPath, baseUrl, speedFactor, verbose, dryRun, html,
                saveSessions, loadSessions, noSsl, fetchCdn, cdnEnv, cdnPath, cdnCommit);
        try {
            executor.execute();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }
}
