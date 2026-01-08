package com.perftest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replay Executor - Replays traffic from a JSONL file with timing control.
 * 
 * Reads events from a JSONL file (produced by log_distiller.py) and fires
 * HTTP requests at the appropriate times, scaled by a speed factor.
 * 
 * Usage:
 * ReplayExecutor executor = new ReplayExecutor(jsonlPath, baseUrl, speedFactor,
 * verbose, dryRun, generateHtml);
 * executor.execute();
 */
public class ReplayExecutor {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
    private static final Gson GSON = new Gson();

    private final String jsonlPath;
    private final String baseUrl;
    private final double speedFactor;
    private final boolean verbose;
    private final boolean dryRun;
    private final boolean generateHtml;

    private final OkHttpClient client;
    private final ScheduledExecutorService scheduler;
    private ExecutorService workerPool; // Initialized dynamically based on max concurrency
    private SessionManager sessionManager; // For dynamic token fetching

    // Stats
    private final AtomicInteger totalEvents = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);

    // Event tracking for HTML report
    private final List<ReplayReportWriter.ReplayEvent> replayEvents = Collections.synchronizedList(new ArrayList<>());
    private volatile long replayStartTime;

    public ReplayExecutor(String jsonlPath, String baseUrl, double speedFactor,
            boolean verbose, boolean dryRun, boolean generateHtml) {
        this.jsonlPath = jsonlPath;
        this.baseUrl = baseUrl;
        this.speedFactor = speedFactor;
        this.verbose = verbose;
        this.dryRun = dryRun;
        this.generateHtml = generateHtml;

        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(200, 5, TimeUnit.MINUTES))
                .build();

        // Scheduler just for timing (small pool - threads don't block)
        this.scheduler = Executors.newScheduledThreadPool(2);
        // Worker pool initialized in execute() after calculating max concurrency
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[Replay] " + msg);
        }
    }

    /**
     * Main execution method. Reads the JSONL file and schedules all requests.
     */
    public Map<String, Object> execute() throws IOException {
        log("Starting replay from: " + jsonlPath);
        log(String.format("Speed factor: %.1fx, Base URL: %s, Dry run: %s, HTML: %s",
                speedFactor, baseUrl, dryRun, generateHtml));

        List<JsonObject> events = loadEvents();
        if (events.isEmpty()) {
            log("No events to replay!");
            return Map.of("success", false, "error", "No events loaded");
        }

        // Sort events by timestamp
        events.sort(Comparator.comparingLong(e -> e.get("ts").getAsLong()));

        long firstTs = events.get(0).get("ts").getAsLong();
        long lastTs = events.get(events.size() - 1).get("ts").getAsLong();
        long originalDurationMs = lastTs - firstTs;

        log(String.format("Loaded %d events spanning %.1f minutes", events.size(), originalDurationMs / 60000.0));
        log(String.format("Replay will take approximately %.1f minutes at %.1fx speed",
                (originalDurationMs / speedFactor) / 60000.0, speedFactor));

        replayStartTime = System.currentTimeMillis();

        // Calculate max concurrency and initialize worker pool
        int maxConcurrency = calculateMaxConcurrency(events, firstTs);
        log(String.format("Calculated max concurrency: %d threads", maxConcurrency));
        this.workerPool = Executors.newFixedThreadPool(maxConcurrency);

        // Initialize session manager for lazy token fetching (skip for dry runs)
        if (!dryRun) {
            this.sessionManager = new SessionManager(client, baseUrl, verbose);
            log("SessionManager ready (lazy init mode - tokens fetched on demand)");
        }

        // Schedule all events
        CountDownLatch latch = new CountDownLatch(events.size());

        for (JsonObject event : events) {
            long eventTs = event.get("ts").getAsLong();
            long scheduledDelayMs = (long) ((eventTs - firstTs) / speedFactor);

            scheduler.schedule(() -> {
                // Hand off to worker pool immediately (scheduler thread doesn't block)
                workerPool.submit(() -> {
                    try {
                        fireRequest(event, scheduledDelayMs);
                    } finally {
                        latch.countDown();
                    }
                });
            }, scheduledDelayMs, TimeUnit.MILLISECONDS);
        }

        // Wait for all events to complete
        try {
            long expectedDurationMs = (long) (originalDurationMs / speedFactor) + 60000;
            boolean completed = latch.await(expectedDurationMs, TimeUnit.MILLISECONDS);
            if (!completed) {
                log("Warning: Timeout waiting for all requests to complete");
            }
        } catch (InterruptedException e) {
            log("Interrupted while waiting for requests");
            Thread.currentThread().interrupt();
        }

        long replayEndTime = System.currentTimeMillis();
        long actualDurationMs = replayEndTime - replayStartTime;
        double actualDurationSec = actualDurationMs / 1000.0;

        // Print summary
        System.out.println("\n" + "=".repeat(60));
        System.out.println("REPLAY SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("  Source file:      %s%n", jsonlPath);
        System.out.printf("  Speed factor:     %.1fx%n", speedFactor);
        System.out.printf("  Total events:     %d%n", totalEvents.get());
        System.out.printf("  Successful:       %d%n", successCount.get());
        System.out.printf("  Failed:           %d%n", failCount.get());
        System.out.printf("  Avg latency:      %.1f ms%n",
                totalEvents.get() > 0 ? (double) totalLatencyMs.get() / totalEvents.get() : 0);
        System.out.printf("  Replay duration:  %.1f seconds%n", actualDurationSec);
        System.out.println("=".repeat(60));

        // Generate HTML report if requested
        if (generateHtml && !replayEvents.isEmpty()) {
            try {
                String baseName = Paths.get(jsonlPath).getFileName().toString().replace(".jsonl", "");
                String htmlPath = String.format("replay_%s_%.0fx.html", baseName, speedFactor);

                String savedPath = ReplayReportWriter.saveHtml(
                        "Replay: " + baseName,
                        replayEvents,
                        speedFactor,
                        actualDurationMs,
                        originalDurationMs,
                        htmlPath);
                System.out.println("\nHTML report saved to: " + savedPath);
            } catch (IOException e) {
                System.err.println("Failed to save HTML report: " + e.getMessage());
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("total_events", totalEvents.get());
        result.put("success_count", successCount.get());
        result.put("fail_count", failCount.get());
        result.put("avg_latency_ms", totalEvents.get() > 0 ? (double) totalLatencyMs.get() / totalEvents.get() : 0);
        result.put("duration_sec", actualDurationSec);
        return result;
    }

    /**
     * Load events from the JSONL file.
     */
    private List<JsonObject> loadEvents() throws IOException {
        List<JsonObject> events = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonlPath))) {
            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty())
                    continue;

                try {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    if (event.has("payload") && event.has("ts")) {
                        events.add(event);
                    }
                } catch (Exception e) {
                    if (verbose) {
                        log(String.format("Skipping invalid JSON at line %d: %s", lineNum, e.getMessage()));
                    }
                }

                if (lineNum % 100000 == 0) {
                    log(String.format("Loaded %d events from %d lines...", events.size(), lineNum));
                }
            }
        }

        return events;
    }

    /**
     * Fire a single request based on the event data.
     */
    private void fireRequest(JsonObject event, long scheduledDelayMs) {
        totalEvents.incrementAndGet();

        long actualTime = System.currentTimeMillis() - replayStartTime;
        String endpoint = event.has("endpoint") ? event.get("endpoint").getAsString() : "/api/v1/plays";
        String user = event.has("user") ? event.get("user").getAsString() : "unknown";
        JsonObject payload = event.getAsJsonObject("payload");

        if (dryRun) {
            log(String.format("[DRY-RUN] Would POST to %s for user %s", endpoint, user));
            successCount.incrementAndGet();

            if (generateHtml) {
                replayEvents.add(new ReplayReportWriter.ReplayEvent(
                        scheduledDelayMs, actualTime, 1, true, endpoint, user, null));
            }
            return;
        }

        // Update timestamps in payload to current time
        long now = System.currentTimeMillis();
        if (payload.has("timestamp")) {
            payload.addProperty("timestamp", now);
        }
        if (payload.has("updatedTimestamp")) {
            payload.addProperty("updatedTimestamp", now);
        }

        // Replace loadToken and playId with fresh values from SessionManager
        if (sessionManager != null && payload.has("userId") && payload.has("id") && payload.has("series")) {
            String userId = payload.get("userId").getAsString();
            String puzzleId = payload.get("id").getAsString();
            String series = payload.get("series").getAsString();

            SessionManager.SessionTokens tokens = sessionManager.getOrCreateSession(userId, puzzleId, series);
            if (tokens.isValid()) {
                payload.addProperty("loadToken", tokens.loadToken);
                payload.addProperty("playId", tokens.playId);
            }
        }

        String url = baseUrl + endpoint.replaceFirst("^/", "");

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(GSON.toJson(payload), JSON_MEDIA_TYPE))
                .build();

        long start = System.nanoTime();
        String error = null;
        boolean success = false;
        long latencyMs = 0;

        try (Response response = client.newCall(request).execute()) {
            latencyMs = (System.nanoTime() - start) / 1_000_000;
            totalLatencyMs.addAndGet(latencyMs);

            if (response.isSuccessful()) {
                successCount.incrementAndGet();
                success = true;
                if (verbose) {
                    log(String.format("OK %s user=%s latency=%dms", endpoint, user, latencyMs));
                }
            } else {
                failCount.incrementAndGet();
                error = "HTTP " + response.code();
                log(String.format("FAIL %s user=%s status=%d", endpoint, user, response.code()));
            }

            if (response.body() != null) {
                response.body().close();
            }
        } catch (IOException e) {
            latencyMs = (System.nanoTime() - start) / 1_000_000;
            failCount.incrementAndGet();
            error = e.getMessage();
            log(String.format("ERROR %s user=%s: %s", endpoint, user, e.getMessage()));
        }

        // Track event for HTML report
        if (generateHtml) {
            replayEvents.add(new ReplayReportWriter.ReplayEvent(
                    scheduledDelayMs, actualTime, latencyMs, success, endpoint, user, error));
        }
    }

    /**
     * Calculate maximum concurrent requests based on event timing.
     * Uses 100ms time windows to estimate peak concurrency.
     */
    private int calculateMaxConcurrency(List<JsonObject> events, long firstTs) {
        // Count events per 100ms window
        Map<Long, Integer> eventsPerWindow = new HashMap<>();

        for (JsonObject event : events) {
            long eventTs = event.get("ts").getAsLong();
            long delayMs = (long) ((eventTs - firstTs) / speedFactor);
            long windowKey = delayMs / 100; // 100ms buckets
            eventsPerWindow.merge(windowKey, 1, Integer::sum);
        }

        int maxInWindow = eventsPerWindow.values().stream()
                .max(Integer::compareTo)
                .orElse(10);

        // Add buffer for HTTP latency overlap (requests from prior windows still
        // in-flight)
        int calculated = Math.max(maxInWindow * 2, 20);
        // Cap at reasonable maximum to avoid resource exhaustion
        return Math.min(calculated, 500);
    }

    /**
     * Shutdown the executor and client.
     */
    public void shutdown() {
        scheduler.shutdown();
        if (workerPool != null) {
            workerPool.shutdown();
        }
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();

        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
            if (workerPool != null) {
                workerPool.awaitTermination(30, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main method for standalone execution.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ReplayExecutor <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --base-url <url>   Base URL (default: https://cdn-test.amuselabs.com/pmm/)");
            System.out.println("  --speed <factor>   Speed factor (default: 1.0)");
            System.out.println("  --dry-run          Don't actually send requests");
            System.out.println("  --html             Generate HTML report");
            System.out.println("  -v, --verbose      Verbose output");
            return;
        }

        String jsonlPath = args[0];
        String baseUrl = "https://cdn-test.amuselabs.com/pmm/";
        double speedFactor = 1.0;
        boolean verbose = false;
        boolean dryRun = false;
        boolean html = false;

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
                case "-v":
                case "--verbose":
                    verbose = true;
                    break;
            }
        }

        ReplayExecutor executor = new ReplayExecutor(jsonlPath, baseUrl, speedFactor, verbose, dryRun, html);
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
