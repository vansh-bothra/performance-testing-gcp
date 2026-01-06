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
 * Streaming Replay Executor - Memory-efficient replay of large JSONL files.
 * 
 * Unlike ReplayExecutor which loads all events into memory, this version:
 * - Reads the file line-by-line (streaming)
 * - Schedules events in batches as it reads
 * - Uses bounded data structures for stats/reporting
 * - Can handle files larger than available RAM
 * 
 * Usage:
 * java -cp target/api-flow-v2.jar com.perftest.StreamingReplayExecutor
 * <jsonl-file> [options]
 */
public class StreamingReplayExecutor {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
    private static final Gson GSON = new Gson();

    // Configuration
    private final String jsonlPath;
    private final String baseUrl;
    private final double speedFactor;
    private final boolean verbose;
    private final boolean dryRun;
    private final boolean generateHtml;

    // HTTP client with connection pooling
    private final OkHttpClient client;

    // Scheduler for timing-based event dispatch
    private final ScheduledExecutorService scheduler;

    // Worker pool for HTTP requests (initialized dynamically after pre-scan)
    private ExecutorService workerPool;

    // Session manager for dynamic token fetching
    private SessionManager sessionManager;
    private final Set<String> uniqueSessions = ConcurrentHashMap.newKeySet();

    // Stats - atomic counters for thread-safe updates
    private final AtomicInteger totalEvents = new AtomicInteger(0);
    private final AtomicInteger scheduledEvents = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);

    // Latency histogram buckets (0-50, 50-100, 100-200, 200-500, 500-1000, 1000+)
    private final AtomicInteger[] latencyBuckets = new AtomicInteger[6];

    // Aggregated stats for HTML report
    private final ConcurrentHashMap<Integer, TimeStats> timeStatsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> errorCountsMap = new ConcurrentHashMap<>();

    // Reservoir sampling for latency percentiles (store up to 10k latencies)
    private static final int MAX_LATENCY_SAMPLES = 10000;
    private final List<Long> latencySamples = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger latencySampleCounter = new AtomicInteger(0);

    // Inner class for time-aggregated stats
    private static class TimeStats {
        final AtomicInteger events = new AtomicInteger(0);
        final AtomicInteger success = new AtomicInteger(0);
        final AtomicInteger fail = new AtomicInteger(0);
        final AtomicLong totalLatency = new AtomicLong(0);
    }

    // Sampled events for HTML report (bounded to prevent OOM)
    private static final int MAX_SAMPLE_SIZE = 500;
    private final List<ReplayReportWriter.ReplayEvent> sampledEvents = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger sampleCounter = new AtomicInteger(0);

    // Timing
    private volatile long replayStartTime;
    private volatile long firstEventTs = -1;

    // Latch to track completion
    private final AtomicInteger pendingRequests = new AtomicInteger(0);

    public StreamingReplayExecutor(String jsonlPath, String baseUrl, double speedFactor,
            boolean verbose, boolean dryRun, boolean generateHtml) {
        this.jsonlPath = jsonlPath;
        this.baseUrl = baseUrl;
        this.speedFactor = speedFactor;
        this.verbose = verbose;
        this.dryRun = dryRun;
        this.generateHtml = generateHtml;

        // Initialize latency buckets
        for (int i = 0; i < latencyBuckets.length; i++) {
            latencyBuckets[i] = new AtomicInteger(0);
        }

        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(500, 5, TimeUnit.MINUTES))
                .build();

        // Scheduler for timing events
        this.scheduler = Executors.newScheduledThreadPool(2);

        // Worker pool initialized in execute() after pre-scan calculates max
        // concurrency
    }

    private void log(String msg) {
        if (verbose) {
            System.out.println("[StreamingReplay] " + msg);
        }
    }

    private void progress(String msg) {
        System.out.println("[Progress] " + msg);
    }

    /**
     * Main execution method. Streams through the JSONL file and schedules requests.
     */
    public Map<String, Object> execute() throws IOException {
        log("Starting streaming replay from: " + jsonlPath);
        log(String.format("Speed factor: %.1fx, Base URL: %s, Dry run: %s",
                speedFactor, baseUrl, dryRun));

        replayStartTime = System.currentTimeMillis();

        long fileSize = Files.size(Paths.get(jsonlPath));
        progress(String.format("File size: %.2f GB", fileSize / (1024.0 * 1024 * 1024)));

        // Pre-scan to calculate max concurrency
        progress("Pre-scanning file to calculate optimal thread pool size...");
        int maxConcurrency = preScanForConcurrency();
        log(String.format("Calculated max concurrency: %d threads", maxConcurrency));
        this.workerPool = Executors.newFixedThreadPool(maxConcurrency);

        // Initialize session manager for lazy token fetching (skip for dry runs)
        if (!dryRun) {
            this.sessionManager = new SessionManager(client, baseUrl, verbose);
            progress("SessionManager ready (lazy init mode - tokens fetched on demand)");
        }

        // Stream through the file
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(jsonlPath), "UTF-8"),
                8 * 1024 * 1024)) { // 8MB buffer for faster reading

            String line;
            int lineNum = 0;
            long lastProgressTime = System.currentTimeMillis();

            while ((line = reader.readLine()) != null) {
                lineNum++;

                if (line.trim().isEmpty())
                    continue;

                try {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    if (event.has("payload") && event.has("ts")) {
                        scheduleEvent(event);
                    }
                } catch (Exception e) {
                    if (verbose && lineNum < 100) {
                        log(String.format("Skipping invalid JSON at line %d: %s", lineNum, e.getMessage()));
                    }
                }

                // Progress every 100k lines or every 5 seconds
                if (lineNum % 100000 == 0) {
                    long now = System.currentTimeMillis();
                    if (now - lastProgressTime > 5000) {
                        progress(String.format("Read %,d lines, scheduled %,d events, completed %,d...",
                                lineNum, scheduledEvents.get(), successCount.get() + failCount.get()));
                        lastProgressTime = now;
                    }
                }
            }

            totalEvents.set(lineNum);
            progress(String.format("Finished reading %,d lines, scheduled %,d events",
                    lineNum, scheduledEvents.get()));
        }

        // Wait for all scheduled events to complete
        progress("Waiting for all scheduled requests to complete...");
        waitForCompletion();

        long replayEndTime = System.currentTimeMillis();
        long actualDurationMs = replayEndTime - replayStartTime;
        double actualDurationSec = actualDurationMs / 1000.0;

        // Print summary
        printSummary(actualDurationSec);

        // Generate HTML report if requested
        if (generateHtml && !sampledEvents.isEmpty()) {
            generateHtmlReport(actualDurationMs);
        }

        // Return results
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("total_lines", totalEvents.get());
        result.put("scheduled_events", scheduledEvents.get());
        result.put("success_count", successCount.get());
        result.put("fail_count", failCount.get());
        result.put("avg_latency_ms", successCount.get() > 0 ? (double) totalLatencyMs.get() / successCount.get() : 0);
        result.put("duration_sec", actualDurationSec);
        return result;
    }

    /**
     * Pre-scan the file to calculate maximum concurrent requests.
     * Quickly reads all timestamps without parsing payloads, counts events per
     * 100ms window for concurrency and 1000ms window for RPS.
     */
    private int preScanForConcurrency() throws IOException {
        Map<Long, Integer> eventsPerWindow100ms = new HashMap<>();
        Map<Long, Integer> eventsPerWindow1000ms = new HashMap<>();
        long scanFirstTs = -1;
        Set<String> sessionsFound = new HashSet<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(jsonlPath), "UTF-8"),
                4 * 1024 * 1024)) { // 4MB buffer for fast reading

            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                if (line.trim().isEmpty())
                    continue;

                try {
                    // Quick parse - extract timestamp and session info
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    if (event.has("ts")) {
                        long ts = event.get("ts").getAsLong();

                        if (scanFirstTs < 0) {
                            scanFirstTs = ts;
                        }

                        long delayMs = (long) ((ts - scanFirstTs) / speedFactor);
                        long windowKey100ms = delayMs / 100; // 100ms buckets for concurrency
                        long windowKey1000ms = delayMs / 1000; // 1000ms buckets for RPS
                        eventsPerWindow100ms.merge(windowKey100ms, 1, Integer::sum);
                        eventsPerWindow1000ms.merge(windowKey1000ms, 1, Integer::sum);

                        // Collect unique (userId, puzzleId, series) for session init
                        if (event.has("payload")) {
                            JsonObject payload = event.getAsJsonObject("payload");
                            String userId = payload.has("userId") ? payload.get("userId").getAsString() : null;
                            String puzzleId = payload.has("id") ? payload.get("id").getAsString() : null;
                            String series = payload.has("series") ? payload.get("series").getAsString() : null;
                            if (userId != null && puzzleId != null && series != null) {
                                sessionsFound.add(userId + "|" + puzzleId + "|" + series);
                            }
                        }
                    }
                } catch (Exception e) {
                    // Skip malformed lines during pre-scan
                }

                if (lineCount % 500000 == 0) {
                    progress(String.format("Pre-scan: %,d lines processed...", lineCount));
                }
            }

            progress(String.format("Pre-scan complete: %,d lines, %,d seconds of traffic, %,d unique sessions",
                    lineCount, eventsPerWindow1000ms.size(), sessionsFound.size()));
        }

        uniqueSessions.addAll(sessionsFound);

        int maxInWindow100ms = eventsPerWindow100ms.values().stream()
                .max(Integer::compareTo)
                .orElse(10);

        int maxRps = eventsPerWindow1000ms.values().stream()
                .max(Integer::compareTo)
                .orElse(1);

        double avgRps = eventsPerWindow1000ms.isEmpty() ? 0
                : eventsPerWindow1000ms.values().stream().mapToInt(i -> i).average().orElse(0);

        progress(String.format("Max RPS: %,d | Avg RPS: %.1f | Max burst (100ms): %,d",
                maxRps, avgRps, maxInWindow100ms));

        // Add buffer for HTTP latency overlap (requests from prior windows still
        // in-flight). Increased multiplier to 10x to handle high latency (300ms+).
        int calculated = Math.max(maxInWindow100ms * 10, 20);
        // Cap at reasonable maximum
        return Math.min(calculated, 500);
    }

    /**
     * Schedule a single event for execution at the appropriate time.
     */
    private void scheduleEvent(JsonObject event) {
        long eventTs = event.get("ts").getAsLong();

        // Initialize first event timestamp
        if (firstEventTs < 0) {
            firstEventTs = eventTs;
        }

        // Calculate when to fire this event (relative to first event, scaled by speed)
        long delayMs = (long) ((eventTs - firstEventTs) / speedFactor);

        // Schedule the event
        pendingRequests.incrementAndGet();
        scheduledEvents.incrementAndGet();

        scheduler.schedule(() -> {
            workerPool.submit(() -> {
                try {
                    fireRequest(event, delayMs);
                } finally {
                    pendingRequests.decrementAndGet();
                }
            });
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Fire a single HTTP request.
     */
    private void fireRequest(JsonObject event, long scheduledDelayMs) {
        long actualTime = System.currentTimeMillis() - replayStartTime;
        String endpoint = event.has("endpoint") ? event.get("endpoint").getAsString() : "/api/v1/plays";
        String user = event.has("user") ? event.get("user").getAsString() : "unknown";
        JsonObject payload = event.getAsJsonObject("payload");

        if (dryRun) {
            successCount.incrementAndGet();
            recordLatency(0);
            recordStats(actualTime, 0, true, null);
            maybeSampleEvent(scheduledDelayMs, actualTime, 1, true, endpoint, user, null);
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

        // Ensure payload has necessary fields (re-inject if stripped from log)
        if (!payload.has("userId")) {
            payload.addProperty("userId", user);
        }
        if (!payload.has("series")) {
            payload.addProperty("series", "gandalf"); // Hardcoded as per SessionManager
        }
        if (!payload.has("id")) {
            payload.addProperty("id", "d4725144"); // Hardcoded as per SessionManager
        }

        // Replace loadToken and playId with fresh values from SessionManager
        if (sessionManager != null) {
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
            recordLatency(latencyMs);

            if (response.isSuccessful()) {
                successCount.incrementAndGet();
                success = true;
            } else {
                failCount.incrementAndGet();
                error = "HTTP " + response.code();
                if (verbose) {
                    log(String.format("FAIL %s user=%s status=%d", endpoint, user, response.code()));
                }
            }

            if (response.body() != null) {
                response.body().close();
            }
        } catch (IOException e) {
            latencyMs = (System.nanoTime() - start) / 1_000_000;
            failCount.incrementAndGet();
            recordLatency(latencyMs);
            error = e.getMessage();
            if (verbose) {
                log(String.format("ERROR %s user=%s: %s", endpoint, user, e.getMessage()));
            }
        }

        // Record aggregated stats for HTML report
        recordStats(actualTime, latencyMs, success, error);

        maybeSampleEvent(scheduledDelayMs, actualTime, latencyMs, success, endpoint, user, error);
    }

    /**
     * Record stats for charts.
     */
    private void recordStats(long actualTime, long latencyMs, boolean success, String error) {
        if (!generateHtml)
            return;

        // Update TimeStats
        int bucket = (int) (actualTime / 1000);
        timeStatsMap.computeIfAbsent(bucket, k -> new TimeStats());
        TimeStats stats = timeStatsMap.get(bucket);
        stats.events.incrementAndGet();
        if (success) {
            stats.success.incrementAndGet();
            stats.totalLatency.addAndGet(latencyMs);

            // Reservoir sampling for percentiles
            int count = latencySampleCounter.incrementAndGet();
            if (count <= MAX_LATENCY_SAMPLES) {
                latencySamples.add(latencyMs);
            } else {
                int replaceIndex = ThreadLocalRandom.current().nextInt(count);
                if (replaceIndex < MAX_LATENCY_SAMPLES) {
                    latencySamples.set(replaceIndex, latencyMs);
                }
            }
        } else {
            stats.fail.incrementAndGet();
            if (error != null) {
                String truncatedError = error.length() > 80 ? error.substring(0, 80) : error;
                errorCountsMap.merge(truncatedError, 1, Integer::sum);
            }
        }
    }

    /**
     * Record latency in histogram buckets.
     */
    private void recordLatency(long latencyMs) {
        int bucket;
        if (latencyMs < 50)
            bucket = 0;
        else if (latencyMs < 100)
            bucket = 1;
        else if (latencyMs < 200)
            bucket = 2;
        else if (latencyMs < 500)
            bucket = 3;
        else if (latencyMs < 1000)
            bucket = 4;
        else
            bucket = 5;

        latencyBuckets[bucket].incrementAndGet();
    }

    /**
     * Sample events for HTML report (reservoir sampling to stay bounded).
     */
    private void maybeSampleEvent(long scheduledTime, long actualTime, long latencyMs,
            boolean success, String endpoint, String user, String error) {
        if (!generateHtml)
            return;

        int count = sampleCounter.incrementAndGet();

        // Reservoir sampling: keep first MAX_SAMPLE_SIZE, then randomly replace
        if (count <= MAX_SAMPLE_SIZE) {
            sampledEvents.add(new ReplayReportWriter.ReplayEvent(
                    scheduledTime, actualTime, latencyMs, success, endpoint, user, error));
        } else {
            // Random replacement with decreasing probability
            int replaceIndex = ThreadLocalRandom.current().nextInt(count);
            if (replaceIndex < MAX_SAMPLE_SIZE) {
                sampledEvents.set(replaceIndex, new ReplayReportWriter.ReplayEvent(
                        scheduledTime, actualTime, latencyMs, success, endpoint, user, error));
            }
        }
    }

    /**
     * Wait for all pending requests to complete.
     */
    private void waitForCompletion() {
        int lastPending = -1;
        int stuckCount = 0;
        long lastProgressTime = 0;

        while (pendingRequests.get() > 0) {
            int pending = pendingRequests.get();

            if (pending == lastPending) {
                stuckCount++;
                if (stuckCount > 60) { // 60 seconds stuck
                    log("Warning: Some requests appear stuck, continuing...");
                    break;
                }
            } else {
                stuckCount = 0;
            }
            lastPending = pending;

            // Log every 5 seconds with full stats
            long now = System.currentTimeMillis();
            if (now - lastProgressTime >= 5000) {
                int completed = successCount.get() + failCount.get();
                double avgLatency = successCount.get() > 0 ? (double) totalLatencyMs.get() / successCount.get() : 0;
                progress(String.format(
                        "Pending: %,d | Completed: %,d | Success: %,d | Failed: %,d | Avg latency: %.0fms",
                        pending, completed, successCount.get(), failCount.get(), avgLatency));
                lastProgressTime = now;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Print summary statistics.
     */
    private void printSummary(double actualDurationSec) {
        int total = successCount.get() + failCount.get();
        double avgLatency = total > 0 ? (double) totalLatencyMs.get() / total : 0;

        System.out.println("\n" + "=".repeat(60));
        System.out.println("STREAMING REPLAY SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("  Source file:      %s%n", jsonlPath);
        System.out.printf("  Speed factor:     %.1fx%n", speedFactor);
        System.out.printf("  Total lines:      %,d%n", totalEvents.get());
        System.out.printf("  Scheduled events: %,d%n", scheduledEvents.get());
        System.out.printf("  Completed:        %,d%n", total);
        System.out.printf("  Successful:       %,d%n", successCount.get());
        System.out.printf("  Failed:           %,d%n", failCount.get());
        System.out.printf("  Avg latency:      %.1f ms%n", avgLatency);
        System.out.printf("  Replay duration:  %.1f seconds%n", actualDurationSec);
        System.out.println("-".repeat(60));
        System.out.println("  Latency Distribution:");
        System.out.printf("    0-50ms:     %,d%n", latencyBuckets[0].get());
        System.out.printf("    50-100ms:   %,d%n", latencyBuckets[1].get());
        System.out.printf("    100-200ms:  %,d%n", latencyBuckets[2].get());
        System.out.printf("    200-500ms:  %,d%n", latencyBuckets[3].get());
        System.out.printf("    500-1000ms: %,d%n", latencyBuckets[4].get());
        System.out.printf("    1000ms+:    %,d%n", latencyBuckets[5].get());
        System.out.println("=".repeat(60));
    }

    /**
     * Generate HTML report from sampled events.
     */
    private void generateHtmlReport(long actualDurationMs) {
        try {
            String baseName = Paths.get(jsonlPath).getFileName().toString().replace(".jsonl", "");
            String htmlPath = String.format("streaming_replay_%s_%.0fx.html", baseName, speedFactor);

            // Prepare aggregated data
            int maxBucket = timeStatsMap.keySet().stream().max(Integer::compareTo).orElse(0);
            int bucketCount = Math.min(Math.max(maxBucket + 1, 1), 300); // Max 300 buckets
            int bucketSize = Math.max((maxBucket + 1) / bucketCount, 1);

            List<String> timeLabels = new ArrayList<>();
            List<Integer> eventsPerSecond = new ArrayList<>();
            List<Integer> successPerSecond = new ArrayList<>();
            List<Integer> failPerSecond = new ArrayList<>();
            List<Double> avgLatencyPerSecond = new ArrayList<>();

            // Aggregate seconds into buckets
            for (int i = 0; i < bucketCount; i++) {
                int startSec = i * bucketSize;
                int endSec = (i + 1) * bucketSize;

                int totalEvents = 0;
                int totalSuccess = 0;
                int totalFail = 0;
                long totalLatency = 0;

                for (int s = startSec; s < endSec; s++) {
                    TimeStats stats = timeStatsMap.get(s);
                    if (stats != null) {
                        totalEvents += stats.events.get();
                        totalSuccess += stats.success.get();
                        totalFail += stats.fail.get();
                        totalLatency += stats.totalLatency.get();
                    }
                }

                int seconds = startSec % 60;
                int minutes = startSec / 60;
                timeLabels.add(String.format("%d:%02d", minutes, seconds));

                eventsPerSecond.add(totalEvents);
                successPerSecond.add(totalSuccess);
                failPerSecond.add(totalFail);
                avgLatencyPerSecond.add(totalSuccess > 0 ? (double) totalLatency / totalSuccess : 0);
            }

            // Latency stats
            Collections.sort(latencySamples);
            long min = latencySamples.isEmpty() ? 0 : latencySamples.get(0);
            long max = latencySamples.isEmpty() ? 0 : latencySamples.get(latencySamples.size() - 1);
            long p50 = latencySamples.isEmpty() ? 0 : latencySamples.get(latencySamples.size() / 2);
            long p95 = latencySamples.isEmpty() ? 0 : latencySamples.get((int) (latencySamples.size() * 0.95));
            long p99 = latencySamples.isEmpty() ? 0 : latencySamples.get((int) (latencySamples.size() * 0.99));
            double avg = successCount.get() > 0 ? (double) totalLatencyMs.get() / successCount.get() : 0;

            // Histogram
            List<String> histogramLabels = Arrays.asList(
                    "0-50", "50-100", "100-200", "200-500", "500-1000", "1000+");
            List<Integer> histogramBins = new ArrayList<>();
            for (AtomicInteger bucket : latencyBuckets) {
                histogramBins.add(bucket.get());
            }

            // Generate HTML using the stats-based method
            String html = ReplayReportWriter.generateHtmlFromStats(
                    "Streaming Replay: " + baseName, speedFactor, actualDurationMs,
                    totalEvents.get(), successCount.get(), failCount.get(),
                    avg, min, max, p50, p95, p99,
                    timeLabels, eventsPerSecond, successPerSecond, failPerSecond, avgLatencyPerSecond,
                    histogramLabels, histogramBins,
                    sampledEvents, new HashMap<>(errorCountsMap));

            String outputPath = htmlPath.endsWith(".html") ? htmlPath : htmlPath + ".html";
            try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
                writer.write(html);
            }

            System.out.println("\nHTML report saved to: " + outputPath);
            System.out.println("(Charts based on ALL events; Gantt based on sample)");
        } catch (IOException e) {
            System.err.println("Failed to save HTML report: " + e.getMessage());
        }
    }

    /**
     * Shutdown executors and client.
     */
    public void shutdown() {
        scheduler.shutdown();
        workerPool.shutdown();
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();

        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
            workerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main method for standalone execution.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: StreamingReplayExecutor <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --base-url <url>   Base URL (default: https://cdn-test.amuselabs.com/pmm/)");
            System.out.println("  --speed <factor>   Speed factor (default: 1.0)");
            System.out.println("  --dry-run          Don't actually send requests");
            System.out.println("  --html             Generate HTML report (uses sampled events)");
            System.out.println("  -v, --verbose      Verbose output");
            System.out.println();
            System.out.println("Memory-efficient streaming replay for large files (>RAM).");
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

        StreamingReplayExecutor executor = new StreamingReplayExecutor(
                jsonlPath, baseUrl, speedFactor, verbose, dryRun, html);
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
