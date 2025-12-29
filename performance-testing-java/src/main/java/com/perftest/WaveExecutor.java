package com.perftest;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wave-based load testing with TRUE RPS: start `rps` threads every second for
 * `duration`
 * seconds, without waiting for previous waves to complete.
 * 
 * This means threads from different waves may overlap and run concurrently.
 */
public class WaveExecutor {

    private final int rps;
    private final int duration;
    private final boolean useRandomUid;
    private final List<String> uidPool;
    private final String title;
    private final boolean verbose;
    private final boolean useV3;

    public WaveExecutor(int rps, int duration, boolean useRandomUid,
            List<String> uidPool, String title, boolean verbose) {
        this(rps, duration, useRandomUid, uidPool, title, verbose, false);
    }

    public WaveExecutor(int rps, int duration, boolean useRandomUid,
            List<String> uidPool, String title, boolean verbose, boolean useV3) {
        this.rps = rps;
        this.duration = duration;
        this.useRandomUid = useRandomUid;
        this.uidPool = uidPool;
        this.title = title;
        this.verbose = verbose;
        this.useV3 = useV3;
    }

    public Map<String, Object> execute() {
        int totalThreads = rps * duration;

        if (verbose) {
            System.out.printf("Starting TRUE RPS wave execution: %d req/sec for %d seconds (%d total)%n",
                    rps, duration, totalThreads);
            System.out.printf("Using hardcoded puzzle: %s, state_len: %d%n",
                    ApiConfig.PUZZLE_ID, ApiConfig.STATE_LEN);
            System.out.println("NOTE: Waves launch every 1s regardless of previous wave completion");
            System.out.println("-".repeat(60));
        }

        // Shared thread pool for ALL API calls - large enough to handle overlapping
        // waves
        // Max concurrent = rps * avg_completion_time_in_seconds (e.g., 5 * 4 = 20)
        ExecutorService workerPool = Executors.newFixedThreadPool(rps * 8); // 5x for safety margin

        // Scheduler to launch waves every second
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // Collect ALL futures across all waves
        List<Future<Map<String, Object>>> allFutures = Collections.synchronizedList(new ArrayList<>());

        // Track wave launch times for accurate timing
        Map<Integer, Long> waveLaunchTimes = new ConcurrentHashMap<>();

        // Counter for progress tracking
        AtomicInteger wavesLaunched = new AtomicInteger(0);
        AtomicInteger wavesCompleted = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);

        // Track wave thread completion counts
        Map<Integer, AtomicInteger> waveThreadCounts = new ConcurrentHashMap<>();
        for (int w = 1; w <= duration; w++) {
            waveThreadCounts.put(w, new AtomicInteger(0));
        }

        // Track latencies for completed waves (for progress reporting)
        List<Double> recentLatencies = Collections.synchronizedList(new ArrayList<>());

        long overallStart = System.nanoTime();

        // Schedule wave launches: one every second for 'duration' seconds
        CountDownLatch allWavesLaunched = new CountDownLatch(duration);

        for (int wave = 0; wave < duration; wave++) {
            final int waveNum = wave + 1;

            // Schedule this wave to start at exactly wave*1000ms from now
            scheduler.schedule(() -> {
                long waveStartTime = System.nanoTime();
                waveLaunchTimes.put(waveNum, waveStartTime);

                if (verbose) {
                    System.out.printf("%nWave %d/%d: Launching %d threads (t=%.1fs)...%n",
                            waveNum, duration, rps, (waveStartTime - overallStart) / 1_000_000_000.0);
                }

                // Submit threads for this wave
                for (int i = 0; i < rps; i++) {
                    final int threadId = i;

                    Future<Map<String, Object>> future = workerPool.submit(() -> {
                        Map<String, Object> threadResult = new HashMap<>();
                        threadResult.put("wave", waveNum);
                        threadResult.put("thread", threadId);
                        threadResult.put("launch_time", System.currentTimeMillis());

                        try {
                            ApiConfig config = ApiConfig.builder()
                                    .useRandomUid(useRandomUid)
                                    .uidPool(uidPool)
                                    .build();

                            if (useV3) {
                                ApiFlowV3 flow = new ApiFlowV3(config, verbose);
                                try {
                                    Map<String, Object> result = flow.runSequentialFlow();
                                    threadResult.put("result", result);
                                } finally {
                                    flow.close();
                                }
                            } else {
                                ApiFlow flow = new ApiFlow(config, verbose);
                                try {
                                    Map<String, Object> result = flow.runSequentialFlow();
                                    threadResult.put("result", result);
                                } finally {
                                    flow.close();
                                }
                            }
                        } catch (Exception e) {
                            threadResult.put("error", e.getMessage());
                        }

                        threadResult.put("completion_time", System.currentTimeMillis());

                        // Track wave completion and calculate latency
                        @SuppressWarnings("unchecked")
                        Map<String, Object> result = (Map<String, Object>) threadResult.get("result");
                        if (result != null && Boolean.TRUE.equals(result.get("success"))) {
                            double latency = calculateTotalLatency(result);
                            recentLatencies.add(latency);
                        }

                        // Check if this wave is complete
                        int waveCompleteCount = waveThreadCounts.get(waveNum).incrementAndGet();
                        if (waveCompleteCount == rps) {
                            int completedWaves = wavesCompleted.incrementAndGet();

                            // Print progress every 30 waves (non-verbose mode)
                            if (!verbose && completedWaves % 30 == 0) {
                                double avgLatency = 0;
                                if (!recentLatencies.isEmpty()) {
                                    avgLatency = recentLatencies.stream().mapToDouble(d -> d).average().orElse(0);
                                }
                                System.out.printf("Completed %d/%d waves (avg latency: %.0fms)%n",
                                        completedWaves, duration, avgLatency);
                                recentLatencies.clear(); // Reset for next batch
                            }
                        }

                        completedCount.incrementAndGet();

                        return threadResult;
                    });

                    allFutures.add(future);
                }

                wavesLaunched.incrementAndGet();
                allWavesLaunched.countDown();

            }, wave * 1000L, TimeUnit.MILLISECONDS); // Launch at wave*1000ms
        }

        // Wait for all waves to be launched
        try {
            allWavesLaunched.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (verbose) {
            System.out.printf("%nAll %d waves launched. Waiting for %d threads to complete...%n",
                    duration, totalThreads);
        }

        // Shutdown scheduler (all waves are launched)
        scheduler.shutdown();

        // Collect all results (this blocks until all threads complete)
        List<Map<String, Object>> allResults = new ArrayList<>();
        for (Future<Map<String, Object>> future : allFutures) {
            try {
                allResults.add(future.get());
            } catch (Exception e) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", e.getMessage());
                allResults.add(errorResult);
            }
        }

        // Shutdown worker pool
        workerPool.shutdown();

        double totalTimeMs = (System.nanoTime() - overallStart) / 1_000_000.0;

        if (verbose) {
            System.out.printf("%nAll threads complete. Total time: %.1fms%n", totalTimeMs);
        }

        // Compute wave-level statistics (group by wave)
        List<Map<String, Object>> waves = computeWaveStats(allResults);

        // Build run data
        Map<String, Object> runData = new HashMap<>();
        runData.put("title", title);
        runData.put("timestamp", java.time.Instant.now().toString());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rps", rps);
        configMap.put("duration", duration);
        configMap.put("total_threads", totalThreads);
        configMap.put("puzzle_id", ApiConfig.PUZZLE_ID);
        configMap.put("state_len", ApiConfig.STATE_LEN);
        configMap.put("true_rps", true); // Mark as true RPS mode
        runData.put("config", configMap);

        runData.put("waves", waves);
        runData.put("results", allResults);
        runData.put("total_time_ms", totalTimeMs);

        return runData;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> computeWaveStats(List<Map<String, Object>> allResults) {
        // Group results by wave
        Map<Integer, List<Map<String, Object>>> resultsByWave = new TreeMap<>();
        for (Map<String, Object> result : allResults) {
            int wave = (Integer) result.getOrDefault("wave", 0);
            resultsByWave.computeIfAbsent(wave, k -> new ArrayList<>()).add(result);
        }

        List<Map<String, Object>> waves = new ArrayList<>();

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : resultsByWave.entrySet()) {
            int waveNum = entry.getKey();
            List<Map<String, Object>> waveResults = entry.getValue();

            List<Map<String, Object>> successful = new ArrayList<>();
            for (Map<String, Object> r : waveResults) {
                Map<String, Object> result = (Map<String, Object>) r.get("result");
                if (result != null && Boolean.TRUE.equals(result.get("success"))) {
                    successful.add(r);
                }
            }

            List<Double> latencies = new ArrayList<>();
            for (Map<String, Object> r : successful) {
                Map<String, Object> res = (Map<String, Object>) r.get("result");
                double total = calculateTotalLatency(res);
                latencies.add(total);
            }

            Map<String, Object> waveStats = new HashMap<>();
            waveStats.put("wave_number", waveNum);
            waveStats.put("threads", waveResults.size());
            waveStats.put("success", successful.size());
            waveStats.put("failed", waveResults.size() - successful.size());
            waveStats.put("latencies", latencies);

            if (!latencies.isEmpty()) {
                waveStats.put("min", Collections.min(latencies));
                waveStats.put("max", Collections.max(latencies));
                waveStats.put("avg", latencies.stream().mapToDouble(d -> d).average().orElse(0));
            }

            waves.add(waveStats);

            if (verbose) {
                System.out.printf("  Wave %d stats: %d/%d success, avg=%.0fms%n",
                        waveNum, successful.size(), waveResults.size(),
                        latencies.isEmpty() ? 0 : latencies.stream().mapToDouble(d -> d).average().orElse(0));
            }
        }

        return waves;
    }

    @SuppressWarnings("unchecked")
    private double calculateTotalLatency(Map<String, Object> res) {
        double total = 0;

        Map<String, Object> s1 = (Map<String, Object>) res.get("step1");
        Map<String, Object> s2 = (Map<String, Object>) res.get("step2");
        Map<String, Object> s3 = (Map<String, Object>) res.get("step3");
        Map<String, Object> s4 = (Map<String, Object>) res.get("step4");

        if (s1 != null)
            total += (Double) s1.getOrDefault("latency_ms", 0.0);
        if (s2 != null)
            total += (Double) s2.getOrDefault("latency_ms", 0.0);
        if (s3 != null)
            total += (Double) s3.getOrDefault("latency_ms", 0.0);

        if (s4 != null) {
            List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.get("iterations");
            if (iterations != null) {
                for (Map<String, Object> it : iterations) {
                    total += (Double) it.getOrDefault("latency_ms", 0.0);
                }
            }
        }

        return total;
    }
}
