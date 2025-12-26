package com.perftest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.*;

/**
 * Main entry point for API Flow V2 Performance Testing.
 * Java port of api_flow_v2.py
 */
public class ApiFlowV2 {

    @Parameter(names = { "--random-uid" }, description = "Generate random UID for each run")
    private boolean randomUid = false;

    @Parameter(names = { "--uid" }, description = "UID to use")
    private String uid = "vansh";

    @Parameter(names = { "--uid-pool-size" }, description = "Size of UID pool")
    private int uidPoolSize = 0;

    @Parameter(names = { "--parallel" }, description = "Number of parallel threads")
    private int parallel = 0;

    @Parameter(names = { "--rps" }, description = "Requests per second (wave mode)")
    private int rps = 0;

    @Parameter(names = { "--duration" }, description = "Duration in seconds (wave mode)")
    private int duration = 1;

    @Parameter(names = { "--title" }, description = "Title for this test run")
    private String title = "";

    @Parameter(names = { "--output" }, description = "Output path for CSV results")
    private String output = "";

    @Parameter(names = { "-v", "--verbose" }, description = "Print step-by-step progress")
    private boolean verbose = false;

    @Parameter(names = { "--html" }, description = "Generate HTML dashboard after saving CSV")
    private boolean html = false;

    @Parameter(names = { "--help", "-h" }, help = true, description = "Show help")
    private boolean help = false;

    public static void main(String[] args) {
        ApiFlowV2 app = new ApiFlowV2();
        JCommander jc = JCommander.newBuilder()
                .addObject(app)
                .programName("api-flow-v2")
                .build();

        jc.parse(args);

        if (app.help) {
            jc.usage();
            return;
        }

        app.run();
    }

    public void run() {
        if (verbose) {
            System.out.printf("V2 Mode: puzzle_id=%s, state_len=%d%n%n",
                    ApiConfig.PUZZLE_ID, ApiConfig.STATE_LEN);
        }

        List<String> uidPool = null;
        if (uidPoolSize > 0) {
            uidPool = new ArrayList<>();
            for (int i = 0; i < uidPoolSize; i++) {
                uidPool.add(ApiConfig.generateRandomUid());
            }
            if (verbose) {
                System.out.printf("Generated UID pool of %d users%n", uidPool.size());
            }
        }

        if (rps > 0) {
            runWaveMode(uidPool);
        } else if (parallel > 0) {
            runParallelMode(uidPool);
        } else {
            runSingleMode(uidPool);
        }
    }

    @SuppressWarnings("unchecked")
    private void runWaveMode(List<String> uidPool) {
        if (verbose) {
            System.out.println("=".repeat(60));
            System.out.println("Wave Execution Mode (V2)");
            if (!title.isEmpty()) {
                System.out.println("Title: " + title);
            }
            System.out.println("=".repeat(60) + "\n");
        }

        WaveExecutor executor = new WaveExecutor(rps, duration, randomUid, uidPool, title, verbose);
        Map<String, Object> runData = executor.execute();

        // Print summary
        System.out.println("\n" + "=".repeat(60));
        System.out.println("WAVE EXECUTION SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("  Title:            %s%n", runData.getOrDefault("title", "(none)"));
        System.out.printf("  Puzzle ID:        %s%n", ApiConfig.PUZZLE_ID);
        System.out.printf("  State Length:     %d%n", ApiConfig.STATE_LEN);

        Map<String, Object> config = (Map<String, Object>) runData.get("config");
        System.out.printf("  Config:           %d rps × %ds%n", config.get("rps"), config.get("duration"));
        System.out.printf("  Total Wall Time:  %.0f ms%n", runData.get("total_time_ms"));
        System.out.println();

        System.out.printf("  %-6s %-10s %-10s %-10s %-10s%n", "Wave", "Success", "Min", "Max", "Avg");
        System.out.println("  " + "-".repeat(46));

        List<Map<String, Object>> waves = (List<Map<String, Object>>) runData.get("waves");
        for (Map<String, Object> wave : waves) {
            String successStr = String.format("%d/%d", wave.get("success"), wave.get("threads"));
            String minVal = wave.containsKey("min") ? String.format("%.0f", wave.get("min")) : "-";
            String maxVal = wave.containsKey("max") ? String.format("%.0f", wave.get("max")) : "-";
            String avgVal = wave.containsKey("avg") ? String.format("%.0f", wave.get("avg")) : "-";
            System.out.printf("  %-6d %-10s %-10s %-10s %-10s%n",
                    wave.get("wave_number"), successStr, minVal, maxVal, avgVal);
        }
        System.out.println("=".repeat(60));

        if (!output.isEmpty()) {
            try {
                String filepath = CsvResultWriter.saveResults(runData, output);
                System.out.println("\nResults saved to: " + filepath);

                // Generate HTML if requested
                if (html) {
                    String htmlPath = HtmlReportWriter.saveHtml(filepath, runData, filepath);
                    System.out.println("HTML dashboard: " + htmlPath);
                }
            } catch (Exception e) {
                System.err.println("Failed to save results: " + e.getMessage());
            }
        }
    }

    private void runParallelMode(List<String> uidPool) {
        if (verbose) {
            System.out.println("=".repeat(60));
            System.out.printf("Running %d parallel threads (V2)...%n", parallel);
            System.out.println("=".repeat(60) + "\n");
        }

        long startTime = System.nanoTime();
        List<Map<String, Object>> results = new ArrayList<>();

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(parallel);
        List<java.util.concurrent.Future<Map<String, Object>>> futures = new ArrayList<>();

        for (int i = 0; i < parallel; i++) {
            final int threadId = i;
            final List<String> pool = uidPool;
            futures.add(executor.submit(() -> {
                Map<String, Object> threadResult = new HashMap<>();
                threadResult.put("thread", threadId);

                try {
                    ApiConfig config = ApiConfig.builder()
                            .uid(uid)
                            .useRandomUid(randomUid)
                            .uidPool(pool)
                            .build();

                    ApiFlow flow = new ApiFlow(config, verbose);
                    try {
                        Map<String, Object> result = flow.runSequentialFlow();
                        threadResult.put("result", result);
                        if (verbose) {
                            System.out.printf("Thread %d done: %s%n", threadId,
                                    Boolean.TRUE.equals(result.get("success")) ? "OK" : "FAILED");
                        }
                    } finally {
                        flow.close();
                    }
                } catch (Exception e) {
                    threadResult.put("error", e.getMessage());
                    if (verbose) {
                        System.out.printf("Thread %d crashed: %s%n", threadId, e.getMessage());
                    }
                }

                return threadResult;
            }));
        }

        for (java.util.concurrent.Future<Map<String, Object>> future : futures) {
            try {
                results.add(future.get());
            } catch (Exception e) {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", e.getMessage());
                results.add(errorResult);
            }
        }

        executor.shutdown();

        double totalTimeMs = (System.nanoTime() - startTime) / 1_000_000.0;
        printResultsTable(results, totalTimeMs);
    }

    private void runSingleMode(List<String> uidPool) {
        if (verbose) {
            System.out.println("=".repeat(60));
            System.out.println("Running single thread (V2)...");
            System.out.println("=".repeat(60) + "\n");
        }

        long startTime = System.nanoTime();

        ApiConfig config = ApiConfig.builder()
                .uid(uid)
                .useRandomUid(randomUid)
                .uidPool(uidPool)
                .build();

        ApiFlow flow = new ApiFlow(config, verbose);
        try {
            Map<String, Object> result = flow.runSequentialFlow();
            double totalTimeMs = (System.nanoTime() - startTime) / 1_000_000.0;
            printSingleResult(result, totalTimeMs);
        } finally {
            flow.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void printResultsTable(List<Map<String, Object>> results, double totalTimeMs) {
        System.out.println();
        System.out.println("=".repeat(90));
        System.out.println("RESULTS SUMMARY");
        System.out.println("=".repeat(90));
        System.out.println();
        System.out.printf("%-8s %-10s %-12s %-10s %-10s %-10s %-12s %-10s%n",
                "Thread", "Status", "UID", "Step1", "Step2", "Step3", "Step4 Avg", "Total");
        System.out.println("-".repeat(90));

        List<Double> allLatencies = new ArrayList<>();
        int successCount = 0;
        int failCount = 0;

        for (Map<String, Object> item : results) {
            Object threadId = item.getOrDefault("thread", "-");

            if (item.containsKey("error") && !item.containsKey("result")) {
                System.out.printf("%-8s %-10s %-12s %-10s %-10s %-10s %-12s %-10s%n",
                        threadId, "CRASHED", "-", "-", "-", "-", "-", "-");
                failCount++;
                continue;
            }

            Map<String, Object> result = (Map<String, Object>) item.get("result");
            if (result == null || !Boolean.TRUE.equals(result.get("success"))) {
                Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
                String uidStr = (String) s1.getOrDefault("uid", "-");
                System.out.printf("%-8s %-10s %-12s %-10s %-10s %-10s %-12s %-10s%n",
                        threadId, "FAILED", uidStr, "-", "-", "-", "-", "-");
                failCount++;
                continue;
            }

            successCount++;

            Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
            Map<String, Object> s2 = (Map<String, Object>) result.getOrDefault("step2", Map.of());
            Map<String, Object> s3 = (Map<String, Object>) result.getOrDefault("step3", Map.of());
            Map<String, Object> s4 = (Map<String, Object>) result.getOrDefault("step4", Map.of());

            String uidStr = ((String) s1.getOrDefault("uid", "-"));
            if (uidStr.length() > 10)
                uidStr = uidStr.substring(0, 10);

            double l1 = (Double) s1.getOrDefault("latency_ms", 0.0);
            double l2 = (Double) s2.getOrDefault("latency_ms", 0.0);
            double l3 = (Double) s3.getOrDefault("latency_ms", 0.0);

            List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.getOrDefault("iterations", List.of());
            double l4Total = 0;
            for (Map<String, Object> it : iterations) {
                l4Total += (Double) it.getOrDefault("latency_ms", 0.0);
            }
            double l4Avg = iterations.isEmpty() ? 0 : l4Total / iterations.size();

            double totalLatency = l1 + l2 + l3 + l4Total;
            allLatencies.add(totalLatency);

            System.out.printf("%-8s %-10s %-12s %-10.1f %-10.1f %-10.1f %-12.1f %-10.1f%n",
                    threadId, "OK", uidStr, l1, l2, l3, l4Avg, totalLatency);
        }

        System.out.println("-".repeat(90));
        System.out.println();
        System.out.println("AGGREGATE STATISTICS");
        System.out.println("-".repeat(40));
        System.out.printf("  Total Threads:     %d%n", results.size());
        System.out.printf("  Successful:        %d%n", successCount);
        System.out.printf("  Failed/Crashed:    %d%n", failCount);
        System.out.printf("  Total Wall Time:   %.1f ms (%.2f s)%n", totalTimeMs, totalTimeMs / 1000);

        if (!allLatencies.isEmpty()) {
            System.out.println();
            System.out.println("  Per-Thread Latency (sum of all steps):");
            System.out.printf("    Min:    %.1f ms%n", Collections.min(allLatencies));
            System.out.printf("    Max:    %.1f ms%n", Collections.max(allLatencies));
            System.out.printf("    Avg:    %.1f ms%n", allLatencies.stream().mapToDouble(d -> d).average().orElse(0));
        }

        System.out.println();
        System.out.println("=".repeat(90));
    }

    @SuppressWarnings("unchecked")
    private void printSingleResult(Map<String, Object> result, double totalTimeMs) {
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("RESULTS SUMMARY");
        System.out.println("=".repeat(60));

        if (!Boolean.TRUE.equals(result.get("success"))) {
            System.out.println("  Status: FAILED");
            System.out.printf("  Error: %s%n", result.getOrDefault("error", "unknown"));
        } else {
            Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
            Map<String, Object> s2 = (Map<String, Object>) result.getOrDefault("step2", Map.of());
            Map<String, Object> s3 = (Map<String, Object>) result.getOrDefault("step3", Map.of());
            Map<String, Object> s4 = (Map<String, Object>) result.getOrDefault("step4", Map.of());

            System.out.println("  Status: OK");
            System.out.printf("  UID: %s%n", s1.getOrDefault("uid", "-"));
            System.out.printf("  Puzzle ID: %s%n", s3.getOrDefault("puzzle_id", "-"));
            System.out.println();
            System.out.println("  Step Latencies:");
            System.out.printf("    Step 1 (date-picker):      %.1f ms%n", s1.getOrDefault("latency_ms", 0.0));
            System.out.printf("    Step 2 (postPickerStatus): %.1f ms%n", s2.getOrDefault("latency_ms", 0.0));
            System.out.printf("    Step 3 (load crossword):   %.1f ms%n", s3.getOrDefault("latency_ms", 0.0));

            List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.getOrDefault("iterations", List.of());
            if (!iterations.isEmpty()) {
                double s4Total = 0;
                for (Map<String, Object> it : iterations) {
                    s4Total += (Double) it.getOrDefault("latency_ms", 0.0);
                }
                System.out.printf("    Step 4 (10 play posts):    %.1f ms total, %.1f ms avg%n",
                        s4Total, s4Total / iterations.size());
            }

            double l1 = (Double) s1.getOrDefault("latency_ms", 0.0);
            double l2 = (Double) s2.getOrDefault("latency_ms", 0.0);
            double l3 = (Double) s3.getOrDefault("latency_ms", 0.0);
            double l4Total = 0;
            for (Map<String, Object> it : iterations) {
                l4Total += (Double) it.getOrDefault("latency_ms", 0.0);
            }
            double totalApi = l1 + l2 + l3 + l4Total;

            System.out.println();
            System.out.printf("  Total API Time:    %.1f ms%n", totalApi);
        }

        System.out.printf("  Total Wall Time:   %.1f ms (%.2f s)%n", totalTimeMs, totalTimeMs / 1000);
        System.out.println("=".repeat(60));
    }
}
