package com.perftest;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * HTML report generation for performance test results.
 * Ported from Python's view_results.py with timestamp support.
 */
public class HtmlReportWriter {

    @SuppressWarnings("unchecked")
    public static String generateHtml(String csvPath, Map<String, Object> runData) {
        String title = (String) runData.getOrDefault("title",
                Path.of(csvPath).getFileName().toString().replace(".csv", ""));
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        List<Map<String, Object>> results = (List<Map<String, Object>>) runData.get("results");
        Map<String, Object> stats = computeStats(results);
        Map<String, Object> overall = (Map<String, Object>) stats.get("overall");
        List<Map<String, Object>> waveStats = (List<Map<String, Object>>) stats.get("waves");
        List<Double> allLatencies = (List<Double>) stats.get("allLatencies");

        // Prepare chart data
        List<Double> threadLatencies = new ArrayList<>();
        List<Integer> threadWaves = new ArrayList<>();
        for (Map<String, Object> item : results) {
            int waveNum = (Integer) item.getOrDefault("wave", 1);
            Map<String, Object> result = (Map<String, Object>) item.get("result");
            if (result != null && Boolean.TRUE.equals(result.get("success"))) {
                double total = calculateTotalLatency(result);
                threadLatencies.add(total);
            } else {
                threadLatencies.add(0.0);
            }
            threadWaves.add(waveNum);
        }

        // Wave averages
        List<Double> waveAvgs = waveStats.stream()
                .map(w -> (Double) w.getOrDefault("avg", 0.0))
                .collect(Collectors.toList());
        List<String> waveLabels = waveStats.stream()
                .map(w -> "Wave " + w.get("wave"))
                .collect(Collectors.toList());

        // Histogram bins
        List<Integer> histogramBins = new ArrayList<>();
        List<String> histogramLabels = new ArrayList<>();
        if (!allLatencies.isEmpty()) {
            double minLat = Collections.min(allLatencies);
            double maxLat = Collections.max(allLatencies);
            int binCount = Math.min(20, allLatencies.size());
            double binWidth = binCount > 0 ? (maxLat - minLat) / binCount : 1;

            for (int i = 0; i < binCount; i++) {
                histogramBins.add(0);
                double binStart = minLat + i * binWidth;
                double binEnd = binStart + binWidth;
                histogramLabels.add(String.format("%.0f-%.0f", binStart, binEnd));
            }

            for (double lat : allLatencies) {
                int bin = (int) ((lat - minLat) / binWidth);
                if (bin >= binCount)
                    bin = binCount - 1;
                if (bin >= 0 && bin < binCount) {
                    histogramBins.set(bin, histogramBins.get(bin) + 1);
                }
            }
        }

        // Build wave table rows
        StringBuilder waveTableRows = new StringBuilder();
        for (int i = 0; i < waveStats.size(); i++) {
            Map<String, Object> wave = waveStats.get(i);
            String successStr = String.format("%d/%d", wave.get("success"), wave.get("threads"));
            int failed = (Integer) wave.get("failed");
            int threads = (Integer) wave.get("threads");
            String successClass = failed == 0 ? "success" : (failed < threads / 2 ? "warning" : "danger");
            String hiddenClass = i >= 10 ? "hidden-row" : "";

            waveTableRows.append(String.format("""
                    <tr class="%s" data-wave-row>
                        <td>%d</td>
                        <td class="%s">%s</td>
                        <td>%.0f</td>
                        <td>%.0f</td>
                        <td>%.0f</td>
                        <td>%.0f</td>
                        <td>%.0f</td>
                        <td>%.0f</td>
                    </tr>
                    """, hiddenClass, wave.get("wave"), successClass, successStr,
                    wave.getOrDefault("step1_avg", 0.0),
                    wave.getOrDefault("step2_avg", 0.0),
                    wave.getOrDefault("step3_avg", 0.0),
                    wave.getOrDefault("step4_avg", 0.0),
                    wave.getOrDefault("avg", 0.0),
                    wave.getOrDefault("p95", 0.0)));
        }

        // Build thread detail sections
        StringBuilder threadSections = new StringBuilder();
        for (Map<String, Object> wave : waveStats) {
            List<Map<String, Object>> waveRows = (List<Map<String, Object>>) wave.get("rows");
            double stdev = (Double) wave.getOrDefault("stdev", 0.0);
            double avg = (Double) wave.getOrDefault("avg", 0.0);

            StringBuilder threadRowsHtml = new StringBuilder();
            for (int i = 0; i < waveRows.size(); i++) {
                Map<String, Object> item = waveRows.get(i);
                Map<String, Object> r = (Map<String, Object>) item.get("result");
                boolean isSuccess = r != null && Boolean.TRUE.equals(r.get("success"));
                double totalMs = isSuccess ? calculateTotalLatency(r) : 0;
                boolean isOutlier = isSuccess && stdev > 0 && totalMs > (avg + 2 * stdev);
                String rowClass = isOutlier ? "outlier" : (!isSuccess ? "failure" : "");
                String hiddenClass = i >= 10 ? "hidden-row" : "";

                Map<String, Object> s1 = isSuccess ? (Map<String, Object>) r.getOrDefault("step1", Map.of()) : Map.of();
                Map<String, Object> s2 = isSuccess ? (Map<String, Object>) r.getOrDefault("step2", Map.of()) : Map.of();
                Map<String, Object> s3 = isSuccess ? (Map<String, Object>) r.getOrDefault("step3", Map.of()) : Map.of();
                Map<String, Object> s4 = isSuccess ? (Map<String, Object>) r.getOrDefault("step4", Map.of()) : Map.of();

                String uid = (String) s1.getOrDefault("uid", "-");
                String step1Start = CsvResultWriter.formatTimestamp(s1.get("start_timestamp"));
                String step1Ms = isSuccess ? String.format("%.1f", s1.getOrDefault("latency_ms", 0.0)) : "-";
                String step2Ms = isSuccess ? String.format("%.1f", s2.getOrDefault("latency_ms", 0.0)) : "-";
                String step3Ms = isSuccess ? String.format("%.1f", s3.getOrDefault("latency_ms", 0.0)) : "-";

                List<Map<String, Object>> iterations = isSuccess
                        ? (List<Map<String, Object>>) s4.getOrDefault("iterations", List.of())
                        : List.of();
                double s4Avg = 0;
                if (!iterations.isEmpty()) {
                    double s4Total = iterations.stream()
                            .mapToDouble(it -> (Double) it.getOrDefault("latency_ms", 0.0))
                            .sum();
                    s4Avg = s4Total / iterations.size();
                }
                String step4AvgMs = isSuccess ? String.format("%.1f", s4Avg) : "-";

                String error = "";
                if (!isSuccess && r != null) {
                    error = (String) r.getOrDefault("error", "");
                }

                threadRowsHtml.append(String.format("""
                        <tr class="%s %s" data-thread-row-%d>
                            <td>%d</td>
                            <td>%s</td>
                            <td class="timestamp-cell">%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td>%s</td>
                            <td class="%s">%.1f</td>
                            <td class="error-cell">%s</td>
                        </tr>
                        """, rowClass, hiddenClass, wave.get("wave"),
                        item.getOrDefault("thread", i),
                        uid,
                        step1Start,
                        isSuccess ? "✓" : "✗",
                        step1Ms, step2Ms, step3Ms, step4AvgMs,
                        isOutlier ? "outlier-value" : "",
                        totalMs,
                        error));
            }

            boolean showMoreThreads = waveRows.size() > 10;
            String toggleBtn = showMoreThreads ? String.format(
                    "<button class=\"toggle-btn\" onclick=\"toggleThreadRows(%d, this)\">Show all (%d)</button>",
                    wave.get("wave"), waveRows.size()) : "";

            threadSections.append(String.format("""
                    <details class="wave-details">
                        <summary>Wave %d - %d/%d success %s</summary>
                        <table class="thread-table">
                            <thead>
                                <tr>
                                    <th>Thread</th>
                                    <th>UID</th>
                                    <th>Start Time</th>
                                    <th>OK</th>
                                    <th>Step1 (ms)</th>
                                    <th>Step2 (ms)</th>
                                    <th>Step3 (ms)</th>
                                    <th>Step4 Avg (ms)</th>
                                    <th>Total (ms)</th>
                                    <th>Error</th>
                                </tr>
                            </thead>
                            <tbody>
                                %s
                            </tbody>
                        </table>
                    </details>
                    """, wave.get("wave"), wave.get("success"), wave.get("threads"), toggleBtn, threadRowsHtml));
        }

        boolean showMoreWaves = waveStats.size() > 10;
        String showMoreBtn = showMoreWaves
                ? String.format("<button class=\"toggle-btn\" onclick=\"toggleWaveRows(this)\">Show all (%d)</button>",
                        waveStats.size())
                : "";

        double successRate = (Double) overall.getOrDefault("success_rate", 0.0);
        String successRateClass = successRate >= 95 ? "success" : (successRate >= 80 ? "warning" : "danger");

        return generateHtmlTemplate(
                title, timestamp, csvPath,
                overall, successRateClass,
                histogramLabels, histogramBins,
                waveLabels, waveAvgs,
                threadLatencies, threadWaves,
                waveTableRows.toString(), showMoreBtn,
                threadSections.toString(),
                waveStats.size());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> computeStats(List<Map<String, Object>> results) {
        Map<Integer, List<Map<String, Object>>> waves = new TreeMap<>();
        for (Map<String, Object> item : results) {
            int waveNum = (Integer) item.getOrDefault("wave", 1);
            waves.computeIfAbsent(waveNum, k -> new ArrayList<>()).add(item);
        }

        List<Map<String, Object>> waveStats = new ArrayList<>();
        List<Double> allLatencies = new ArrayList<>();
        int totalSuccess = 0, totalFailed = 0;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : waves.entrySet()) {
            int waveNum = entry.getKey();
            List<Map<String, Object>> waveItems = entry.getValue();

            int success = 0, failed = 0;
            List<Double> latencies = new ArrayList<>();
            List<Double> step1Latencies = new ArrayList<>();
            List<Double> step2Latencies = new ArrayList<>();
            List<Double> step3Latencies = new ArrayList<>();
            List<Double> step4Latencies = new ArrayList<>();

            for (Map<String, Object> item : waveItems) {
                Map<String, Object> result = (Map<String, Object>) item.get("result");
                if (result != null && Boolean.TRUE.equals(result.get("success"))) {
                    success++;
                    double total = calculateTotalLatency(result);
                    latencies.add(total);
                    allLatencies.add(total);

                    Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
                    Map<String, Object> s2 = (Map<String, Object>) result.getOrDefault("step2", Map.of());
                    Map<String, Object> s3 = (Map<String, Object>) result.getOrDefault("step3", Map.of());
                    Map<String, Object> s4 = (Map<String, Object>) result.getOrDefault("step4", Map.of());

                    step1Latencies.add((Double) s1.getOrDefault("latency_ms", 0.0));
                    step2Latencies.add((Double) s2.getOrDefault("latency_ms", 0.0));
                    step3Latencies.add((Double) s3.getOrDefault("latency_ms", 0.0));

                    List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.getOrDefault("iterations",
                            List.of());
                    if (!iterations.isEmpty()) {
                        double s4Total = iterations.stream()
                                .mapToDouble(it -> (Double) it.getOrDefault("latency_ms", 0.0))
                                .sum();
                        step4Latencies.add(s4Total / iterations.size());
                    }
                } else {
                    failed++;
                }
            }

            totalSuccess += success;
            totalFailed += failed;

            Map<String, Object> waveStat = new HashMap<>();
            waveStat.put("wave", waveNum);
            waveStat.put("threads", waveItems.size());
            waveStat.put("success", success);
            waveStat.put("failed", failed);
            waveStat.put("rows", waveItems);

            if (!latencies.isEmpty()) {
                waveStat.put("min", Collections.min(latencies));
                waveStat.put("max", Collections.max(latencies));
                waveStat.put("avg", latencies.stream().mapToDouble(d -> d).average().orElse(0));
                List<Double> sorted = new ArrayList<>(latencies);
                Collections.sort(sorted);
                waveStat.put("p95", sorted.get((int) (sorted.size() * 0.95)));
                waveStat.put("stdev", calculateStdev(latencies));
                waveStat.put("step1_avg", step1Latencies.stream().mapToDouble(d -> d).average().orElse(0));
                waveStat.put("step2_avg", step2Latencies.stream().mapToDouble(d -> d).average().orElse(0));
                waveStat.put("step3_avg", step3Latencies.stream().mapToDouble(d -> d).average().orElse(0));
                waveStat.put("step4_avg", step4Latencies.stream().mapToDouble(d -> d).average().orElse(0));
            } else {
                waveStat.put("min", 0.0);
                waveStat.put("max", 0.0);
                waveStat.put("avg", 0.0);
                waveStat.put("p95", 0.0);
                waveStat.put("stdev", 0.0);
                waveStat.put("step1_avg", 0.0);
                waveStat.put("step2_avg", 0.0);
                waveStat.put("step3_avg", 0.0);
                waveStat.put("step4_avg", 0.0);
            }

            waveStats.add(waveStat);
        }

        Map<String, Object> overall = new HashMap<>();
        overall.put("total_threads", results.size());
        overall.put("success", totalSuccess);
        overall.put("failed", totalFailed);
        overall.put("success_rate", results.isEmpty() ? 0.0 : (double) totalSuccess / results.size() * 100);

        if (!allLatencies.isEmpty()) {
            overall.put("min", Collections.min(allLatencies));
            overall.put("max", Collections.max(allLatencies));
            overall.put("avg", allLatencies.stream().mapToDouble(d -> d).average().orElse(0));
            List<Double> sorted = new ArrayList<>(allLatencies);
            Collections.sort(sorted);
            overall.put("p50", sorted.get(sorted.size() / 2));
            overall.put("p95", sorted.get((int) (sorted.size() * 0.95)));
        } else {
            overall.put("min", 0.0);
            overall.put("max", 0.0);
            overall.put("avg", 0.0);
            overall.put("p50", 0.0);
            overall.put("p95", 0.0);
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("waves", waveStats);
        stats.put("overall", overall);
        stats.put("allLatencies", allLatencies);
        return stats;
    }

    @SuppressWarnings("unchecked")
    private static double calculateTotalLatency(Map<String, Object> result) {
        Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
        Map<String, Object> s2 = (Map<String, Object>) result.getOrDefault("step2", Map.of());
        Map<String, Object> s3 = (Map<String, Object>) result.getOrDefault("step3", Map.of());
        Map<String, Object> s4 = (Map<String, Object>) result.getOrDefault("step4", Map.of());

        double l1 = (Double) s1.getOrDefault("latency_ms", 0.0);
        double l2 = (Double) s2.getOrDefault("latency_ms", 0.0);
        double l3 = (Double) s3.getOrDefault("latency_ms", 0.0);

        List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.getOrDefault("iterations", List.of());
        double l4 = iterations.stream().mapToDouble(it -> (Double) it.getOrDefault("latency_ms", 0.0)).sum();

        return l1 + l2 + l3 + l4;
    }

    private static double calculateStdev(List<Double> values) {
        if (values.size() < 2)
            return 0;
        double mean = values.stream().mapToDouble(d -> d).average().orElse(0);
        double variance = values.stream().mapToDouble(d -> Math.pow(d - mean, 2)).average().orElse(0);
        return Math.sqrt(variance);
    }

    private static String toJson(List<?> list) {
        return list.stream()
                .map(item -> item instanceof String ? "\"" + item + "\"" : String.valueOf(item))
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String generateHtmlTemplate(
            String title, String timestamp, String csvPath,
            Map<String, Object> overall, String successRateClass,
            List<String> histogramLabels, List<Integer> histogramBins,
            List<String> waveLabels, List<Double> waveAvgs,
            List<Double> threadLatencies, List<Integer> threadWaves,
            String waveTableRows, String showMoreBtn,
            String threadSections, int waveCount) {

        List<Integer> threadIndices = new ArrayList<>();
        for (int i = 1; i <= threadLatencies.size(); i++)
            threadIndices.add(i);

        return String.format(
                """
                        <!DOCTYPE html>
                        <html lang="en">
                        <head>
                            <meta charset="UTF-8">
                            <meta name="viewport" content="width=device-width, initial-scale=1.0">
                            <title>%s - Performance Results</title>
                            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                            <style>
                                :root {
                                    --bg-primary: #0d1117;
                                    --bg-secondary: #161b22;
                                    --bg-tertiary: #21262d;
                                    --text-primary: #c9d1d9;
                                    --text-secondary: #8b949e;
                                    --accent: #58a6ff;
                                    --success: #3fb950;
                                    --warning: #d29922;
                                    --danger: #f85149;
                                    --border: #30363d;
                                }

                                * { box-sizing: border-box; margin: 0; padding: 0; }

                                body {
                                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                                    background: var(--bg-primary);
                                    color: var(--text-primary);
                                    line-height: 1.6;
                                    padding: 2rem;
                                }

                                .container { max-width: 1400px; margin: 0 auto; }

                                header {
                                    margin-bottom: 2rem;
                                    padding-bottom: 1rem;
                                    border-bottom: 1px solid var(--border);
                                }

                                h1 { font-size: 1.8rem; margin-bottom: 0.5rem; }
                                .meta { color: var(--text-secondary); font-size: 0.9rem; }

                                .cards {
                                    display: grid;
                                    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                                    gap: 1rem;
                                    margin-bottom: 2rem;
                                }

                                .card {
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    padding: 1rem;
                                }

                                .card-label { font-size: 0.8rem; color: var(--text-secondary); text-transform: uppercase; }
                                .card-value { font-size: 1.5rem; font-weight: 600; margin-top: 0.25rem; }
                                .card-value.success { color: var(--success); }
                                .card-value.warning { color: var(--warning); }
                                .card-value.danger { color: var(--danger); }

                                .section-header { display: flex; align-items: center; gap: 1rem; margin: 2rem 0 1rem; }
                                h2 { font-size: 1.2rem; color: var(--text-secondary); margin: 0; }

                                .toggle-btn {
                                    background: var(--bg-tertiary);
                                    border: 1px solid var(--border);
                                    color: var(--accent);
                                    padding: 0.25rem 0.75rem;
                                    border-radius: 4px;
                                    cursor: pointer;
                                    font-size: 0.8rem;
                                }
                                .toggle-btn:hover { background: var(--accent); color: var(--bg-primary); }

                                table {
                                    width: 100%%;
                                    border-collapse: collapse;
                                    background: var(--bg-secondary);
                                    border-radius: 8px;
                                    overflow: hidden;
                                }

                                th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }
                                th { background: var(--bg-tertiary); font-weight: 600; font-size: 0.85rem; text-transform: uppercase; color: var(--text-secondary); }
                                tr:last-child td { border-bottom: none; }
                                .hidden-row { display: none; }

                                .success { color: var(--success); }
                                .warning { color: var(--warning); }
                                .danger { color: var(--danger); }

                                .charts-grid {
                                    display: grid;
                                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                                    gap: 1.5rem;
                                    margin: 2rem 0;
                                }

                                .chart-container {
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    padding: 1.5rem;
                                }

                                .chart-title { font-size: 0.9rem; color: var(--text-secondary); margin-bottom: 1rem; text-transform: uppercase; }
                                .charts-toggle { display: none; }
                                .charts-toggle.visible { display: grid; }

                                .wave-details { margin-bottom: 1rem; }
                                .wave-details summary {
                                    cursor: pointer;
                                    padding: 0.75rem 1rem;
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    font-weight: 500;
                                    display: flex;
                                    align-items: center;
                                    gap: 1rem;
                                }
                                .wave-details summary:hover { background: var(--bg-tertiary); }
                                .wave-details[open] summary { border-radius: 8px 8px 0 0; border-bottom: none; }

                                .thread-table { border-radius: 0 0 8px 8px; font-size: 0.9rem; }
                                .thread-table th { font-size: 0.75rem; }

                                tr.outlier { background: rgba(248, 81, 73, 0.1); }
                                tr.failure { background: rgba(248, 81, 73, 0.15); }
                                .outlier-value { color: var(--danger); font-weight: 600; }
                                .error-cell { color: var(--danger); font-size: 0.8rem; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
                                .timestamp-cell { font-size: 0.75rem; color: var(--text-secondary); white-space: nowrap; }

                                .legend {
                                    margin-top: 2rem;
                                    padding: 1rem;
                                    background: var(--bg-secondary);
                                    border-radius: 8px;
                                    font-size: 0.85rem;
                                    color: var(--text-secondary);
                                }
                                .legend-item { display: inline-block; margin-right: 2rem; }
                                .legend-color { display: inline-block; width: 12px; height: 12px; border-radius: 2px; margin-right: 0.5rem; vertical-align: middle; }
                                .legend-color.outlier { background: rgba(248, 81, 73, 0.3); }
                                .legend-color.failure { background: rgba(248, 81, 73, 0.5); }
                            </style>
                        </head>
                        <body>
                            <div class="container">
                                <header>
                                    <h1>%s</h1>
                                    <div class="meta">Generated: %s | Source: %s</div>
                                </header>

                                <div class="cards">
                                    <div class="card">
                                        <div class="card-label">Total Threads</div>
                                        <div class="card-value">%d</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">Success Rate</div>
                                        <div class="card-value %s">%.1f%%</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">Avg Latency</div>
                                        <div class="card-value">%.0f ms</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">P95 Latency</div>
                                        <div class="card-value">%.0f ms</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">Min / Max</div>
                                        <div class="card-value">%.0f / %.0f</div>
                                    </div>
                                </div>

                                <div class="section-header">
                                    <h2>Charts</h2>
                                    <button class="toggle-btn" id="chartsToggleBtn" onclick="toggleCharts(this)">View charts</button>
                                </div>
                                <div class="charts-grid charts-toggle" id="chartsContainer">
                                    <div class="chart-container">
                                        <div class="chart-title">Latency Distribution (Histogram)</div>
                                        <canvas id="histogramChart"></canvas>
                                    </div>
                                    <div class="chart-container">
                                        <div class="chart-title">Per-Wave Average Latency</div>
                                        <canvas id="waveChart"></canvas>
                                    </div>
                                    <div class="chart-container" style="grid-column: span 2; height: 300px;">
                                        <div class="chart-title">Per-Thread Completion Time</div>
                                        <canvas id="threadChart"></canvas>
                                    </div>
                                </div>

                                <div class="section-header">
                                    <h2>Per-Wave Summary</h2>
                                    %s
                                </div>
                                <table id="waveTable">
                                    <thead>
                                        <tr>
                                            <th>Wave</th>
                                            <th>Success</th>
                                            <th>Step1 Avg</th>
                                            <th>Step2 Avg</th>
                                            <th>Step3 Avg</th>
                                            <th>Step4 Avg</th>
                                            <th>Total Avg</th>
                                            <th>P95</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        %s
                                    </tbody>
                                </table>

                                <div class="section-header">
                                    <h2>Thread Details (click to expand)</h2>
                                </div>
                                %s

                                <div class="legend">
                                    <span class="legend-item"><span class="legend-color outlier"></span> Outlier (>2σ from mean)</span>
                                    <span class="legend-item"><span class="legend-color failure"></span> Failed request</span>
                                </div>
                            </div>

                            <script>
                                Chart.defaults.color = '#8b949e';
                                Chart.defaults.borderColor = '#30363d';

                                new Chart(document.getElementById('histogramChart'), {
                                    type: 'bar',
                                    data: {
                                        labels: %s,
                                        datasets: [{
                                            label: 'Threads',
                                            data: %s,
                                            backgroundColor: 'rgba(88, 166, 255, 0.6)',
                                            borderColor: 'rgba(88, 166, 255, 1)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        plugins: { legend: { display: false } },
                                        scales: {
                                            x: { title: { display: true, text: 'Latency (ms)' }, grid: { display: false } },
                                            y: { title: { display: true, text: 'Count' }, beginAtZero: true }
                                        }
                                    }
                                });

                                new Chart(document.getElementById('waveChart'), {
                                    type: 'line',
                                    data: {
                                        labels: %s,
                                        datasets: [{
                                            label: 'Avg Latency (ms)',
                                            data: %s,
                                            borderColor: '#3fb950',
                                            backgroundColor: 'rgba(63, 185, 80, 0.1)',
                                            fill: true,
                                            tension: 0.3
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        plugins: { legend: { display: false } },
                                        scales: { y: { beginAtZero: true, title: { display: true, text: 'Latency (ms)' } } }
                                    }
                                });

                                const threadWaves = %s;
                                const threadData = %s;
                                let activePoint = null;
                                const threadChart = new Chart(document.getElementById('threadChart'), {
                                    type: 'line',
                                    data: {
                                        labels: %s,
                                        datasets: [{
                                            label: 'Completion Time (ms)',
                                            data: threadData,
                                            borderColor: '#58a6ff',
                                            backgroundColor: 'rgba(88, 166, 255, 0.1)',
                                            fill: true,
                                            pointRadius: 0,
                                            tension: 0.1
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: { legend: { display: false } },
                                        scales: {
                                            x: { grid: { display: false } },
                                            y: { beginAtZero: true, title: { display: true, text: 'Latency (ms)' } }
                                        }
                                    }
                                });

                                function toggleWaveRows(btn) {
                                    const rows = document.querySelectorAll('[data-wave-row]');
                                    const isHidden = rows[10]?.classList.contains('hidden-row');
                                    rows.forEach((row, i) => { if (i >= 10) row.classList.toggle('hidden-row', !isHidden); });
                                    btn.textContent = isHidden ? 'Show less' : 'Show all (%d)';
                                }

                                function toggleThreadRows(waveNum, btn) {
                                    const rows = document.querySelectorAll(`[data-thread-row-${waveNum}]`);
                                    const hiddenRows = Array.from(rows).filter((_, i) => i >= 10);
                                    const isHidden = hiddenRows[0]?.classList.contains('hidden-row');
                                    hiddenRows.forEach(row => row.classList.toggle('hidden-row', !isHidden));
                                    btn.textContent = isHidden ? 'Show less' : `Show all (${rows.length})`;
                                }

                                function toggleCharts(btn) {
                                    const container = document.getElementById('chartsContainer');
                                    const isVisible = container.classList.contains('visible');
                                    container.classList.toggle('visible', !isVisible);
                                    btn.textContent = isVisible ? 'View charts' : 'Hide charts';
                                }
                            </script>
                        </body>
                        </html>
                                        """,
                title, title, timestamp, Paths.get(csvPath).getFileName(),
                overall.getOrDefault("total_threads", 0),
                successRateClass, overall.getOrDefault("success_rate", 0.0),
                overall.getOrDefault("avg", 0.0),
                overall.getOrDefault("p95", 0.0),
                overall.getOrDefault("min", 0.0), overall.getOrDefault("max", 0.0),
                showMoreBtn, waveTableRows, threadSections,
                toJson(histogramLabels), toJson(histogramBins),
                toJson(waveLabels), toJson(waveAvgs),
                toJson(threadWaves), toJson(threadLatencies),
                toJson(threadIndices),
                waveCount);
    }

    public static String saveHtml(String csvPath, Map<String, Object> runData, String outputPath) throws IOException {
        String htmlContent = generateHtml(csvPath, runData);

        String htmlPath;
        if (outputPath.endsWith(".html")) {
            htmlPath = outputPath;
        } else {
            htmlPath = outputPath.replace(".csv", ".html");
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(htmlPath))) {
            writer.print(htmlContent);
        }

        return htmlPath;
    }
}
