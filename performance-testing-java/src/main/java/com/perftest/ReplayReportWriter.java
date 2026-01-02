package com.perftest;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * HTML report generation for replay test results.
 * Features a Gantt-style timeline visualization showing request scheduling and
 * execution.
 */
public class ReplayReportWriter {

    /**
     * Event data structure for replay results.
     */
    public static class ReplayEvent {
        public long scheduledTime; // When the event was scheduled to fire (relative to start)
        public long actualTime; // When it actually fired
        public long latencyMs; // Request latency
        public boolean success;
        public String endpoint;
        public String user;
        public String error;

        public ReplayEvent(long scheduledTime, long actualTime, long latencyMs,
                boolean success, String endpoint, String user, String error) {
            this.scheduledTime = scheduledTime;
            this.actualTime = actualTime;
            this.latencyMs = latencyMs;
            this.success = success;
            this.endpoint = endpoint;
            this.user = user;
            this.error = error;
        }
    }

    /**
     * Generate HTML report from replay results.
     */
    /**
     * Generate HTML report from pre-aggregated stats.
     */
    public static String generateHtmlFromStats(
            String title, double speedFactor, long durationMs,
            int totalEvents, int successCount, int failCount,
            double avgLatency, long minLatency, long maxLatency, long p50, long p95, long p99,
            List<String> timeLabels, List<Integer> eventsPerSecond, List<Integer> successPerSecond,
            List<Integer> failPerSecond, List<Double> avgLatencyPerSecond,
            List<String> histogramLabels, List<Integer> histogramBins,
            List<ReplayEvent> sampleEvents, Map<String, Integer> errorCounts) {

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        double successRate = totalEvents > 0 ? (double) successCount / totalEvents * 100 : 0;

        // Build Gantt data (sample up to 200 events for visualization)
        List<ReplayEvent> ganttEvents = sampleEvents.size() <= 200 ? sampleEvents
                : sampleEvents.subList(0, 200);

        StringBuilder ganttData = new StringBuilder("[");
        for (int i = 0; i < ganttEvents.size(); i++) {
            ReplayEvent e = ganttEvents.get(i);
            if (i > 0)
                ganttData.append(",");
            ganttData.append(String.format(
                    "{x:[%d,%d],y:%d,success:%s}",
                    e.actualTime, e.actualTime + e.latencyMs, i,
                    e.success ? "true" : "false"));
        }
        ganttData.append("]");

        // Error summary HTML
        StringBuilder errorRows = new StringBuilder();
        errorCounts.entrySet().stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .limit(10)
                .forEach(entry -> {
                    errorRows.append(String.format("""
                            <tr>
                                <td class="error-count">%d</td>
                                <td class="error-message">%s</td>
                            </tr>
                            """, entry.getValue(),
                            entry.getKey().replace("<", "&lt;").replace(">", "&gt;")));
                });

        String successRateClass = successRate >= 95 ? "success" : (successRate >= 80 ? "warning" : "danger");

        return generateHtmlTemplate(
                title, timestamp, speedFactor, durationMs,
                totalEvents, successCount, failCount, successRate, successRateClass,
                avgLatency, minLatency, maxLatency, p50, p95, p99,
                toJson(timeLabels), toJson(eventsPerSecond),
                toJson(successPerSecond),
                toJson(failPerSecond),
                toJson(avgLatencyPerSecond),
                toJson(histogramLabels), toJson(histogramBins),
                ganttData.toString(), ganttEvents.size(),
                errorRows.toString(), errorCounts.size());
    }

    /**
     * Generate HTML report from full event list (legacy/non-streaming mode).
     */
    public static String generateHtml(String title, List<ReplayEvent> events,
            double speedFactor, long durationMs) {

        // Compute stats
        int totalEvents = events.size();
        int successCount = (int) events.stream().filter(e -> e.success).count();
        int failCount = totalEvents - successCount;

        List<Long> latencies = events.stream()
                .filter(e -> e.success && e.latencyMs > 0)
                .map(e -> e.latencyMs)
                .sorted()
                .collect(Collectors.toList());

        double avgLatency = latencies.isEmpty() ? 0 : latencies.stream().mapToLong(l -> l).average().orElse(0);
        long minLatency = latencies.isEmpty() ? 0 : Collections.min(latencies);
        long maxLatency = latencies.isEmpty() ? 0 : Collections.max(latencies);
        long p50 = latencies.isEmpty() ? 0 : latencies.get(latencies.size() / 2);
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        // Compute timeline data (bucket events by second)
        long maxTime = events.stream().mapToLong(e -> e.actualTime).max().orElse(durationMs);
        int bucketCount = (int) Math.min(Math.max(maxTime / 1000, 1), 300); // Max 300 buckets (5 min)
        long bucketSizeMs = maxTime / bucketCount;

        int[] eventsPerSecond = new int[bucketCount];
        int[] successPerSecond = new int[bucketCount];
        int[] failPerSecond = new int[bucketCount];
        double[] avgLatencyPerSecond = new double[bucketCount];
        int[] latencyCountPerSecond = new int[bucketCount];

        for (ReplayEvent event : events) {
            int bucket = (int) Math.min(event.actualTime / bucketSizeMs, bucketCount - 1);
            if (bucket >= 0 && bucket < bucketCount) {
                eventsPerSecond[bucket]++;
                if (event.success) {
                    successPerSecond[bucket]++;
                    avgLatencyPerSecond[bucket] += event.latencyMs;
                    latencyCountPerSecond[bucket]++;
                } else {
                    failPerSecond[bucket]++;
                }
            }
        }

        // Finalize latency averages
        for (int i = 0; i < bucketCount; i++) {
            if (latencyCountPerSecond[i] > 0) {
                avgLatencyPerSecond[i] /= latencyCountPerSecond[i];
            }
        }

        // Build timeline labels
        List<String> timeLabels = new ArrayList<>();
        List<Integer> eventsList = new ArrayList<>();
        List<Integer> successList = new ArrayList<>();
        List<Integer> failList = new ArrayList<>();
        List<Double> avgLatencyList = new ArrayList<>();

        for (int i = 0; i < bucketCount; i++) {
            long ms = i * bucketSizeMs;
            int seconds = (int) (ms / 1000);
            int minutes = seconds / 60;
            seconds = seconds % 60;
            timeLabels.add(String.format("%d:%02d", minutes, seconds));
            eventsList.add(eventsPerSecond[i]);
            successList.add(successPerSecond[i]);
            failList.add(failPerSecond[i]);
            avgLatencyList.add(avgLatencyPerSecond[i]);
        }

        // Build histogram bins
        List<Integer> histogramBins = new ArrayList<>();
        List<String> histogramLabels = new ArrayList<>();
        if (!latencies.isEmpty()) {
            int binCount = Math.min(20, latencies.size());
            long binMin = minLatency;
            long binMax = maxLatency;
            long binWidth = binCount > 0 ? Math.max((binMax - binMin) / binCount, 1) : 1;

            for (int i = 0; i < binCount; i++) {
                histogramBins.add(0);
                histogramLabels.add(String.format("%d-%d", binMin + i * binWidth, binMin + (i + 1) * binWidth));
            }

            for (long lat : latencies) {
                int bin = (int) ((lat - binMin) / binWidth);
                if (bin >= binCount)
                    bin = binCount - 1;
                if (bin >= 0 && bin < binCount) {
                    histogramBins.set(bin, histogramBins.get(bin) + 1);
                }
            }
        }

        // Error summary
        Map<String, Integer> errorCounts = new HashMap<>();
        for (ReplayEvent event : events) {
            if (!event.success && event.error != null && !event.error.isEmpty()) {
                String truncatedError = event.error.length() > 80
                        ? event.error.substring(0, 80) + "..."
                        : event.error;
                errorCounts.merge(truncatedError, 1, Integer::sum);
            }
        }

        return generateHtmlFromStats(
                title, speedFactor, durationMs,
                totalEvents, successCount, failCount,
                avgLatency, minLatency, maxLatency, p50, p95, p99,
                timeLabels, eventsList, successList, failList, avgLatencyList,
                histogramLabels, histogramBins,
                events, errorCounts);
    }

    private static String toJson(List<?> list) {
        return list.stream()
                .map(item -> item instanceof String ? "\"" + item + "\"" : String.valueOf(item))
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String generateHtmlTemplate(
            String title, String timestamp, double speedFactor, long durationMs,
            int totalEvents, int successCount, int failCount, double successRate, String successRateClass,
            double avgLatency, long minLatency, long maxLatency, long p50, long p95, long p99,
            String timeLabels, String eventsPerSecond, String successPerSecond, String failPerSecond,
            String avgLatencyPerSecond, String histogramLabels, String histogramBins,
            String ganttData, int ganttSize, String errorRows, int errorCount) {

        return String.format(
                """
                        <!DOCTYPE html>
                        <html lang="en">
                        <head>
                            <meta charset="UTF-8">
                            <meta name="viewport" content="width=device-width, initial-scale=1.0">
                            <title>%s - Replay Results</title>
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
                                .badge {
                                    display: inline-block;
                                    background: var(--accent);
                                    color: var(--bg-primary);
                                    padding: 0.2rem 0.6rem;
                                    border-radius: 12px;
                                    font-size: 0.8rem;
                                    font-weight: 600;
                                    margin-left: 0.5rem;
                                }

                                .cards {
                                    display: grid;
                                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                                    gap: 1rem;
                                    margin-bottom: 2rem;
                                }

                                .card {
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    padding: 1rem;
                                }

                                .card-label { font-size: 0.75rem; color: var(--text-secondary); text-transform: uppercase; }
                                .card-value { font-size: 1.4rem; font-weight: 600; margin-top: 0.25rem; }
                                .card-value.success { color: var(--success); }
                                .card-value.warning { color: var(--warning); }
                                .card-value.danger { color: var(--danger); }

                                h2 { font-size: 1.1rem; color: var(--text-secondary); margin: 2rem 0 1rem; }

                                .charts-grid {
                                    display: grid;
                                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                                    gap: 1.5rem;
                                    margin: 1rem 0 2rem;
                                }

                                .chart-container {
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    padding: 1.5rem;
                                }

                                .chart-title { font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 1rem; text-transform: uppercase; }

                                .gantt-container {
                                    background: var(--bg-secondary);
                                    border: 1px solid var(--border);
                                    border-radius: 8px;
                                    padding: 1.5rem;
                                    margin-bottom: 2rem;
                                }

                                .gantt-chart {
                                    height: 400px;
                                    overflow-y: auto;
                                }

                                table {
                                    width: 100%%;
                                    border-collapse: collapse;
                                    background: var(--bg-secondary);
                                    border-radius: 8px;
                                    overflow: hidden;
                                }

                                th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }
                                th { background: var(--bg-tertiary); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; color: var(--text-secondary); }
                                tr:last-child td { border-bottom: none; }
                                .error-count { color: var(--danger); font-weight: 600; width: 60px; text-align: center; }
                                .error-message { font-family: 'Monaco', 'Menlo', monospace; font-size: 0.85rem; color: var(--text-primary); }
                                .no-errors { padding: 1.5rem; text-align: center; color: var(--success); }
                            </style>
                        </head>
                        <body>
                            <div class="container">
                                <header>
                                    <h1>%s <span class="badge">%.1fx speed</span></h1>
                                    <div class="meta">Generated: %s | Duration: %.1f seconds | Original duration: %.1f minutes</div>
                                </header>

                                <div class="cards">
                                    <div class="card">
                                        <div class="card-label">Total Events</div>
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
                                        <div class="card-label">P50 / P95</div>
                                        <div class="card-value">%d / %d ms</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">P99</div>
                                        <div class="card-value">%d ms</div>
                                    </div>
                                    <div class="card">
                                        <div class="card-label">Min / Max</div>
                                        <div class="card-value">%d / %d ms</div>
                                    </div>
                                </div>

                                <h2>📈 Traffic Over Time</h2>
                                <div class="charts-grid">
                                    <div class="chart-container">
                                        <div class="chart-title">Requests Per Second</div>
                                        <canvas id="rpsChart"></canvas>
                                    </div>
                                    <div class="chart-container">
                                        <div class="chart-title">Average Latency Over Time</div>
                                        <canvas id="latencyTimeChart"></canvas>
                                    </div>
                                </div>

                                <h2>📊 Latency Distribution</h2>
                                <div class="charts-grid">
                                    <div class="chart-container">
                                        <div class="chart-title">Histogram (ms)</div>
                                        <canvas id="histogramChart"></canvas>
                                    </div>
                                </div>

                                <h2>📋 Request Timeline (first %d requests)</h2>
                                <div class="gantt-container">
                                    <div class="chart-title">Gantt Chart - Request Execution</div>
                                    <div class="gantt-chart">
                                        <canvas id="ganttChart"></canvas>
                                    </div>
                                </div>

                                <h2>⚠️ Error Summary (%d unique errors)</h2>
                                %s

                            </div>

                            <script>
                                const timeLabels = %s;
                                const eventsPerSecond = %s;
                                const successPerSecond = %s;
                                const failPerSecond = %s;
                                const avgLatencyPerSecond = %s;
                                const histogramLabels = %s;
                                const histogramBins = %s;
                                const ganttData = %s;

                                // RPS Chart
                                new Chart(document.getElementById('rpsChart'), {
                                    type: 'line',
                                    data: {
                                        labels: timeLabels,
                                        datasets: [{
                                            label: 'Success',
                                            data: successPerSecond,
                                            borderColor: '#3fb950',
                                            backgroundColor: 'rgba(63, 185, 80, 0.1)',
                                            fill: true,
                                            tension: 0.3
                                        }, {
                                            label: 'Failed',
                                            data: failPerSecond,
                                            borderColor: '#f85149',
                                            backgroundColor: 'rgba(248, 81, 73, 0.1)',
                                            fill: true,
                                            tension: 0.3
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        scales: {
                                            x: { ticks: { color: '#8b949e', maxTicksLimit: 15 }, grid: { color: '#30363d' } },
                                            y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' }, beginAtZero: true }
                                        },
                                        plugins: { legend: { labels: { color: '#c9d1d9' } } }
                                    }
                                });

                                // Latency over time
                                new Chart(document.getElementById('latencyTimeChart'), {
                                    type: 'line',
                                    data: {
                                        labels: timeLabels,
                                        datasets: [{
                                            label: 'Avg Latency (ms)',
                                            data: avgLatencyPerSecond,
                                            borderColor: '#58a6ff',
                                            backgroundColor: 'rgba(88, 166, 255, 0.1)',
                                            fill: true,
                                            tension: 0.3
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        scales: {
                                            x: { ticks: { color: '#8b949e', maxTicksLimit: 15 }, grid: { color: '#30363d' } },
                                            y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' }, beginAtZero: true }
                                        },
                                        plugins: { legend: { labels: { color: '#c9d1d9' } } }
                                    }
                                });

                                // Histogram
                                new Chart(document.getElementById('histogramChart'), {
                                    type: 'bar',
                                    data: {
                                        labels: histogramLabels,
                                        datasets: [{
                                            label: 'Requests',
                                            data: histogramBins,
                                            backgroundColor: 'rgba(88, 166, 255, 0.6)',
                                            borderColor: '#58a6ff',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: {
                                        responsive: true,
                                        scales: {
                                            x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } },
                                            y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' }, beginAtZero: true }
                                        },
                                        plugins: { legend: { display: false } }
                                    }
                                });

                                // Gantt Chart (horizontal bar)
                                const ganttDatasets = ganttData.map((d, i) => ({
                                    label: 'Request ' + i,
                                    data: [{ x: d.x, y: d.y }],
                                    backgroundColor: d.success ? 'rgba(63, 185, 80, 0.7)' : 'rgba(248, 81, 73, 0.7)',
                                    barThickness: 4
                                }));

                                new Chart(document.getElementById('ganttChart'), {
                                    type: 'bar',
                                    data: { datasets: ganttDatasets },
                                    options: {
                                        indexAxis: 'y',
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        scales: {
                                            x: {
                                                type: 'linear',
                                                position: 'top',
                                                title: { display: true, text: 'Time (ms)', color: '#8b949e' },
                                                ticks: { color: '#8b949e' },
                                                grid: { color: '#30363d' }
                                            },
                                            y: {
                                                type: 'linear',
                                                ticks: { color: '#8b949e', display: false },
                                                grid: { color: '#30363d' }
                                            }
                                        },
                                        plugins: { legend: { display: false } }
                                    }
                                });
                            </script>
                        </body>
                        </html>
                        """,
                title, // <title>
                title, speedFactor, // h1 with badge
                timestamp, durationMs / 1000.0, (durationMs * speedFactor) / 60000.0, // meta
                totalEvents, // total events card
                successRateClass, successRate, // success rate card
                avgLatency, // avg latency card
                p50, p95, // p50/p95 card
                p99, // p99 card
                minLatency, maxLatency, // min/max card
                ganttSize, // gantt section title
                errorCount, // error section title
                errorCount > 0
                        ? "<table><thead><tr><th>Count</th><th>Error Message</th></tr></thead><tbody>" + errorRows
                                + "</tbody></table>"
                        : "<div class=\"no-errors\">✓ No errors recorded</div>",
                timeLabels, eventsPerSecond, successPerSecond, failPerSecond, avgLatencyPerSecond,
                histogramLabels, histogramBins, ganttData);
    }

    /**
     * Save HTML report to file.
     */
    public static String saveHtml(String title, List<ReplayEvent> events,
            double speedFactor, long durationMs, String outputPath) throws IOException {
        String html = generateHtml(title, events, speedFactor, durationMs);

        String htmlPath = outputPath.endsWith(".html") ? outputPath : outputPath + ".html";

        try (PrintWriter writer = new PrintWriter(new FileWriter(htmlPath))) {
            writer.write(html);
        }

        return htmlPath;
    }
}
