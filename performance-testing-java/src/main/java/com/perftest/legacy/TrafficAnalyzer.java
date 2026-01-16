package com.perftest.legacy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Traffic Analyzer - Analyzes JSONL traffic files and generates HTML reports
 * showing RPS (requests per second) over time.
 * 
 * Usage:
 * java -cp target/api-flow-v2.jar com.perftest.TrafficAnalyzer <jsonl-file>
 * [options]
 */
public class TrafficAnalyzer {

    private final String jsonlPath;
    private final double speedFactor;
    private final boolean verbose;

    public TrafficAnalyzer(String jsonlPath, double speedFactor, boolean verbose) {
        this.jsonlPath = jsonlPath;
        this.speedFactor = speedFactor;
        this.verbose = verbose;
    }

    /**
     * Analyze the traffic file and generate an HTML report.
     */
    public void analyze() throws IOException {
        System.out.println("Analyzing: " + jsonlPath);
        System.out.printf("Speed factor: %.1fx%n", speedFactor);

        long fileSize = Files.size(Paths.get(jsonlPath));
        System.out.printf("File size: %.2f GB%n", fileSize / (1024.0 * 1024 * 1024));

        // Count events per second
        Map<Long, Integer> eventsPerSecond = new TreeMap<>();
        Map<String, Integer> endpointCounts = new HashMap<>();
        Map<String, Integer> seriesCounts = new HashMap<>();
        long scanFirstTs = -1;
        int totalEvents = 0;

        System.out.println("Scanning file...");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(jsonlPath), "UTF-8"),
                4 * 1024 * 1024)) {

            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                if (line.trim().isEmpty())
                    continue;

                try {
                    JsonObject event = JsonParser.parseString(line).getAsJsonObject();
                    if (event.has("ts")) {
                        long ts = event.get("ts").getAsLong();

                        if (scanFirstTs < 0) {
                            scanFirstTs = ts;
                        }

                        long delayMs = (long) ((ts - scanFirstTs) / speedFactor);
                        long secondKey = delayMs / 1000;
                        eventsPerSecond.merge(secondKey, 1, Integer::sum);
                        totalEvents++;

                        // Track endpoint distribution
                        if (event.has("endpoint")) {
                            String endpoint = event.get("endpoint").getAsString();
                            endpointCounts.merge(endpoint, 1, Integer::sum);
                        }

                        // Track series distribution
                        if (event.has("series")) {
                            String series = event.get("series").getAsString();
                            seriesCounts.merge(series, 1, Integer::sum);
                        }
                    }
                } catch (Exception e) {
                    // Skip malformed lines
                }

                if (lineCount % 500000 == 0) {
                    System.out.printf("Processed %,d lines...%n", lineCount);
                }
            }
        }

        // Calculate statistics
        int maxRps = eventsPerSecond.values().stream().max(Integer::compareTo).orElse(0);
        double avgRps = eventsPerSecond.values().stream().mapToInt(i -> i).average().orElse(0);
        int minRps = eventsPerSecond.values().stream().min(Integer::compareTo).orElse(0);
        long durationSeconds = eventsPerSecond.isEmpty() ? 0
                : Collections.max(eventsPerSecond.keySet()) - Collections.min(eventsPerSecond.keySet()) + 1;

        System.out.println("\n" + "=".repeat(60));
        System.out.println("TRAFFIC ANALYSIS SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("  Total Events:      %,d%n", totalEvents);
        System.out.printf("  Duration:          %,d seconds (%.1f hours)%n", durationSeconds, durationSeconds / 3600.0);
        System.out.printf("  Max RPS:           %,d%n", maxRps);
        System.out.printf("  Avg RPS:           %.1f%n", avgRps);
        System.out.printf("  Min RPS:           %,d%n", minRps);
        System.out.println("=".repeat(60));

        // Generate HTML report
        String baseName = Paths.get(jsonlPath).getFileName().toString().replace(".jsonl", "");
        String htmlPath = String.format("traffic_analysis_%s.html", baseName);
        generateHtmlReport(htmlPath, baseName, eventsPerSecond, endpointCounts, seriesCounts,
                totalEvents, durationSeconds, maxRps, avgRps, minRps);

        System.out.println("\nHTML report saved to: " + htmlPath);
    }

    private void generateHtmlReport(String outputPath, String title,
            Map<Long, Integer> eventsPerSecond,
            Map<String, Integer> endpointCounts,
            Map<String, Integer> seriesCounts,
            int totalEvents, long durationSeconds,
            int maxRps, double avgRps, int minRps) throws IOException {

        // Use all data points (no downsampling)
        List<String> formattedLabels = new ArrayList<>();
        List<Integer> rpsValues = new ArrayList<>();

        for (Map.Entry<Long, Integer> entry : eventsPerSecond.entrySet()) {
            long sec = entry.getKey();
            long hours = sec / 3600;
            long mins = (sec % 3600) / 60;
            long secs = sec % 60;
            // Only show HH:MM for cleaner labels (Chart.js will auto-thin anyway)
            formattedLabels.add(String.format("%d:%02d", hours, mins));
            rpsValues.add(entry.getValue());
        }

        // Sort endpoints by count
        List<Map.Entry<String, Integer>> sortedEndpoints = new ArrayList<>(endpointCounts.entrySet());
        sortedEndpoints.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Sort series by count (top 20)
        List<Map.Entry<String, Integer>> sortedSeries = new ArrayList<>(seriesCounts.entrySet());
        sortedSeries.sort((a, b) -> b.getValue().compareTo(a.getValue()));
        if (sortedSeries.size() > 20) {
            sortedSeries = sortedSeries.subList(0, 20);
        }

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"en\">\n<head>\n");
        html.append("<meta charset=\"UTF-8\">\n");
        html.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("<title>Traffic Analysis: ").append(title).append("</title>\n");
        html.append("<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n");
        html.append("<style>\n");
        html.append("body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; ");
        html.append("margin: 0; padding: 20px; background: #1a1a2e; color: #eee; }\n");
        html.append("h1 { color: #00d9ff; margin-bottom: 10px; }\n");
        html.append(".container { max-width: 1400px; margin: 0 auto; }\n");
        html.append(
                ".stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }\n");
        html.append(".stat-card { background: #16213e; border-radius: 12px; padding: 20px; text-align: center; }\n");
        html.append(".stat-value { font-size: 2.5em; font-weight: bold; color: #00d9ff; }\n");
        html.append(".stat-label { color: #888; margin-top: 5px; }\n");
        html.append(".chart-container { background: #16213e; border-radius: 12px; padding: 20px; margin: 20px 0; }\n");
        html.append(".chart-title { color: #00d9ff; margin-bottom: 15px; }\n");
        html.append("table { width: 100%; border-collapse: collapse; margin-top: 10px; }\n");
        html.append("th, td { padding: 10px; text-align: left; border-bottom: 1px solid #333; }\n");
        html.append("th { color: #00d9ff; }\n");
        html.append(
                ".bar { background: linear-gradient(90deg, #00d9ff, #0f3460); height: 20px; border-radius: 4px; }\n");
        html.append("</style>\n</head>\n<body>\n");
        html.append("<div class=\"container\">\n");
        html.append("<h1>Traffic Analysis: ").append(title).append("</h1>\n");
        html.append("<p style=\"color: #888;\">Speed factor: ").append(String.format("%.1fx", speedFactor))
                .append("</p>\n");

        // Stats cards
        html.append("<div class=\"stats-grid\">\n");
        html.append("<div class=\"stat-card\"><div class=\"stat-value\">").append(String.format("%,d", totalEvents))
                .append("</div><div class=\"stat-label\">Total Events</div></div>\n");
        html.append("<div class=\"stat-card\"><div class=\"stat-value\">")
                .append(String.format("%.1f", durationSeconds / 3600.0))
                .append("h</div><div class=\"stat-label\">Duration</div></div>\n");
        html.append("<div class=\"stat-card\"><div class=\"stat-value\">").append(String.format("%,d", maxRps))
                .append("</div><div class=\"stat-label\">Max RPS</div></div>\n");
        html.append("<div class=\"stat-card\"><div class=\"stat-value\">").append(String.format("%.1f", avgRps))
                .append("</div><div class=\"stat-label\">Avg RPS</div></div>\n");
        html.append("<div class=\"stat-card\"><div class=\"stat-value\">").append(String.format("%,d", minRps))
                .append("</div><div class=\"stat-label\">Min RPS</div></div>\n");
        html.append("</div>\n");

        // RPS Chart
        html.append("<div class=\"chart-container\">\n");
        html.append("<h2 class=\"chart-title\">Requests Per Second Over Time</h2>\n");
        html.append("<canvas id=\"rpsChart\" height=\"100\"></canvas>\n");
        html.append("</div>\n");

        // Endpoint distribution
        html.append("<div class=\"chart-container\">\n");
        html.append("<h2 class=\"chart-title\">Endpoint Distribution</h2>\n");
        html.append("<table>\n<tr><th>Endpoint</th><th>Count</th><th>%</th><th></th></tr>\n");
        for (Map.Entry<String, Integer> entry : sortedEndpoints) {
            double pct = 100.0 * entry.getValue() / totalEvents;
            html.append("<tr><td>").append(entry.getKey()).append("</td>");
            html.append("<td>").append(String.format("%,d", entry.getValue())).append("</td>");
            html.append("<td>").append(String.format("%.1f%%", pct)).append("</td>");
            html.append("<td><div class=\"bar\" style=\"width: ").append(String.format("%.1f", pct))
                    .append("%\"></div></td></tr>\n");
        }
        html.append("</table>\n</div>\n");

        // Series distribution
        html.append("<div class=\"chart-container\">\n");
        html.append("<h2 class=\"chart-title\">Top 20 Series</h2>\n");
        html.append("<table>\n<tr><th>Series</th><th>Count</th><th>%</th><th></th></tr>\n");
        for (Map.Entry<String, Integer> entry : sortedSeries) {
            double pct = 100.0 * entry.getValue() / totalEvents;
            html.append("<tr><td>").append(entry.getKey()).append("</td>");
            html.append("<td>").append(String.format("%,d", entry.getValue())).append("</td>");
            html.append("<td>").append(String.format("%.1f%%", pct)).append("</td>");
            html.append("<td><div class=\"bar\" style=\"width: ").append(String.format("%.1f", Math.min(pct * 5, 100)))
                    .append("%\"></div></td></tr>\n");
        }
        html.append("</table>\n</div>\n");

        // Chart.js script
        html.append("<script>\n");
        html.append("const ctx = document.getElementById('rpsChart').getContext('2d');\n");
        html.append("new Chart(ctx, {\n");
        html.append("  type: 'line',\n");
        html.append("  data: {\n");
        html.append("    labels: ").append(toJsonArray(formattedLabels)).append(",\n");
        html.append("    datasets: [{\n");
        html.append("      label: 'RPS',\n");
        html.append("      data: ").append(rpsValues).append(",\n");
        html.append("      borderColor: '#00d9ff',\n");
        html.append("      backgroundColor: 'rgba(0, 217, 255, 0.1)',\n");
        html.append("      fill: true,\n");
        html.append("      tension: 0.3,\n");
        html.append("      pointRadius: 0\n");
        html.append("    }]\n");
        html.append("  },\n");
        html.append("  options: {\n");
        html.append("    responsive: true,\n");
        html.append("    plugins: { legend: { labels: { color: '#eee' } } },\n");
        html.append("    scales: {\n");
        html.append("      x: { ticks: { color: '#888', maxTicksLimit: 20 }, grid: { color: '#333' } },\n");
        html.append(
                "      y: { ticks: { color: '#888' }, grid: { color: '#333' }, title: { display: true, text: 'Requests/Second', color: '#888' } }\n");
        html.append("    }\n");
        html.append("  }\n");
        html.append("});\n");
        html.append("</script>\n");

        html.append("</div>\n</body>\n</html>");

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
            writer.write(html.toString());
        }
    }

    private String toJsonArray(List<String> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0)
                sb.append(",");
            sb.append("\"").append(list.get(i)).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: TrafficAnalyzer <jsonl-file> [options]");
            System.out.println("Options:");
            System.out.println("  --speed <factor>   Speed factor for time scaling (default: 1.0)");
            System.out.println("  -v, --verbose      Verbose output");
            System.out.println();
            System.out.println("Generates an HTML report showing RPS over time.");
            return;
        }

        String jsonlPath = args[0];
        double speedFactor = 1.0;
        boolean verbose = false;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--speed":
                    speedFactor = Double.parseDouble(args[++i]);
                    break;
                case "-v":
                case "--verbose":
                    verbose = true;
                    break;
            }
        }

        TrafficAnalyzer analyzer = new TrafficAnalyzer(jsonlPath, speedFactor, verbose);
        try {
            analyzer.analyze();
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
