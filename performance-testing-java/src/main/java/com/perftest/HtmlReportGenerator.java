package com.perftest;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Standalone CLI tool to regenerate HTML reports from existing CSV files.
 * 
 * Usage:
 * java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator
 * results/test.csv
 * java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator
 * results/test.csv -o custom.html
 */
public class HtmlReportGenerator {

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String csvPath = args[0];
        String outputPath = null;

        // Parse optional -o flag
        for (int i = 1; i < args.length; i++) {
            if ("-o".equals(args[i]) && i + 1 < args.length) {
                outputPath = args[i + 1];
                break;
            }
        }

        try {
            Path csvFile = Paths.get(csvPath);
            if (!Files.exists(csvFile)) {
                System.err.println("Error: CSV file not found: " + csvPath);
                System.exit(1);
            }

            System.out.println("Loading CSV: " + csvPath);
            Map<String, Object> runData = loadCsvToRunData(csvPath);

            if (outputPath == null) {
                outputPath = csvPath.replace(".csv", ".html");
            }

            String htmlPath = HtmlReportWriter.saveHtml(csvPath, runData, outputPath);
            System.out.println("HTML report generated: " + htmlPath);

        } catch (Exception e) {
            System.err.println("Error generating HTML: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("HTML Report Generator - Convert CSV results to interactive HTML dashboard");
        System.out.println();
        System.out.println("Usage:");
        System.out.println(
                "  java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator <csv-file> [-o <output.html>]");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator results/test.csv");
        System.out.println(
                "  java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator results/test.csv -o dashboard.html");
    }

    private static Map<String, Object> loadCsvToRunData(String csvPath) throws IOException, CsvException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            List<String[]> allRows = reader.readAll();

            if (allRows.isEmpty()) {
                throw new IOException("CSV file is empty");
            }

            String[] headers = allRows.get(0);
            Map<String, Integer> headerIndex = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                headerIndex.put(headers[i], i);
            }

            for (int rowIdx = 1; rowIdx < allRows.size(); rowIdx++) {
                String[] row = allRows.get(rowIdx);

                Map<String, Object> item = new HashMap<>();
                item.put("wave", parseIntSafe(getColumn(row, headerIndex, "wave"), 1));
                item.put("thread", parseIntSafe(getColumn(row, headerIndex, "thread"), rowIdx));

                boolean success = "true".equalsIgnoreCase(getColumn(row, headerIndex, "success"));
                String error = getColumn(row, headerIndex, "error");

                Map<String, Object> result = new HashMap<>();
                result.put("success", success);

                if (!success && error != null && !error.isEmpty()) {
                    result.put("error", error);
                }

                // Step 1
                Map<String, Object> step1 = new HashMap<>();
                step1.put("uid", getColumn(row, headerIndex, "uid"));
                step1.put("latency_ms", parseDoubleSafe(getColumn(row, headerIndex, "step1_ms")));
                step1.put("start_timestamp", parseTimestamp(getColumn(row, headerIndex, "step1_start")));
                step1.put("end_timestamp", parseTimestamp(getColumn(row, headerIndex, "step1_end")));
                result.put("step1", step1);

                // Step 2
                Map<String, Object> step2 = new HashMap<>();
                step2.put("latency_ms", parseDoubleSafe(getColumn(row, headerIndex, "step2_ms")));
                step2.put("start_timestamp", parseTimestamp(getColumn(row, headerIndex, "step2_start")));
                step2.put("end_timestamp", parseTimestamp(getColumn(row, headerIndex, "step2_end")));
                result.put("step2", step2);

                // Step 3
                Map<String, Object> step3 = new HashMap<>();
                step3.put("latency_ms", parseDoubleSafe(getColumn(row, headerIndex, "step3_ms")));
                step3.put("start_timestamp", parseTimestamp(getColumn(row, headerIndex, "step3_start")));
                step3.put("end_timestamp", parseTimestamp(getColumn(row, headerIndex, "step3_end")));
                result.put("step3", step3);

                // Step 4 - reconstruct from avg and total
                Map<String, Object> step4 = new HashMap<>();
                double step4Avg = parseDoubleSafe(getColumn(row, headerIndex, "step4_avg_ms"));
                double step4Total = parseDoubleSafe(getColumn(row, headerIndex, "step4_total_ms"));
                step4.put("start_timestamp", parseTimestamp(getColumn(row, headerIndex, "step4_start")));
                step4.put("end_timestamp", parseTimestamp(getColumn(row, headerIndex, "step4_end")));

                // Reconstruct iterations (estimate count from avg/total)
                List<Map<String, Object>> iterations = new ArrayList<>();
                if (step4Avg > 0 && step4Total > 0) {
                    int iterCount = (int) Math.round(step4Total / step4Avg);
                    for (int i = 0; i < iterCount; i++) {
                        Map<String, Object> iter = new HashMap<>();
                        iter.put("latency_ms", step4Avg);
                        iterations.add(iter);
                    }
                }
                step4.put("iterations", iterations);
                result.put("step4", step4);

                item.put("result", result);
                results.add(item);
            }
        }

        // Extract title from filename
        String filename = Paths.get(csvPath).getFileName().toString();
        String title = filename.replace(".csv", "").replace("_v2", "");

        Map<String, Object> runData = new HashMap<>();
        runData.put("title", title);
        runData.put("results", results);

        System.out.println("Loaded " + results.size() + " rows from CSV");
        return runData;
    }

    private static String getColumn(String[] row, Map<String, Integer> headerIndex, String column) {
        Integer idx = headerIndex.get(column);
        if (idx == null || idx >= row.length) {
            return "";
        }
        String val = row[idx];
        if (val == null)
            return "";
        val = val.trim();
        // Remove surrounding quotes if present
        if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    private static int parseIntSafe(String value, int defaultVal) {
        if (value == null || value.isEmpty()) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private static double parseDoubleSafe(String value) {
        if (value == null || value.isEmpty()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static Long parseTimestamp(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        value = value.trim();
        // Just in case getColumn didn't catch it
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }

        try {
            // Try standard format: yyyy-MM-dd HH:mm:ss.SSS
            java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(value, fmt);
            return ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e1) {
            try {
                // Try format without millis: yyyy-MM-dd HH:mm:ss
                java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter
                        .ofPattern("yyyy-MM-dd HH:mm:ss");
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(value, fmt);
                return ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e2) {
                try {
                    // Try ISO format
                    java.time.OffsetDateTime odt = java.time.OffsetDateTime.parse(value);
                    return odt.toInstant().toEpochMilli();
                } catch (Exception e3) {
                    return null;
                }
            }
        }
    }
}
