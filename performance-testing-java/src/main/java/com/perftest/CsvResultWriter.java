package com.perftest;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * CSV output handling for test results.
 */
public class CsvResultWriter {

    private static final String[] HEADERS = {
            "wave", "thread", "uid", "success", "error",
            "step1_start", "step1_end", "step1_ms",
            "step2_start", "step2_end", "step2_ms",
            "step3_start", "step3_end", "step3_ms",
            "step4_start", "step4_end", "step4_avg_ms", "step4_total_ms",
            "total_ms"
    };

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    static String formatTimestamp(Object timestamp) {
        if (timestamp == null) {
            return "";
        }
        long millis = ((Number) timestamp).longValue();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault())
                .format(TIMESTAMP_FORMAT);
    }

    @SuppressWarnings("unchecked")
    public static String saveResults(Map<String, Object> runData, String outputPath) throws IOException {
        String title = (String) runData.getOrDefault("title", "run");
        String safeTitle = title.replaceAll("[^\\w\\s-]", "").replace(" ", "_");

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));

        String filepath;
        if (outputPath.endsWith(".csv")) {
            filepath = outputPath;
        } else {
            Path dir = Paths.get(outputPath);
            Files.createDirectories(dir);
            String filename = safeTitle.isEmpty() ? timestamp + "_v2.csv" : timestamp + "_" + safeTitle + "_v2.csv";
            filepath = dir.resolve(filename).toString();
        }

        List<Map<String, Object>> results = (List<Map<String, Object>>) runData.get("results");

        try (CSVWriter writer = new CSVWriter(new FileWriter(filepath))) {
            writer.writeNext(HEADERS);

            for (Map<String, Object> item : results) {
                int wave = (Integer) item.getOrDefault("wave", 0);
                int thread = (Integer) item.getOrDefault("thread", 0);

                if (item.containsKey("error") && !item.containsKey("result")) {
                    writer.writeNext(new String[] {
                            String.valueOf(wave),
                            String.valueOf(thread),
                            "",
                            "false",
                            (String) item.get("error"),
                            "", "", "", "", "", "", "", "", "", "", "", "", "", ""
                    });
                    continue;
                }

                Map<String, Object> result = (Map<String, Object>) item.get("result");
                if (result == null || !Boolean.TRUE.equals(result.get("success"))) {
                    Map<String, Object> s1 = result != null
                            ? (Map<String, Object>) result.getOrDefault("step1", Map.of())
                            : Map.of();
                    writer.writeNext(new String[] {
                            String.valueOf(wave),
                            String.valueOf(thread),
                            (String) s1.getOrDefault("uid", ""),
                            "false",
                            result != null ? (String) result.getOrDefault("error", "unknown") : "unknown",
                            "", "", "", "", "", "", "", "", "", "", "", "", "", ""
                    });
                    continue;
                }

                Map<String, Object> s1 = (Map<String, Object>) result.getOrDefault("step1", Map.of());
                Map<String, Object> s2 = (Map<String, Object>) result.getOrDefault("step2", Map.of());
                Map<String, Object> s3 = (Map<String, Object>) result.getOrDefault("step3", Map.of());
                Map<String, Object> s4 = (Map<String, Object>) result.getOrDefault("step4", Map.of());

                List<Map<String, Object>> iterations = (List<Map<String, Object>>) s4.getOrDefault("iterations",
                        List.of());

                double l1 = (Double) s1.getOrDefault("latency_ms", 0.0);
                double l2 = (Double) s2.getOrDefault("latency_ms", 0.0);
                double l3 = (Double) s3.getOrDefault("latency_ms", 0.0);

                double s4Total = 0;
                for (Map<String, Object> it : iterations) {
                    s4Total += (Double) it.getOrDefault("latency_ms", 0.0);
                }
                double s4Avg = iterations.isEmpty() ? 0 : s4Total / iterations.size();

                double total = l1 + l2 + l3 + s4Total;

                writer.writeNext(new String[] {
                        String.valueOf(wave),
                        String.valueOf(thread),
                        (String) s1.getOrDefault("uid", ""),
                        "true",
                        "",
                        formatTimestamp(s1.get("start_timestamp")),
                        formatTimestamp(s1.get("end_timestamp")),
                        String.format("%.2f", l1),
                        formatTimestamp(s2.get("start_timestamp")),
                        formatTimestamp(s2.get("end_timestamp")),
                        String.format("%.2f", l2),
                        formatTimestamp(s3.get("start_timestamp")),
                        formatTimestamp(s3.get("end_timestamp")),
                        String.format("%.2f", l3),
                        formatTimestamp(s4.get("start_timestamp")),
                        formatTimestamp(s4.get("end_timestamp")),
                        String.format("%.2f", s4Avg),
                        String.format("%.2f", s4Total),
                        String.format("%.2f", total)
                });
            }
        }

        return filepath;
    }
}
