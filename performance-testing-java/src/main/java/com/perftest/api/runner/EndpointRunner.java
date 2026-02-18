package com.perftest.api.runner;

import com.perftest.api.client.ApiClient;
import com.perftest.api.config.ApiTestConfig;
import com.perftest.api.endpoints.PlaysEndpoint;
import com.perftest.api.endpoints.PuzzlesEndpoint;
import com.perftest.api.model.ApiResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.io.IOException;

/**
 * CLI entry point for testing a single API endpoint.
 *
 * Usage:
 * java -cp target/flow-runner.jar com.perftest.api.runner.EndpointRunner
 * [options]
 *
 * Options:
 * --endpoint <name> Endpoint to test: puzzles | plays (required)
 * --method <GET|POST> HTTP method (default: GET)
 * --base-url <url> API base URL (default: https://cdn-test.amuselabs.com/pmm/)
 * --auth-config <path> Path to auth_config.json (default: auth_config.json)
 * --series <name> Series/set identifier (default: gandalf)
 * --limit <n> Pagination limit (default: 14)
 * --offset <n> Pagination offset (default: 0)
 * --puzzle-ids <ids> Comma-separated puzzle IDs (plays GET only)
 * --repeat <n> Repeat the request N times (default: 1)
 * -v, --verbose Verbose output
 */
public class EndpointRunner {

    private static final Gson PRETTY_GSON = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }

        // Parse CLI args
        String endpoint = null;
        String method = "GET";
        String baseUrl = "https://cdn-test.amuselabs.com/pmm/";
        String authConfig = "auth_config.json";
        String series = "gandalf";
        int limit = 14;
        int offset = 0;
        String puzzleIds = null;
        int repeat = 1;
        boolean verbose = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--endpoint" -> endpoint = args[++i];
                case "--method" -> method = args[++i].toUpperCase();
                case "--base-url" -> baseUrl = args[++i];
                case "--auth-config" -> authConfig = args[++i];
                case "--series" -> series = args[++i];
                case "--limit" -> limit = Integer.parseInt(args[++i]);
                case "--offset" -> offset = Integer.parseInt(args[++i]);
                case "--puzzle-ids" -> puzzleIds = args[++i];
                case "--repeat" -> repeat = Integer.parseInt(args[++i]);
                case "-v", "--verbose" -> verbose = true;
                case "-h", "--help" -> {
                    printUsage();
                    return;
                }
            }
        }

        if (endpoint == null) {
            System.err.println("Error: --endpoint is required");
            printUsage();
            return;
        }

        // Build config
        ApiTestConfig config = ApiTestConfig.builder()
                .baseUrl(baseUrl)
                .authConfigPath(authConfig)
                .series(series)
                .verbose(verbose)
                .build();

        try (ApiClient client = new ApiClient(config)) {
            System.out.printf("Endpoint: %s | Method: %s | Series: %s | Repeat: %d%n%n",
                    endpoint, method, series, repeat);

            for (int r = 0; r < repeat; r++) {
                if (repeat > 1) {
                    System.out.printf("--- Request %d/%d ---%n", r + 1, repeat);
                }

                ApiResponse response = executeEndpoint(client, endpoint, method,
                        series, limit, offset, puzzleIds, verbose);

                printResult(response, verbose);

                if (repeat > 1) {
                    System.out.println();
                }
            }

            // Summary for repeated requests
            if (repeat > 1) {
                System.out.printf("Completed %d requests to %s /%s%n", repeat, method, endpoint);
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }

    private static ApiResponse executeEndpoint(ApiClient client, String endpoint, String method,
            String series, int limit, int offset,
            String puzzleIds, boolean verbose) throws IOException {
        return switch (endpoint.toLowerCase()) {
            case "puzzles" -> {
                PuzzlesEndpoint ep = new PuzzlesEndpoint(series).limit(limit).offset(offset);
                if (verbose)
                    System.out.println(ep.describe());
                yield ep.get(client);
            }
            case "plays" -> {
                PlaysEndpoint ep = new PlaysEndpoint(series).limit(limit).offset(offset);
                if (puzzleIds != null)
                    ep.puzzleIds(puzzleIds);

                if ("POST".equals(method)) {
                    if (verbose)
                        System.out.println("POST /api/v1/plays");
                    // For standalone POST, build a minimal test payload
                    JsonObject payload = PlaysEndpoint.buildPayload(
                            "", "", "test-user", series, "");
                    yield ep.post(client, payload);
                } else {
                    if (verbose)
                        System.out.println(ep.describe());
                    yield ep.get(client);
                }
            }
            default -> throw new IOException("Unknown endpoint: " + endpoint +
                    ". Available: puzzles, plays");
        };
    }

    private static void printResult(ApiResponse response, boolean verbose) {
        System.out.printf("Status: %d | Latency: %dms%n", response.getStatusCode(), response.getLatencyMs());

        if (response.isSuccessful()) {
            System.out.println("✓ Success");
        } else {
            System.out.println("✗ Failed");
        }

        if (verbose && response.getBody() != null) {
            String prettyJson = PRETTY_GSON.toJson(response.getBody());
            // Truncate very long responses
            if (prettyJson.length() > 2000) {
                System.out.println("Response (truncated):");
                System.out.println(prettyJson.substring(0, 2000) + "\n...(truncated)");
            } else {
                System.out.println("Response:");
                System.out.println(prettyJson);
            }
        } else if (verbose && response.getRawBody() != null && !response.getRawBody().isEmpty()) {
            System.out.println("Response (raw): " + response.getRawBody().substring(0,
                    Math.min(500, response.getRawBody().length())));
        }
    }

    private static void printUsage() {
        System.out.println("API Endpoint Runner");
        System.out.println();
        System.out.println("Usage: EndpointRunner --endpoint <name> [options]");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  puzzles    GET /api/v1/puzzles");
        System.out.println("  plays      GET/POST /api/v1/plays");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --endpoint <name>     Endpoint to test (required)");
        System.out.println("  --method <GET|POST>   HTTP method (default: GET)");
        System.out.println("  --base-url <url>      API base URL");
        System.out.println("  --auth-config <path>  Path to auth_config.json");
        System.out.println("  --series <name>       Series identifier (default: gandalf)");
        System.out.println("  --limit <n>           Pagination limit (default: 14)");
        System.out.println("  --offset <n>          Pagination offset (default: 0)");
        System.out.println("  --puzzle-ids <ids>    Comma-separated puzzle IDs (plays only)");
        System.out.println("  --repeat <n>          Repeat the request N times (default: 1)");
        System.out.println("  -v, --verbose         Verbose output");
        System.out.println("  -h, --help            Show this help");
    }
}
