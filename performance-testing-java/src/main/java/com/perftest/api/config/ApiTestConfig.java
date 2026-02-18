package com.perftest.api.config;

/**
 * Configuration for API testing.
 * Holds base URL, series/set identifiers, timeouts, and auth config path.
 */
public class ApiTestConfig {

    private String baseUrl = "https://cdn-test.amuselabs.com/pmm/";
    private String authConfigPath = "auth_config.json";
    private String series = "gandalf";
    private int timeoutSeconds = 30;
    private int maxConnections = 100;
    private boolean verbose = false;

    public ApiTestConfig() {
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ApiTestConfig config = new ApiTestConfig();

        public Builder baseUrl(String baseUrl) {
            config.baseUrl = baseUrl;
            return this;
        }

        public Builder authConfigPath(String path) {
            config.authConfigPath = path;
            return this;
        }

        public Builder series(String series) {
            config.series = series;
            return this;
        }

        public Builder timeoutSeconds(int timeout) {
            config.timeoutSeconds = timeout;
            return this;
        }

        public Builder maxConnections(int maxConnections) {
            config.maxConnections = maxConnections;
            return this;
        }

        public Builder verbose(boolean verbose) {
            config.verbose = verbose;
            return this;
        }

        public ApiTestConfig build() {
            return config;
        }
    }

    // Getters

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getAuthConfigPath() {
        return authConfigPath;
    }

    public String getSeries() {
        return series;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public boolean isVerbose() {
        return verbose;
    }
}
