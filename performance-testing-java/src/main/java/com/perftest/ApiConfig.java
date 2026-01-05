package com.perftest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Configuration for the test run - base URL, credentials, timeouts etc.
 */
public class ApiConfig {

    private static final String CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();

    private String baseUrl = "https://cdn-test.amuselabs.com/pmm/";
    private String setParam = "gandalf";
    private String uid = "vansh";
    private boolean useRandomUid = false;
    private List<String> uidPool = new ArrayList<>();
    private int timeout = 30;

    // Hardcoded values for V2
    public static final String PUZZLE_ID = "d4725144";
    public static final int STATE_LEN = 185;

    public ApiConfig() {
    }

    public static String generateRandomUid(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARS.charAt(RANDOM.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    public static String generateRandomUid() {
        return generateRandomUid(8);
    }

    public String getUid() {
        if (!uidPool.isEmpty()) {
            return uidPool.get(RANDOM.nextInt(uidPool.size()));
        }
        if (useRandomUid) {
            return generateRandomUid();
        }
        return uid;
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ApiConfig config = new ApiConfig();

        public Builder baseUrl(String baseUrl) {
            config.baseUrl = baseUrl;
            return this;
        }

        public Builder setParam(String setParam) {
            config.setParam = setParam;
            return this;
        }

        public Builder uid(String uid) {
            config.uid = uid;
            return this;
        }

        public Builder useRandomUid(boolean useRandomUid) {
            config.useRandomUid = useRandomUid;
            return this;
        }

        public Builder uidPool(List<String> uidPool) {
            config.uidPool = uidPool != null ? uidPool : new ArrayList<>();
            return this;
        }

        public Builder timeout(int timeout) {
            config.timeout = timeout;
            return this;
        }

        public ApiConfig build() {
            return config;
        }
    }

    // Getters
    public String getBaseUrl() {
        return baseUrl;
    }

    public String getSetParam() {
        return setParam;
    }

    public String getDefaultUid() {
        return uid;
    }

    public boolean isUseRandomUid() {
        return useRandomUid;
    }

    public List<String> getUidPool() {
        return uidPool;
    }

    public int getTimeout() {
        return timeout;
    }
}
