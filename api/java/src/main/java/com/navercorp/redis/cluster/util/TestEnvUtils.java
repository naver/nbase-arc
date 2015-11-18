/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.redis.cluster.util;

import java.util.Properties;

/**
 * @author Costin Leau
 * @author jaehong.kim
 */
public class TestEnvUtils {
    private final static Properties DEFAULTS = new Properties();
    private static final Properties SETTINGS;
    private static final String FILE_NAME = "/test.properties";

    static {
        DEFAULTS.put("host", "localhost");
        DEFAULTS.put("port", "6379");
        DEFAULTS.put("zkAddress", "localhost:2181");
        DEFAULTS.put("clusterName", "test");

        SETTINGS = new Properties(DEFAULTS);

        try {
            SETTINGS.load(TestEnvUtils.class.getResourceAsStream(FILE_NAME));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Cannot read " + FILE_NAME, e);
        }
    }

    public static String getHost() {
        return SETTINGS.getProperty("host");
    }

    public static int getPort() {
        return Integer.valueOf(SETTINGS.getProperty("port"));
    }

    public static String getZkAddress() {
        return SETTINGS.getProperty("zkAddress");
    }

    public static String getClusterName() {
        return SETTINGS.getProperty("clusterName");
    }
}