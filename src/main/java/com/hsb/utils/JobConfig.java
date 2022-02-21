package com.hsb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class JobConfig {

    public static Properties config;
    static {
        config = new Properties();
        config = reader(config, "config.properties");
    }

    public static Properties reader(Properties props, String filePath) {

        try (InputStream resourceAsStream = JobConfig.class.getClassLoader().getResourceAsStream(filePath)) {
            props.load(resourceAsStream);
        } catch (IOException e) {
            System.err.println("Unable to load properties file : " + filePath);
        }
        return props;
    }

    public static int getIntVal(String key) {
        return Integer.parseInt(config.getProperty(key));
    }

    public static String getProperty(String key) {
        return config.getProperty(key);
    }
}
