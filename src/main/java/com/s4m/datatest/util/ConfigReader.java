package com.s4m.datatest.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {

    private ConfigReader(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class.getName());


    /**
     * Config Reader Function
     * @param configPath
     * @return
     * @throws IOException
     */
    public static Properties readConfig(String configPath) throws IOException {

        Properties properties = new Properties();
        InputStream input = ConfigReader.class.getClassLoader().getResourceAsStream(configPath + "/config.properties");
        if (input == null) {
            LOGGER.info("Unable to find config.properties");
        }

        try {
            properties.load(input);
        } catch (IOException var4) {
            LOGGER.error("Unable to read config file: {}", var4.getMessage());
            throw new IOException();
        }

        return properties;
    }
}
