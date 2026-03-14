package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfigReader {

    private static final String CONFIG_PATH = "/vault/secrets/config";
    private static Map<String, String> configs = new HashMap<>();

    static {
        loadConfigs();
    }

    private static void loadConfigs() {
        try (BufferedReader reader = new BufferedReader(new FileReader(CONFIG_PATH))) {

            String line;
            while ((line = reader.readLine()) != null) {

                line = line.trim();

                if (line.isEmpty() || line.startsWith("#"))
                    continue;

                String[] parts = line.split("=", 2);

                if (parts.length == 2) {
                    configs.put(parts[0].trim(), parts[1].trim());
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load config from " + CONFIG_PATH, e);
        }
    }

    public static String get(String key) {
        return configs.get(key);
    }

    public static String getOrDefault(String key, String defaultValue) {
        return configs.getOrDefault(key, defaultValue);
    }
}