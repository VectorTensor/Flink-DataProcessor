package config;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class MongoUtils {

    public static String buildMongoUrl(String username, String password, String hostname) {
        String authPart = "";

        if (username != null && !username.isEmpty() && password != null) {
            // Encode password safely
            String encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8);
            authPart = String.format("%s:%s@", username, encodedPassword);
        }

        return String.format("mongodb://%s%s", authPart, hostname);
    }
}
