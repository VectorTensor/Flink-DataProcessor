package com.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import config.ConfigReader;

import java.io.ObjectInputFilter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;



public class DataStreamJob{

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
    public static void main(String[] args) throws Exception {
        // kafka-headless.kafka:9092
        String config_kafka = ConfigReader.get("CFG_KAFKA_DOMAIN");
        String mongo_user = ConfigReader.get("CFG_MONGO_USER");
        String mongo_password = ConfigReader.get("CFG_MONGO_PASSWORD");
        String mongo_host = ConfigReader.get("CFG_MONGO_HOST");

        String config_mongo = MongoUtils.buildMongoUrl(mongo_user, mongo_password, mongo_host);

        System.out.printf("KAFKA : %s , MONGO_URL : %s", config_kafka, config_mongo);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(config_kafka)
                        .setTopics("test-topic")
                        .setGroupId("flink-consumer")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        DataStream<String> stream =
                env.fromSource(
                        source,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                );

        // Write to MongoDB
        stream.addSink(new MongoSink(config_mongo));

        env.execute("Kafka to MongoDB Flink Job");
    }


    // Custom Mongo Sink
    public static class MongoSink extends RichSinkFunction<String> {

        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> collection;

        private String MongoUrl;

        public MongoSink(String url){

            MongoUrl = url;



        }
//        mongodb://root:mypassword@mongo-mongodb.default:27017

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {

            mongoClient = MongoClients.create(MongoUrl);

            collection =
                    mongoClient
                            .getDatabase("flinkdb")
                            .getCollection("events");
        }

        @Override
        public void invoke(String value, Context context) {

            Document doc = new Document("message", value)
                    .append("timestamp", System.currentTimeMillis());

            collection.insertOne(doc);
        }

        @Override
        public void close() {

            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}
