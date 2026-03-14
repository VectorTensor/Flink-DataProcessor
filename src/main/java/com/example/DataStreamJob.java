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

public class DataStreamJob{

    public static void main(String[] args) throws Exception {
        // kafka-headless.kafka:9092
        String config_kafka = ConfigReader.get("CFG_KAFKA_DOMAIN");
        String config_mongo = ConfigReader.get("CFG_MONGO_URL");

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
