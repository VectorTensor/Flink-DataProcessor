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

public class KafkaToMongoJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
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
        stream.addSink(new MongoSink());

        env.execute("Kafka to MongoDB Flink Job");
    }


    // Custom Mongo Sink
    public static class MongoSink extends RichSinkFunction<String> {

        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> collection;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {

            mongoClient = MongoClients.create("mongodb://localhost:27017");

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
