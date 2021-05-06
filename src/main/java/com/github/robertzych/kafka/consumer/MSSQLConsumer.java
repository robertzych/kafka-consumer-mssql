package com.github.robertzych.kafka.consumer;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
//https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html
public class MSSQLConsumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(MSSQLConsumer.class.getName());

        //https://kafka.apache.org/documentation/#consumerconfigs
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "group1";

        // create consumer configs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Book.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets

        // create consumer
        String topic = "book";
        final Consumer<String, Book> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        // trying seek and assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
//        consumer.assign(Arrays.asList(partitionToReadFrom));
//        long offsetToReadFrom = 0L;
//        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        // poll for new data
        try {
            while (true) {
                ConsumerRecords<String, Book> records = consumer.poll(Duration.ofMillis(1000));

                int recordCount = records.count();
                if (recordCount > 0) {
                    logger.info("Received " + recordCount + " records");

                    // write records to ms sql
                    Connection conn = null;
                    try {
                        DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
                        String dbURL = "jdbc:sqlserver://localhost:1433;database=demo;user=sa;password=Admin123";
                        conn = DriverManager.getConnection(dbURL);
                        logger.info("Connected");
                        Statement statement = conn.createStatement();
                        String sql = "BEGIN TRAN T1";
                        logger.info(sql);
                        statement.executeUpdate(sql);
                        for (ConsumerRecord<String, Book> record : records) {
                            Book book = record.value();
                            sql = "INSERT INTO books VALUES (" + book.id + ", '" + book.title + "')";
                            logger.info(sql);
                            statement.executeUpdate(sql);
                        }

                        sql = "COMMIT TRAN T1";
                        logger.info(sql);
                        statement.executeUpdate(sql);

                        logger.info("Committing the offsets...");
                        consumer.commitSync();
                        logger.info("Offsets have been committed!");
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.info("Going back to the beginning");
                        consumer.seekToBeginning(Arrays.asList(partitionToReadFrom));
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                    } finally {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                            }
                        }
                    }
                } else {
                    logger.info("No records");
                }
            }
        } finally {
            consumer.close();
        }
    }
}
