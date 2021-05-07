package com.github.robertzych.kafka.consumer;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
//https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html
public class MSSQLConsumer {

    final static String OFFSET_FILE_PREFIX = "./offsets/offset_";
    final static Logger logger = LoggerFactory.getLogger(MSSQLConsumer.class.getName());
    final static boolean SIMULATE_CRASH = false;

    public static void main(String[] args) {
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
        final KafkaConsumer<String, Book> consumer = new KafkaConsumer<>(props);
        ConsumerRebalanceListener listener = createListener(consumer);
        consumer.subscribe(Arrays.asList(topic), listener);

        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);

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
                        String sql;
                        for (ConsumerRecord<String, Book> record : records) {
                            logger.info("offset={}, timestamp={}", record.offset(), record.timestamp());
                            Book book = record.value();
                            if (SIMULATE_CRASH && book.id == 3) {
                                logger.error("SIMULATING A CRASH!");
                                return;
                            }
                            sql = "INSERT INTO books VALUES (" + book.id + ", '" + book.title + "')";
                            logger.info(sql);
                            statement.executeUpdate(sql);
                            Files.write(Paths.get(OFFSET_FILE_PREFIX + record.partition()),
                                    Long.valueOf(record.offset()+1).toString().getBytes());
                        }
                        logger.info("Committing the offsets...");
                        consumer.commitSync();
                        logger.info("Offsets have been committed!");
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.info("Resetting to the last committed offset");
                        seekToCommittedOffset(partitionToReadFrom, consumer);
                        sleep();
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
                    sleep();
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
        }
    }

    private static ConsumerRebalanceListener createListener(KafkaConsumer<String, Book> consumer){
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // nothing to do...
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    seekToCommittedOffset(partition, consumer);
                }
            }
        };
    }

    private static void seekToCommittedOffset(TopicPartition partition, KafkaConsumer<String, Book> consumer) {
        try {
            if(Files.exists(Paths.get(OFFSET_FILE_PREFIX + partition.partition()))){
                long offset = Long.parseLong(
                        Files.readAllLines(
                                Paths.get(OFFSET_FILE_PREFIX + partition.partition()),
                                Charset.defaultCharset()).get(0));
                logger.info("Consumer offset set to {}", offset);
                consumer.seek(partition, offset);
            }
        } catch(IOException e) {
            logger.error("Could not read offset from file");
        }
    }
}
