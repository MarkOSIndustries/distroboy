package com.markosindustries.distroboy.example;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.google.common.collect.ImmutableMap;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.kafka.EarliestKafkaOffsetSpec;
import com.markosindustries.distroboy.kafka.KafkaTopicPartitionsSource;
import com.markosindustries.distroboy.kafka.LatestKafkaOffsetSpec;
import com.markosindustries.distroboy.kafka.ReadKafkaTopicPartitionRange;
import java.util.List;
import java.util.Locale;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ExampleKafkaConsumer {
  Logger log = LoggerFactory.getLogger(ExampleKafkaConsumer.class);

  static void runKafkaExample(Cluster cluster) {
    final var kafkaConfig =
        ImmutableMap.<String, Object>builder()
            .put(BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
            .put(CLIENT_ID_CONFIG, "distroboy-example" + randomUUID())
            .put(ENABLE_AUTO_COMMIT_CONFIG, false)
            .put(AUTO_OFFSET_RESET_CONFIG, "earliest")
            .put(MAX_POLL_RECORDS_CONFIG, 1000)
            .put(SESSION_TIMEOUT_MS_CONFIG, 10 * 1000)
            .put(HEARTBEAT_INTERVAL_MS_CONFIG, 3 * 1000)
            .put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
            .put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
            .put(FETCH_MIN_BYTES_CONFIG, 1)
            .put(FETCH_MAX_WAIT_MS_CONFIG, 500)
            .put(
                ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT))
            .build();

    cluster
        .execute(
            DistributedOpSequence.readFrom(
                    new KafkaTopicPartitionsSource(kafkaConfig, List.of("distroboy.example.topic")))
                .flatMap(
                    // TODO: This is an example only. In a real job, you'd likely want to use
                    //  timestamp-based offsets in the past if possible, so that you can ensure
                    //  that all nodes see a consistent view of the data. Earliest/Latest are
                    //  only really safe when the topic isn't being cleaned or added to when the
                    //  distributed operation is running
                    new ReadKafkaTopicPartitionRange<>(
                        kafkaConfig, new EarliestKafkaOffsetSpec(), new LatestKafkaOffsetSpec()))
                .count())
        .onClusterLeader(
            events -> {
              log.info("There are {} events in the topic", events);
            });
  }
}
