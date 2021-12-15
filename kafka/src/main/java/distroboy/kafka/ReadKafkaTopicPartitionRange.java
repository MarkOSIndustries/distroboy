package distroboy.kafka;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.FlatMapOp;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class ReadKafkaTopicPartitionRange<K, V>
    implements FlatMapOp<List<TopicPartition>, ConsumerRecord<K, V>> {
  private final Consumer<K, V> kafkaConsumer;
  private final KafkaOffsetSpec startOffsetInclusiveSpec;
  private final KafkaOffsetSpec endOffsetExclusiveSpec;

  public ReadKafkaTopicPartitionRange(
      Consumer<K, V> kafkaConsumer,
      KafkaOffsetSpec startOffsetInclusiveSpec,
      KafkaOffsetSpec endOffsetExclusiveSpec) {
    this.kafkaConsumer = kafkaConsumer;
    this.startOffsetInclusiveSpec = startOffsetInclusiveSpec;
    this.endOffsetExclusiveSpec = endOffsetExclusiveSpec;
  }

  @Override
  public IteratorWithResources<ConsumerRecord<K, V>> flatMap(List<TopicPartition> input) {
    return IteratorWithResources.from(
        new KafkaTopicPartitionsIterator<K, V>(
            kafkaConsumer, input, startOffsetInclusiveSpec, endOffsetExclusiveSpec));
  }
}
