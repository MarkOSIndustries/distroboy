package distroboy.parquet;

import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ParquetProtobufBytesCollector<T extends Message>
    implements Collector<T, List<T>, byte[]> {
  private final Class<T> rowClass;

  public ParquetProtobufBytesCollector(Class<T> rowClass) {
    this.rowClass = rowClass;
  }

  @Override
  public Supplier<List<T>> supplier() {
    return ArrayList::new;
  }

  @Override
  public BiConsumer<List<T>, T> accumulator() {
    return List::add;
  }

  @Override
  public BinaryOperator<List<T>> combiner() {
    return (l1, l2) -> {
      l1.addAll(l2);
      return l1;
    };
  }

  @Override
  public Function<List<T>, byte[]> finisher() {
    return (rows) -> {
      final var stream = new ByteArrayOutputStream();
      final var outputFile = new OutputStreamAdapter(() -> stream);
      try (final var writer = ParquetProtobuf.parquetProtobufWriter(outputFile, rowClass)) {
        for (T row : rows) {
          writer.write(row);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return stream.toByteArray();
    };
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Collections.emptySet();
  }
}
