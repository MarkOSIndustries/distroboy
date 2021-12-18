package distroboy.parquet;

import com.google.protobuf.Message;
import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.ReduceOp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

public class WriteViaProtobufToParquet<I extends Message, O> implements ReduceOp<I, List<O>> {
  private final ParquetWriter<I> writer;
  private final O result;

  // TODO: support deciding how many files per node (or records per file perhaps)
  public WriteViaProtobufToParquet(OutputFile outputFile, O result, Class<I> rowClass) {
    this.result = result;
    try {
      this.writer = ParquetProtobuf.parquetProtobufWriter(outputFile, rowClass);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IteratorWithResources<List<O>> asIterator(List<O> aggregate) {
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ReduceOp.super.asIterator(aggregate);
  }

  @Override
  public List<O> initAggregate() {
    final var agg = new ArrayList<O>();
    agg.add(result);
    return agg;
  }

  @Override
  public List<O> reduceInput(List<O> aggregate, I input) throws Exception {
    writer.write(input);
    return aggregate;
  }

  @Override
  public List<O> reduceOutput(List<O> aggregate, List<O> result) {
    aggregate.addAll(result);
    return aggregate;
  }
}
