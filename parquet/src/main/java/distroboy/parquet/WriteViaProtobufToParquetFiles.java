package distroboy.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import com.google.protobuf.Message;
import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.ReduceOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.slf4j.LoggerFactory;

public class WriteViaProtobufToParquetFiles<I extends Message> implements ReduceOp<I, List<Path>> {
  private final Path path;
  private final ParquetWriter<I> writer;

  // TODO: support deciding how many files per node (or records per file perhaps)
  public WriteViaProtobufToParquetFiles(Path path, Class<I> rowClass) {
    this.path = path;
    try {
      this.writer =
          ProtoParquetWriter.<I>builder(
                  new SimpleOutputFile(new File(path.toAbsolutePath().toString())))
              .withMessage(rowClass)
              .withCompressionCodec(CompressionCodecName.SNAPPY)
              .withWriteMode(OVERWRITE)
              .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IteratorWithResources<List<Path>> asIterator(List<Path> aggregate) {
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ReduceOp.super.asIterator(aggregate);
  }

  @Override
  public List<Path> initAggregate() {
    final var agg = new ArrayList<Path>();
    agg.add(path);
    return agg;
  }

  @Override
  public List<Path> reduceInput(List<Path> aggregate, I input) throws Exception {
    LoggerFactory.getLogger("CRAZY").warn("Writing {}", input.toString());
    writer.write(input);
    return aggregate;
  }

  @Override
  public List<Path> reduceOutput(List<Path> aggregate, List<Path> result) {
    aggregate.addAll(result);
    return aggregate;
  }
}
