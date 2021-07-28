package distroboy.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.ReduceOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class WriteViaAvroToParquetFiles<I> implements ReduceOp<I, List<Path>> {
  private final Path path;
  private final ParquetWriter<I> writer;

  // TODO: support deciding how many files per node (or records per file perhaps)
  public WriteViaAvroToParquetFiles(Path path, Class<I> clazz) {
    this.path = path;
    try {
      this.writer =
          AvroParquetWriter.<I>builder(
                  new SimpleOutputFile(new File(path.toAbsolutePath().toString())))
              .withSchema(ReflectData.AllowNull.get().getSchema(clazz)) // generate nullable fields
              .withDataModel(ReflectData.get())
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
    writer.write(input);
    return aggregate;
  }

  @Override
  public List<Path> reduceOutput(List<Path> aggregate, List<Path> result) {
    aggregate.addAll(result);
    return aggregate;
  }
}
