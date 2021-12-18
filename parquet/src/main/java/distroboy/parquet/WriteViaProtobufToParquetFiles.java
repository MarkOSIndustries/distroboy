package distroboy.parquet;

import com.google.protobuf.Message;
import java.io.File;
import java.nio.file.Path;

public class WriteViaProtobufToParquetFiles<I extends Message>
    extends WriteViaProtobufToParquet<I, Path> {
  // TODO: support deciding how many files per node (or records per file perhaps)
  public WriteViaProtobufToParquetFiles(Path path, Class<I> rowClass) {
    super(new SimpleOutputFile(new File(path.toAbsolutePath().toString())), path, rowClass);
  }
}
