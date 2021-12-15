package distroboy.parquet;

import com.google.protobuf.Message;
import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.FlatMapOp;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;

public class ReadViaProtobufFromParquetFiles<O extends Message> implements FlatMapOp<Path, O> {
  @Override
  public IteratorWithResources<O> flatMap(Path input) {
    try {
      // ProtoRecordConverter re-uses a Builder internally... and you can't
      // tell it not to, as it's newed up internally. So we need to iterate
      // builders and call build on each one.
      ParquetReader<O.Builder> reader =
          ProtoParquetReader.<O.Builder>builder(
                  new SimpleInputFile(new File(input.toAbsolutePath().toString())))
              .build();

      return new IteratorWithResources<O>() {
        O.Builder next = reader.read();

        @Override
        public void close() throws Exception {
          reader.close();
        }

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public O next() {
          O result = (O) next.build();
          try {
            next = reader.read();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
