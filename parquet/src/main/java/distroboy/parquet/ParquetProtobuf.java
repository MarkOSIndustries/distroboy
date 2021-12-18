package distroboy.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collector;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoParquetWriter;

public interface ParquetProtobuf {
  static <T extends Message> ParquetWriter<T> parquetProtobufWriter(
      OutputFile outputFile, Class<T> rowClass) throws IOException {
    return ProtoParquetWriter.<T>builder(outputFile)
        .withMessage(rowClass)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(OVERWRITE)
        .build();
  }

  static <T extends Message> Collector<T, List<T>, byte[]> toParquetProtobufBytes(
      Class<T> rowClass) {
    return new ParquetProtobufBytesCollector<T>(rowClass);
  }
}
