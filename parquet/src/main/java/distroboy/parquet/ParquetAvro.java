package distroboy.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collector;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

public interface ParquetAvro {
  static <T> ParquetWriter<T> parquetAvroWriter(OutputFile outputFile, Class<T> rowClass)
      throws IOException {
    return AvroParquetWriter.<T>builder(outputFile)
        .withSchema(ReflectData.AllowNull.get().getSchema(rowClass)) // generate nullable fields
        .withDataModel(ReflectData.get())
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(OVERWRITE)
        .build();
  }

  static <T extends Message> Collector<T, List<T>, byte[]> toParquetAvroBytes(Class<T> rowClass) {
    return new ParquetAvroBytesCollector<T>(rowClass);
  }
}
