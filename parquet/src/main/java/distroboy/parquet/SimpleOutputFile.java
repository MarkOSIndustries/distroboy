package distroboy.parquet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class SimpleOutputFile implements OutputFile {
  private final File file;

  public SimpleOutputFile(File file) {
    this.file = file;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new SimplePositionOutputStream(new FileOutputStream(file, false));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new SimplePositionOutputStream(new FileOutputStream(file, false));
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  @Override
  public String getPath() {
    return file.getAbsolutePath();
  }
}
