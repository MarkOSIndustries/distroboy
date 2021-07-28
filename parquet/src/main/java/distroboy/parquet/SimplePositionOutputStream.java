package distroboy.parquet;

import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.parquet.io.PositionOutputStream;

public class SimplePositionOutputStream extends PositionOutputStream {
  private final FileOutputStream fileOutputStream;
  private long pos;

  public SimplePositionOutputStream(FileOutputStream fileOutputStream) {
    this.fileOutputStream = fileOutputStream;
    this.pos = 0;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    fileOutputStream.write(b);
    this.pos++;
  }
}
