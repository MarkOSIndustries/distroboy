package distroboy.parquet;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.io.PositionOutputStream;

public class SimplePositionOutputStream extends PositionOutputStream {
  private final OutputStream outputStream;
  private long pos;

  public SimplePositionOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
    this.pos = 0;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
    this.pos++;
  }
}
