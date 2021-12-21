package com.markosindustries.distroboy.parquet;

import java.io.IOException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class InputStreamAdapter implements InputFile {
  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return null;
  }
}
