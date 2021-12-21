package com.markosindustries.distroboy.parquet;

import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class OutputStreamAdapter implements OutputFile {

  private final OutputStreamSupplier outputStreamSupplier;

  public OutputStreamAdapter(OutputStreamSupplier outputStreamSupplier) {
    this.outputStreamSupplier = outputStreamSupplier;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new SimplePositionOutputStream(outputStreamSupplier.get());
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new SimplePositionOutputStream(outputStreamSupplier.get());
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
    return null;
  }
}
