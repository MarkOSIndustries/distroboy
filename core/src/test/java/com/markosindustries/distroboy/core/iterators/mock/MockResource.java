package com.markosindustries.distroboy.core.iterators.mock;

public class MockResource implements AutoCloseable {
  private boolean isClosed = false;

  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() throws Exception {
    isClosed = true;
  }
}
