package com.markosindustries.distroboy.core.iterators.mock;

public class MockResource implements AutoCloseable {
  public boolean isClosed = false;

  @Override
  public void close() throws Exception {
    isClosed = true;
  }
}
