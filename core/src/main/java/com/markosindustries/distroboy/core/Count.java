package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.core.operations.LongReduceOp;

/** Reduce a distributed dataset by counting the number of items in it. */
public class Count<Input> implements LongReduceOp<Input> {
  @Override
  public Long reduceInput(Long aggregate, Input input) {
    return aggregate + 1;
  }

  @Override
  public Long reduceOutput(Long aggregate, Long result) {
    return aggregate + result;
  }
}
