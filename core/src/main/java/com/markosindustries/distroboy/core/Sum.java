package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.core.operations.LongReduceOp;

/** Reduce a distributed dataset of Longs by adding them all together */
public class Sum implements LongReduceOp<Long> {
  @Override
  public Long reduceInput(Long aggregate, Long input) {
    return aggregate + input;
  }

  @Override
  public Long reduceOutput(Long aggregate, Long result) {
    return aggregate + result;
  }
}
