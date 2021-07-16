package distroboy.core;

import distroboy.core.operations.LongReduceOp;

public class Total implements LongReduceOp<Long> {
  @Override
  public Long reduceInput(Long aggregate, Long input) {
    return aggregate + input;
  }

  @Override
  public Long reduceOutput(Long aggregate, Long result) {
    return aggregate + result;
  }
}
