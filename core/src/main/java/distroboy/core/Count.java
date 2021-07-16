package distroboy.core;

import distroboy.core.operations.LongReduceOp;

public class Count<I> implements LongReduceOp<I> {
  @Override
  public Long reduceInput(Long aggregate, I input) {
    return aggregate + 1;
  }

  @Override
  public Long reduceOutput(Long aggregate, Long result) {
    return aggregate + result;
  }
}
