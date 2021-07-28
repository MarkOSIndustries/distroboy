package distroboy.core.operations;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.schemas.DataSourceRange;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppliedOperation<I, O, CI, CO> implements Operand<O, CO> {
  private static final Logger log = LoggerFactory.getLogger(AppliedOperation.class);

  private final Operand<I, CI> operand;
  private final Operation<I, O, CO> operation;

  public <T, U> AppliedOperation(Operand<I, CI> operand, Operation<I, O, CO> operation) {
    this.operand = operand;
    this.operation = operation;
  }

  @Override
  public List<Operand<?, ?>> dependencies() {
    return List.of(operand);
  }

  @Override
  public IteratorWithResources<O> enumerateRangeForNode(DataSourceRange dataSourceRange)
      throws Exception {
    return operation.apply(operand.enumerateRangeForNode(dataSourceRange));
  }

  @Override
  public CO collect(Iterator<O> results) {
    return operation.collect(results);
  }
}
