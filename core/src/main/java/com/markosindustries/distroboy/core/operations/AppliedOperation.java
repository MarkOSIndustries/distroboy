package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an {@link Operation} applied to an {@link Operand} - to be treated as a new {@link
 * Operand} of it's own.
 *
 * @param <Input> The type of data supplied by the upstream operand.
 * @param <Output> The type of data produced by the applied operation.
 * @param <CollectedOutput> The type of data collected by the applied operation.
 */
public class AppliedOperation<Input, Output, CollectedOutput>
    implements Operand<Output, CollectedOutput> {
  private static final Logger log = LoggerFactory.getLogger(AppliedOperation.class);

  private final Operand<Input, ?> operand;
  private final Operation<Input, Output, CollectedOutput> operation;

  /**
   * Represents an {@link Operation} applied to an {@link Operand} - to be treated as a new {@link
   * Operand} of it's own.
   *
   * @param operand The operand which the operation will be applied to
   * @param operation The operation to apply to the operand
   */
  public AppliedOperation(
      Operand<Input, ?> operand, Operation<Input, Output, CollectedOutput> operation) {
    this.operand = operand;
    this.operation = operation;
  }

  @Override
  public List<Operand<?, ?>> dependencies() {
    return List.of(operand);
  }

  @Override
  public IteratorWithResources<Output> enumerateRangeForNode(DataSourceRange dataSourceRange)
      throws Exception {
    return operation.apply(operand.enumerateRangeForNode(dataSourceRange));
  }

  @Override
  public CollectedOutput collect(Iterator<Output> results) {
    return operation.collect(results);
  }
}
