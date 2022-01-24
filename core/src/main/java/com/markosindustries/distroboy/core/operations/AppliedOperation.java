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
 * @param <I> The type of data supplied by the upstream operand.
 * @param <O> The type of data produced by the applied operation.
 * @param <CO> The type of data collected by the applied operation.
 */
public class AppliedOperation<I, O, CO> implements Operand<O, CO> {
  private static final Logger log = LoggerFactory.getLogger(AppliedOperation.class);

  private final Operand<I, ?> operand;
  private final Operation<I, O, CO> operation;

  /**
   * Represents an {@link Operation} applied to an {@link Operand} - to be treated as a new {@link
   * Operand} of it's own.
   *
   * @param operand The operand which the operation will be applied to
   * @param operation The operation to apply to the operand
   */
  public AppliedOperation(Operand<I, ?> operand, Operation<I, O, CO> operation) {
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
