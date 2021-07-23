package distroboy.core.operations;

import distroboy.core.Count;
import distroboy.core.clustering.serialisation.ProtobufValues;
import distroboy.core.clustering.serialisation.Serialiser;
import distroboy.core.clustering.serialisation.Serialisers;
import distroboy.schemas.DataReference;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DistributedOpSequence<Input, Outcome, CollectedOutcome> {
  private final DataSource<Input> dataSource;
  private final Operand<Outcome, CollectedOutcome> operand;
  private final Serialiser<Outcome> serialiser;

  public DistributedOpSequence(
      DataSource<Input> dataSource,
      Operand<Outcome, CollectedOutcome> operand,
      Serialiser<Outcome> serialiser) {
    this.dataSource = dataSource;
    this.operand = operand;
    this.serialiser = serialiser;
  }

  public DataSource<Input> getDataSource() {
    return dataSource;
  }

  public Operand<Outcome, CollectedOutcome> getOperand() {
    return operand;
  }

  public Serialiser<Outcome> getSerialiser() {
    return serialiser;
  }

  public static <I, DS extends Operand<I, List<I>> & DataSource<I>> Builder<I, I, List<I>> readFrom(
      DS dataSource) {
    return new Builder<>(dataSource, dataSource);
  }

  public static class Builder<I, O, CO> {
    protected final DataSource<I> dataSource;
    protected final Operand<O, CO> operand;

    Builder(DataSource<I> dataSource, Operand<O, CO> operand) {
      this.dataSource = dataSource;
      this.operand = operand;
    }

    public <O2> Builder<I, O2, List<O2>> map(MapOp<O, O2> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }

    public <O2, O2I extends Iterator<O2>> IteratorBuilder<I, O2, O2I, List<O2I>> mapToIterators(
        MapOp<O, O2I> mapOp) {
      return new IteratorBuilder<>(dataSource, operand.then(mapOp));
    }

    public <O2> Builder<I, O2, List<O2>> flatMap(FlatMapOp<O, O2> flatMapOp) {
      return new Builder<>(dataSource, operand.then(flatMapOp));
    }

    public <O2> Builder<I, O2, O2> reduce(ReduceOp<O, O2> reduceOp) {
      return new Builder<>(dataSource, operand.then(reduceOp));
    }

    public Builder<I, O, List<O>> filter(FilterOp<O> filterOp) {
      return new Builder<>(dataSource, operand.then(filterOp));
    }

    public DistributedOpSequence<I, O, CO> collect(Serialiser<O> serialiser) {
      return new DistributedOpSequence<>(dataSource, operand, serialiser);
    }

    public DistributedOpSequence<I, Long, Long> count() {
      return new DistributedOpSequence<>(
          dataSource, operand.then(new Count<>()), Serialisers.longValues);
    }

    public <X> DistributedOpSequence<I, DataReference, List<DataReference>> persistToHeap(
        Serialiser<O> serialiser) {
      return new DistributedOpSequence<>(
          dataSource,
          operand.then(new PersistToHeap<>(serialiser)),
          new ProtobufValues<>(DataReference::parseFrom));
    }
  }

  public static class IteratorBuilder<I, O, OI extends Iterator<O>, C> extends Builder<I, OI, C> {
    IteratorBuilder(DataSource<I> dataSource, Operand<OI, C> operand) {
      super(dataSource, operand);
    }

    public <K> Builder<I, Map.Entry<K, List<O>>, Map<K, List<O>>> groupBy(
        GroupByOp<O, OI, K> groupByOp) {
      return new Builder<>(dataSource, operand.then(groupByOp));
    }

    public IteratorBuilder<I, O, OI, List<OI>> materialise() {
      return new IteratorBuilder<>(dataSource, operand.then(new MaterialiseOp<>()));
    }
  }
}
