package distroboy.core;

import java.util.function.Consumer;
import java.util.function.Function;

public interface DistributedOpResult<T> {
  boolean isClusterLeader();

  T getResult();

  <U> DistributedOpResult<U> map(Function<T, U> mapper);

  default void onClusterLeader(Consumer<T> consume) {
    if (isClusterLeader()) {
      consume.accept(getResult());
    }
  }

  class LeaderResult<T> implements DistributedOpResult<T> {
    private final T result;

    public LeaderResult(T result) {
      this.result = result;
    }

    @Override
    public boolean isClusterLeader() {
      return true;
    }

    @Override
    public T getResult() {
      return result;
    }

    @Override
    public <U> DistributedOpResult<U> map(Function<T, U> mapper) {
      return new LeaderResult<>(mapper.apply(result));
    }
  }

  class WorkerResult<T> implements DistributedOpResult<T> {
    @Override
    public boolean isClusterLeader() {
      return false;
    }

    @Override
    public T getResult() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <U> DistributedOpResult<U> map(Function<T, U> mapper) {
      return new WorkerResult<>();
    }
  }
}
