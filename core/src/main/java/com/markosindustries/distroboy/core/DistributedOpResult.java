package com.markosindustries.distroboy.core;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents a result of running a distributed operation via distroboy. Only the cluster leader
 * will receive the result, the other nodes only know that the operation is finished.
 *
 * @param <T> The type of the operation result
 */
public interface DistributedOpResult<T> {
  /**
   * Is the executing node the cluster leader?
   *
   * @return true on cluster leader, false on all other nodes
   */
  boolean isClusterLeader();

  /**
   * If the current node is the cluster leader, returns the result of the distributed operation.
   * Otherwise throws an {@code UnsupportedOperationException}
   *
   * @return The result of the distributed operation.
   */
  T getResult();

  /**
   * Analagous to {@code Optional.map} - if the executing node is the cluster leader, returns a new
   * result with the given mapping applied. Does nothing on non-leader nodes.
   *
   * @param mapper A transformation to apply to the result
   * @param <U> The resulting type of the transformation
   * @return A new DistributedOpResult which has been transformed.
   */
  <U> DistributedOpResult<U> map(Function<T, U> mapper);

  /**
   * Runs the given consumer with the operation result if and only if this node is the cluster
   * leader. Otherwise does nothing.
   *
   * @param consume A lambda which accept the result of the operation
   */
  default void onClusterLeader(Consumer<T> consume) {
    if (isClusterLeader()) {
      consume.accept(getResult());
    }
  }

  /**
   * The cluster leader's result (which actually contains the result)
   *
   * @param <T> The type of the result of the distributed operation
   */
  class LeaderResult<T> implements DistributedOpResult<T> {
    private final T result;

    LeaderResult(T result) {
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

  /**
   * The non-leader cluster members result (which is merely a signal that the operation is complete)
   *
   * @param <T> The type of the result of the distributed operation
   */
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
