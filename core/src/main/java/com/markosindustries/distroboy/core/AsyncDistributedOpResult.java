package com.markosindustries.distroboy.core;

/**
 * Represents a result of running a distributed operation via distroboy. Only the cluster leader
 * will receive the result, the other nodes only know that the operation will eventually be run.
 *
 * @param <T> The type of the operation result
 */
public interface AsyncDistributedOpResult<T> extends DistributedOpResult<T> {
  default DistributedOpResult<T> waitForAllMembers(Cluster cluster) {
    cluster.waitForAllMembers();
    return this;
  }

  /**
   * The cluster leader's result (which actually contains the result)
   *
   * @param <T> The type of the result of the distributed operation
   */
  class LeaderResult<T> extends DistributedOpResult.LeaderResult<T>
      implements AsyncDistributedOpResult<T> {
    LeaderResult(T result) {
      super(result);
    }
  }

  /**
   * The non-leader cluster members result (which is merely a promise to eventually participate in
   * the operation)
   *
   * @param <T> The type of the result of the distributed operation
   */
  class WorkerResult<T> extends DistributedOpResult.WorkerResult<T>
      implements AsyncDistributedOpResult<T> {}
}
