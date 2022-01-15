package com.markosindustries.distroboy.core;

/**
 * A wrapper type to carry along an AutoCloseable resource with the result, so that cleanup can
 * happen once iteration is finished on the node.
 *
 * @param <T> The type of result
 */
public class ResultWithResource<T> {
  private final T result;
  private final AutoCloseable resource;

  /**
   * Create a {@link ResultWithResource}
   *
   * @param result The result
   * @param resource The resource
   */
  public ResultWithResource(T result, AutoCloseable resource) {
    this.result = result;
    this.resource = resource;
  }

  public T getResult() {
    return result;
  }

  public AutoCloseable getResource() {
    return resource;
  }
}
