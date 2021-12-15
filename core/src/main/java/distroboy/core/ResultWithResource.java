package distroboy.core;

public class ResultWithResource<T> {
  private final T result;
  private final AutoCloseable resource;

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
