package distroboy.core.operations;

@FunctionalInterface
public interface HashMapKeysOp<K, V, K2> extends HashMapOp<K, V, K2, V> {
  default V mapValue(V value) {
    return value;
  }
}
