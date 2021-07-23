package distroboy.core.operations;

public interface HashMapValuesOp<K, V, V2> extends HashMapOp<K, V, K, V2> {
  default K mapKey(K key) {
    return key;
  }
}
