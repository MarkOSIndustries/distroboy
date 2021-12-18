package distroboy.core.operations;

public interface HashMapValuesOp<K, V, V2> extends HashMapKeysAndValuesOp<K, V, K, V2> {
  default K mapKey(K key) {
    return key;
  }
}
