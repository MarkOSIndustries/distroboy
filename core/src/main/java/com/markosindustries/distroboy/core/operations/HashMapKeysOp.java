package com.markosindustries.distroboy.core.operations;

@FunctionalInterface
public interface HashMapKeysOp<K, V, K2> extends HashMapKeysAndValuesOp<K, V, K2, V> {
  default V mapValue(V value) {
    return value;
  }
}
