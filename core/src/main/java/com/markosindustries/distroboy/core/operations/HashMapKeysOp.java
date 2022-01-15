package com.markosindustries.distroboy.core.operations;

/**
 * Transform the keys for each item in the data set (leaving the values alone)
 *
 * @param <K> The type of input Map keys
 * @param <V> The type of input Map values
 * @param <K2> The type of output Map keys
 */
@FunctionalInterface
public interface HashMapKeysOp<K, V, K2> extends HashMapKeysAndValuesOp<K, V, K2, V> {
  default V mapValue(V value) {
    return value;
  }
}
