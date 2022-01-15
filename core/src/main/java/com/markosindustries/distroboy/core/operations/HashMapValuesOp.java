package com.markosindustries.distroboy.core.operations;

/**
 * Transform the value for each item in the data set (leaving the keys alone)
 *
 * @param <K> The type of input Map keys
 * @param <V> The type of input Map values
 * @param <V2> The type of output Map values
 */
public interface HashMapValuesOp<K, V, V2> extends HashMapKeysAndValuesOp<K, V, K, V2> {
  default K mapKey(K key) {
    return key;
  }
}
