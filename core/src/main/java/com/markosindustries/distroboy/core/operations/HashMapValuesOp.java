package com.markosindustries.distroboy.core.operations;

/**
 * Transform the value for each item in the data set (leaving the keys alone)
 *
 * @param <Key> The type of input Map keys
 * @param <InputValue> The type of input Map values
 * @param <OutputValue> The type of output Map values
 */
public interface HashMapValuesOp<Key, InputValue, OutputValue>
    extends HashMapKeysAndValuesOp<Key, InputValue, Key, OutputValue> {
  default Key mapKey(Key key) {
    return key;
  }
}
