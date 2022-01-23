package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.MessageLite;
import com.markosindustries.distroboy.core.protobuf.ParseFromByteString;

/** Provides singletons and factory methods for default serialisers */
public interface Serialisers {
  BooleanValues booleanValues = new BooleanValues();
  IntegerValues integerValues = new IntegerValues();
  LongValues longValues = new LongValues();
  FloatValues floatValues = new FloatValues();
  DoubleValues doubleValues = new DoubleValues();
  StringValues stringValues = new StringValues();
  ByteArrayValues byteArrayValues = new ByteArrayValues();
  ByteStringValues byteStringValues = new ByteStringValues();
  Serialiser<Void> voidValues = new VoidValues();

  static <T> ListValues<T> listEntries(Serialiser<T> serialiser) {
    return new ListValues<>(serialiser);
  }

  static <K, V> MapEntries<K, V> mapEntries(
      Serialiser<K> keySerialiser, Serialiser<V> valueSerialiser) {
    return new MapEntries<>(keySerialiser, valueSerialiser);
  }

  static <T extends MessageLite> ProtobufValues<T> protobufValues(
      ParseFromByteString<T> parseFromByteString) {
    return new ProtobufValues<>(parseFromByteString);
  }
}
