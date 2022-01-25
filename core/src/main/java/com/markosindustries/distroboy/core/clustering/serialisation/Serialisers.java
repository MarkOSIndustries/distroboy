package com.markosindustries.distroboy.core.clustering.serialisation;

import com.google.protobuf.MessageLite;
import com.markosindustries.distroboy.core.protobuf.ParseFromByteString;

/** Provides singletons and factory methods for default serialisers */
public interface Serialisers {
  /** Serialiser for boolean values */
  BooleanValues booleanValues = new BooleanValues();
  /** Serialiser for integer values */
  IntegerValues integerValues = new IntegerValues();
  /** Serialiser for long values */
  LongValues longValues = new LongValues();
  /** Serialiser for float values */
  FloatValues floatValues = new FloatValues();
  /** Serialiser for double values */
  DoubleValues doubleValues = new DoubleValues();
  /** Serialiser for {@link String} values */
  StringValues stringValues = new StringValues();
  /** Serialiser for byte[] values */
  ByteArrayValues byteArrayValues = new ByteArrayValues();
  /** Serialiser for {@link com.google.protobuf.ByteString} values */
  ByteStringValues byteStringValues = new ByteStringValues();
  /** Serialiser for {@link Void} values */
  Serialiser<Void> voidValues = new VoidValues();

  /**
   * Serialiser factory for {@link java.util.List}s of values
   *
   * @param serialiser The serialiser for the type the lists contain
   * @param <T> The type the lists contain
   * @return A serialiser for Lists of values
   */
  static <T> ListValues<T> listEntries(Serialiser<T> serialiser) {
    return new ListValues<>(serialiser);
  }

  /**
   * Serialiser factory for {@link java.util.List}s of values
   *
   * @param keySerialiser The serialiser for the key type the maps contain
   * @param valueSerialiser The serialiser for the value type the maps contain
   * @param <K> The type of key the maps contain
   * @param <V> The type of value the maps contain
   * @return A serialiser for Maps of values
   */
  static <K, V> MapEntries<K, V> mapEntries(
      Serialiser<K> keySerialiser, Serialiser<V> valueSerialiser) {
    return new MapEntries<>(keySerialiser, valueSerialiser);
  }

  /**
   * Serialiser factory for protobuf values
   *
   * @param parseFromByteString The protobuf type's <code>
   *     parseFrom({@link com.google.protobuf.ByteString})</code> method (eg: <code>
   *     MyProtobuf::parseFrom</code>)
   * @param <T> The protobuf type to be serialised
   * @return A serialiser for the given protobuf type
   */
  static <T extends MessageLite> ProtobufValues<T> protobufValues(
      ParseFromByteString<T> parseFromByteString) {
    return new ProtobufValues<>(parseFromByteString);
  }
}
