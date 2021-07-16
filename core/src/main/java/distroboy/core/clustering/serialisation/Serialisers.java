package distroboy.core.clustering.serialisation;

import com.google.protobuf.MessageLite;

public interface Serialisers {
  IntegerValues integerValues = new IntegerValues();
  LongValues longValues = new LongValues();
  StringValues stringValues = new StringValues();

  default <T extends MessageLite> ProtobufValues<T> protobufValues(
      ProtobufValues.ParseFrom<T> parseFrom) {
    return new ProtobufValues<>(parseFrom);
  }
}
