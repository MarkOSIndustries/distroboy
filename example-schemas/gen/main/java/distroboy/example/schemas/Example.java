// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: example.proto

package distroboy.example.schemas;

public final class Example {
  private Example() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_distroboy_example_StringWithNumber_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_distroboy_example_StringWithNumber_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\rexample.proto\022\021distroboy.example\"<\n\020St" +
      "ringWithNumber\022\023\n\013some_string\030\001 \001(\t\022\023\n\013s" +
      "ome_number\030\002 \001(\003B\035\n\031distroboy.example.sc" +
      "hemasP\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_distroboy_example_StringWithNumber_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_distroboy_example_StringWithNumber_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_distroboy_example_StringWithNumber_descriptor,
        new java.lang.String[] { "SomeString", "SomeNumber", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
