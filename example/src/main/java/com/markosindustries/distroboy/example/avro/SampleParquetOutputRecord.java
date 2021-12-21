package com.markosindustries.distroboy.example.avro;

public class SampleParquetOutputRecord {
  public InnerThing innerThing;
  public int someNumber;

  // Needed for deserialisation
  private SampleParquetOutputRecord() {}

  public SampleParquetOutputRecord(InnerThing innerThing, int someNumber) {
    this.innerThing = innerThing;
    this.someNumber = someNumber;
  }

  public static class InnerThing {
    public String thingId;

    // Needed for deserialisation
    private InnerThing() {}

    public InnerThing(String thingId) {
      this.thingId = thingId;
    }
  }
}
