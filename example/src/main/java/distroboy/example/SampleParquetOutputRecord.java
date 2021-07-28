package distroboy.example;

public class SampleParquetOutputRecord {
  InnerThing innerThing;
  int someNumber;

  public SampleParquetOutputRecord(InnerThing innerThing, int someNumber) {
    this.innerThing = innerThing;
    this.someNumber = someNumber;
  }

  public static class InnerThing {
    String thingId;

    public InnerThing(String thingId) {
      this.thingId = thingId;
    }
  }
}
