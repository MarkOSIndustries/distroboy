package com.markosindustries.distroboy.example;

import org.aeonbits.owner.Config;

@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({"system:env", "classpath:config.properties"})
public interface ExampleConfig extends Config {
  @Config.Key("COORDINATOR_HOST")
  @Config.DefaultValue("localhost")
  String coordinatorHost();

  @Config.Key("COORDINATOR_PORT")
  @Config.DefaultValue("7070")
  int coordinatorPort();

  @Config.Key("MEMBER_PORT")
  @Config.DefaultValue("7071")
  int memberPort();

  @Config.Key("EXPECTED_MEMBERS")
  @Config.DefaultValue("2")
  int expectedMembers();

  @Config.Key("RUN_EXAMPLE_KAFKA_CONSUMER")
  @Config.DefaultValue("false")
  boolean runExampleKafkaConsumer();

  @Config.Key("RUN_EXAMPLE_S3_CLIENT")
  @Config.DefaultValue("false")
  boolean runExampleS3Client();

  @Config.Key("USE_LOCALSTACK_FOR_S3")
  @Config.DefaultValue("false")
  boolean useLocalStackForS3();

  @Config.Key("S3_ENDPOINT_OVERRIDE")
  @Config.DefaultValue("")
  String s3EndpointOverride();

  @Config.Key("AWS_REGION")
  @Config.DefaultValue("ap-southeast-2")
  String awsRegion();
}
