package distroboy.example;

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
}
