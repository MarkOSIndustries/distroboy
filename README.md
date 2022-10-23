# DistroBoy
[![][license-badge]][license]
[![][docs-badge]][docs]
[![][maven-badge]][maven]
[![][docker-coordinator-badge]][docker-coordinator]

A framework for distributing compute to a group of processes.

## Usage

DistroBoy libraries are published to [maven central][maven]. Add a dependency on the `core` library to start.
```xml
<dependency>
  <groupId>com.markosindustries.distroboy</groupId>
  <artifactId>core</artifactId>
  <version>0.8.0</version>
</dependency>
```

### Examples

- [distroboy-kafka-archiver](https://github.com/MarkOSIndustries/distroboy-kafka-archiver) - uses a variety of DistroBoy features 
- The [example](./example/src/main/java/com/markosindustries/distroboy/example/Main.java) in this repo attempts to exercise most of DistroBoy's capabilities, though is more of an integration test than a "real" job 


## Why yet another distributed compute project?
Yes, I know. Hadoop, Spark, Flink, and a bunch of other projects already fill this space.
Like most projects, this was borne of wanting something which didn't exist yet.
I wanted something which
- Doesn't carry with it the weight of Hadoop
  - setting up Hadoop is complicated and requires a bunch of stuff to already be there to work
- Doesn't carry with it the weight of Scala (just plain Java please)
  - I like using other JVM languages, but I don't want to *have* to use one.
- Can be deployed using Docker
  - I want to be able to run it the same way I run "normal" services, within reason
- Starts fast
- Avoids magic
  - I don't want to disguise side effects behind operations which look like familiar, simpler equivalents (like `groupBy`). I wanted something which made what was actually happening more explicit, at the cost of verbosity 

## How does it work?

#### Clustering
DistroBoy uses a shared known service called the "coordinator" to find cluster peers.
All cluster members start and join a named "lobby" on the coordinator process. Once the expected number of cluster members arrive, the coordinator elects a leader, and all nodes establish a mesh network of RPC connections to transfer data between themselves. After that the coordinator is no longer required.

#### Code sharing
One important aspect of distributed compute is getting the actual code that needs executing distributed to the worker nodes which are going to run it. DistroBoy takes the approach of assuming all nodes were deployed and run using the same code, rather than  trying to do anything fancy with code serialisation.

#### Scaling
DistroBoy doesn't attempt to allow for nodes to dynamically join/leave the cluster. It assumes a fixed cluster composition throughout the job. The intent is that if a node dies, the job is restarted by some scheduling/automation capability outside of the job.  

#### Runtime/Hosting
You can run DistroBoy jobs however you like... as a vanilla JAR, using Spring/Micronaut/Lagom etc. It's designed with running via Docker in mind.

## Why the name?
It reminded me of the cartoon AstroBoy, and seemed inoffensive enough



[license-badge]:https://img.shields.io/github/license/MarkOSIndustries/distroboy
[license]:LICENSE

[docs-badge]:https://javadoc.io/badge2/com.markosindustries.distroboy/core/javadoc.svg
[docs]:https://www.javadoc.io/doc/com.markosindustries.distroboy

[maven-badge]:https://img.shields.io/maven-central/v/com.markosindustries.distroboy/core
[maven]:https://search.maven.org/search?q=g:com.markosindustries.distroboy

[docker-coordinator-badge]:https://img.shields.io/docker/v/markosindustries/distroboy-coordinator?sort=semver&logo=docker&label=docker%20(coordinator)
[docker-coordinator]:https://hub.docker.com/r/markosindustries/distroboy-coordinator/tags
