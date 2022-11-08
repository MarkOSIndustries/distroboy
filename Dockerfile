# Building
FROM openjdk:15 as build_env

ARG VERSION

WORKDIR /home/distroboy

COPY ./gradle ./gradle
ADD ./gradlew ./gradlew
RUN chmod +x ./gradlew
RUN ./gradlew --version

COPY . .
RUN ./gradlew -Pversion_string=$VERSION clean installDist --parallel

# Coordinator
FROM openjdk:15 as coordinator_runtime
COPY --from=build_env /home/distroboy/coordinator/build/install/distroboy-coordinator distroboy-coordinator
RUN mv distroboy-coordinator/coordinator*.jar distroboy-coordinator/coordinator.jar; true
EXPOSE 7070
CMD exec java -jar distroboy-coordinator/coordinator.jar

# Example
FROM openjdk:15 as example_runtime
COPY --from=build_env /home/distroboy/example/build/install/distroboy-example distroboy-example
RUN mv distroboy-example/example*.jar distroboy-example/example.jar; true
COPY --from=build_env /home/distroboy/example/sample-data /sample-data
RUN mkdir /output-data
EXPOSE 7071
CMD exec java -jar distroboy-example/example.jar
