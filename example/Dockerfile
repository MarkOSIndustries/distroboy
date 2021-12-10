FROM openjdk:15 as build_env

ARG VERSION

WORKDIR /home/app

COPY ./gradle ./gradle
ADD ./gradlew ./gradlew
RUN chmod +x ./gradlew
RUN ./gradlew --version

COPY . .
RUN ./gradlew -Pversion_string=$VERSION clean installDist

FROM openjdk:15 as runtime

COPY --from=build_env /home/app/example/build/install/distroboy-example/dependencies dependencies
COPY --from=build_env /home/app/example/build/install/distroboy-example/example*.jar example.jar

COPY --from=build_env /home/app/example/sample-data /sample-data
RUN mkdir /output-data

EXPOSE 7071

CMD exec java -jar example.jar
