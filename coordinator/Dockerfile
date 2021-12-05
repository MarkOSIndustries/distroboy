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

COPY --from=build_env /home/app/coordinator/build/install/distroboy-coordinator/coordinator*.jar coordinator.jar
COPY --from=build_env /home/app/coordinator/build/install/distroboy-coordinator/dependencies dependencies

EXPOSE 7070

CMD exec java -jar coordinator.jar
