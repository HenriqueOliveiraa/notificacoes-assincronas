FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY contracts/pom.xml contracts/pom.xml
COPY notification-api/pom.xml notification-api/pom.xml
COPY alert-processor/pom.xml alert-processor/pom.xml

COPY contracts/src contracts/src
COPY notification-api/src notification-api/src

RUN mvn -B -pl notification-api -am -DskipTests clean package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/notification-api/target/notification-api.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
