# HQ consumer image. Built by infra/docker-compose.yml (service `consumer`), or: docker build -t branch-sales-consumer .

FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
# Dependencies first, so they stay cached while only the code changes
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN ./mvnw -B -q dependency:go-offline
COPY contract contract
COPY src src
# Tests need Docker (Testcontainers); they run outside the image build
RUN ./mvnw -B -q package -DskipTests

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 app
USER app
WORKDIR /app
COPY --from=build /src/target/branch-sales-consumer-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
