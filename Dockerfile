ARG MODULE

FROM eclipse-temurin:21-jdk AS build
ARG MODULE
WORKDIR /workspace
COPY . .
RUN chmod +x mvnw && ./mvnw -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/${MODULE}/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
