# ==========================================
# STAGE 1: Build (Java 21 + Maven)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /app

# Step 1: Cache dependencies
# Copying only the POM first allows Docker to cache the downloaded Maven dependencies
# This makes subsequent builds incredibly fast if only source code changes
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Step 2: Copy source and build
COPY src ./src
# We skip tests here because integration tests require Docker-in-Docker for Testcontainers.
# Tests are verified independently in the CI pipeline before building the image.
RUN mvn clean package -DskipTests

# ==========================================
# STAGE 2: Runtime (Java 21 JRE)
# ==========================================
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Security: Run as a non-root user
RUN addgroup -S forgeflow && adduser -S forgeflowuser -G forgeflow
USER forgeflowuser

# Extract the compiled JAR from the builder stage
# We use a wildcard so it succeeds regardless of the version number in pom.xml
COPY --from=builder --chown=forgeflowuser:forgeflow /app/target/*.jar app.jar

# Expose the standard API port
EXPOSE 8080

# Define the execution command
ENTRYPOINT ["java", "-jar", "app.jar"]
