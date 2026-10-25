# ==========================================
# STAGE 1: Build (Java 21 + Maven)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /app

# Step 1: Cache dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Step 2: Copy source and build
COPY src ./src
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
COPY --from=builder --chown=forgeflowuser:forgeflow /app/target/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
