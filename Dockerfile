FROM eclipse-temurin:21-jre@sha256:0b99b3376fbb50dbf9c0af8535fdd5b8eb0d3fdfc8b1e4967f383b6919f6901a
ARG MODULE=notification-api
WORKDIR /app
COPY --chown=10001:10001 ${MODULE}/target/*.jar /app/app.jar
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
EXPOSE 8080 8081 9090
ENTRYPOINT ["java","-jar","/app/app.jar"]
