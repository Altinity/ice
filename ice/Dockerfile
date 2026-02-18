# use `docker build --build-arg BASE_IMAGE_TAG=debug-nonroot ...` to build a debug version of the image
ARG BASE_IMAGE_TAG="nonroot"
ARG VERSION="0.0.0-SNAPSHOT"

FROM --platform=$BUILDPLATFORM ghcr.io/graalvm/native-image-community:21.0.2 AS build

WORKDIR /app

# cache maven dependencies for faster re-builds
COPY .mvn/ .mvn/
COPY mvnw .
COPY pom.xml .
COPY ice/pom.xml ice/pom.xml
COPY ice-rest-catalog/pom.xml ice-rest-catalog/pom.xml
RUN ./mvnw -am -pl ice dependency:go-offline

ARG VERSION

COPY . .

RUN ./mvnw versions:set -DnewVersion=${VERSION}
RUN ./mvnw -Pno-check -pl ice clean package -Dmaven.test.skip=true

FROM gcr.io/distroless/java21-debian12:${BASE_IMAGE_TAG}

COPY --from=build /app/ice/target/ice-jar /usr/local/bin/ice

# using java -jar instead of executing jar directly as there is not shell in non-"debug" version of distroless
ENTRYPOINT ["java", "-jar", "/usr/local/bin/ice"]
