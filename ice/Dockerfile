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

# Mitigate Java4Shell (CVE in HotSpot x86-64 AVX-512 Base64 intrinsic).
# Do NOT use -XX:UseAVX=2 here — it is x86-only and breaks arm64.
ENTRYPOINT ["java", "-XX:-UseBASE64Intrinsics", "-jar", "/usr/local/bin/ice"]
