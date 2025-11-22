#!/usr/bin/env bash
# Build ICE native images using Docker
# Usage: ./docker-build-native.sh [amd64|arm64|both]

set -euo pipefail

ARCH="${1:-$(uname -m)}"

# Normalize architecture names
case "$ARCH" in
  x86_64|amd64)
    BUILD_ARCH="amd64"
    PLATFORM="linux/amd64"
    ;;
  aarch64|arm64)
    BUILD_ARCH="arm64"
    PLATFORM="linux/arm64"
    ;;
  both)
    echo "Building for both architectures..."
    $0 amd64
    $0 arm64
    exit 0
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    echo "Usage: $0 [amd64|arm64|both]"
    exit 1
    ;;
esac

echo "========================================"
echo "Building ICE native image for $BUILD_ARCH"
echo "Platform: $PLATFORM"
echo "========================================"

# Build the Docker image
docker build \
  --platform="$PLATFORM" \
  --build-arg ARCH="$BUILD_ARCH" \
  -f ice/Dockerfile.native-builder \
  -t ice-native-builder:"$BUILD_ARCH" \
  .

# Extract the binary
CONTAINER_ID=$(docker create ice-native-builder:"$BUILD_ARCH")
docker cp "$CONTAINER_ID":/ice "./ice-$BUILD_ARCH"
docker rm "$CONTAINER_ID"

echo ""
echo "✓ Native binary created: ./ice-$BUILD_ARCH"
echo ""
echo "Test it with:"
echo "  ./ice-$BUILD_ARCH --help"

