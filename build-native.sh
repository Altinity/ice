#!/bin/bash
# Build native images for ice CLI
# Supports: amd64 (static with musl), arm64 (dynamic)

set -e

VERSION="${VERSION:-0.0.0-SNAPSHOT}"
ARCH="${ARCH:-$(uname -m)}"

# Convert architecture names
case "$ARCH" in
  x86_64|amd64)
    ARCH="amd64"
    PROFILE="native-amd64-static"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    PROFILE="native-arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    echo "Supported: x86_64/amd64, aarch64/arm64"
    exit 1
    ;;
esac

echo "============================================"
echo "Building native ice binary"
echo "Architecture: $ARCH"
echo "Profile: $PROFILE"
echo "Version: $VERSION"
echo "============================================"
echo ""

# Check for musl-gcc on amd64 builds
if [ "$ARCH" = "amd64" ]; then
  if ! command -v x86_64-linux-musl-gcc &> /dev/null && ! command -v musl-gcc &> /dev/null; then
    echo "  WARNING: musl compiler not found!"
    echo ""
    echo "For static builds on amd64, you need musl-tools installed."
    echo ""
    echo "Install it with:"
    echo "  Ubuntu/Debian: sudo apt-get install musl-tools"
    echo "  Fedora/RHEL:   sudo dnf install musl-gcc musl-libc-static"
    echo "  Alpine:        apk add musl-dev gcc"
    echo ""
    echo "Or build without static linking using the 'native' profile:"
    echo "  mvn -Pnative -pl ice clean package -Dmaven.test.skip=true"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      exit 1
    fi
  else
    echo "✓ musl compiler found"
  fi
fi
echo ""

# Set version
./mvnw -am -pl ice versions:set -DnewVersion=${VERSION}

# Build native image
echo "Building native image..."
./mvnw -Pno-check -P${PROFILE} -pl ice clean package -Dmaven.test.skip=true

echo ""
echo "============================================"
echo "✓ Native binary built successfully!"
echo "============================================"
echo ""
echo "Binary location: ice/target/ice"
echo ""
echo "Test the binary:"
echo "  ./ice/target/ice --version"
echo "  ./ice/target/ice check"
echo ""

# Show binary info
if [ -f "ice/target/ice" ]; then
  ls -lh ice/target/ice
  file ice/target/ice || true
  
  if [ "$ARCH" = "amd64" ]; then
    echo ""
    echo "Static binary (no dependencies):"
    ldd ice/target/ice 2>&1 || echo "  ✓ Statically linked (expected for amd64)"
  else
    echo ""
    echo "Dynamic binary dependencies:"
    ldd ice/target/ice || true
  fi
fi

