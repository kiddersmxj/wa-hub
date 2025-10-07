#!/usr/bin/env bash
set -euo pipefail

# Debian/Ubuntu: sudo apt-get install -y build-essential cmake libcurl4-openssl-dev nlohmann-json3-dev
# Arch:          sudo pacman -S --needed base-devel cmake curl nlohmann-json

BUILD_DIR="${BUILD_DIR:-build}"
PREFIX="${PREFIX:-/usr/local}"
INSTALL_TOOLS="${INSTALL_TOOLS:-OFF}"   # set to ON to install tools/wa-fifo-append.sh

cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" -DINSTALL_TOOLS="$INSTALL_TOOLS"

cmake --build "$BUILD_DIR" -j"$(nproc)"
sudo cmake --install "$BUILD_DIR"

echo
echo "Binaries installed to: $PREFIX/bin"
echo
echo "Run from repo (configs stay here):"
echo "  export WA_HUB_CONFIG=\"$(pwd)/wa-hub.json\""
echo "  export WA_HUB_ALIASES=\"$(pwd)/aliases.json\"   # if your wa-hub reads this env"
echo "  ./build/bin/wa-hub    --config \"\$WA_HUB_CONFIG\""
echo "  ./build/bin/wa-sub    --help"
echo
echo "Or after install:"
echo "  WA_HUB_CONFIG=\"$(pwd)/wa-hub.json\" wa-hub --config \"\$WA_HUB_CONFIG\""
