#!/usr/bin/env bash
#
# Build a static musl binary of rusts3 with cargo — no Docker.
#
# RocksDB is compiled from source (bundled C++), so a *full* musl cross
# toolchain is required: musl-gcc from apt's musl-tools has no g++ and cannot
# build RocksDB. If no such toolchain is found on PATH, this script fetches the
# self-contained x86_64-linux-musl-cross toolchain from musl.cc into a cache
# directory and uses it. bindgen (librocksdb-sys) additionally needs libclang,
# which we point at the host clang.
#
#   ./mush.sh
#
# Override the toolchain by exporting MUSL_CC / MUSL_CXX to your own musl
# gcc/g++ before running.
#
set -euo pipefail

TARGET="x86_64-unknown-linux-musl"
TOOL_PREFIX="x86_64-linux-musl"
CACHE_DIR="${MUSL_CROSS_CACHE:-$HOME/.cache/musl-cross}"
TOOLCHAIN_URL="https://musl.cc/${TOOL_PREFIX}-cross.tgz"

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")"

# 1. Rust target.
if ! rustup target list --installed 2>/dev/null | grep -qx "$TARGET"; then
    echo ">> Adding rustup target $TARGET"
    rustup target add "$TARGET"
fi

# 2. musl C/C++ toolchain (must include g++ for RocksDB).
resolve_toolchain() {
    # Caller-supplied compilers win.
    if [[ -n "${MUSL_CC:-}" && -n "${MUSL_CXX:-}" ]]; then
        return 0
    fi
    # A full cross toolchain already on PATH.
    if command -v "${TOOL_PREFIX}-gcc" >/dev/null 2>&1 \
        && command -v "${TOOL_PREFIX}-g++" >/dev/null 2>&1; then
        MUSL_CC="$(command -v "${TOOL_PREFIX}-gcc")"
        MUSL_CXX="$(command -v "${TOOL_PREFIX}-g++")"
        return 0
    fi
    # Fetch a self-contained toolchain into the cache.
    local root="$CACHE_DIR/${TOOL_PREFIX}-cross"
    if [[ ! -x "$root/bin/${TOOL_PREFIX}-g++" ]]; then
        echo ">> No musl C++ toolchain found; fetching $TOOLCHAIN_URL"
        mkdir -p "$CACHE_DIR"
        local tgz="$CACHE_DIR/${TOOL_PREFIX}-cross.tgz"
        curl -fL --retry 3 -o "$tgz" "$TOOLCHAIN_URL"
        tar -xzf "$tgz" -C "$CACHE_DIR"
        rm -f "$tgz"
    fi
    MUSL_CC="$root/bin/${TOOL_PREFIX}-gcc"
    MUSL_CXX="$root/bin/${TOOL_PREFIX}-g++"
    export PATH="$root/bin:$PATH"
}
resolve_toolchain

MUSL_AR="$(dirname "$MUSL_CC")/${TOOL_PREFIX}-ar"
[[ -x "$MUSL_AR" ]] || MUSL_AR="ar"

echo ">> Using CC=$MUSL_CC"
echo ">> Using CXX=$MUSL_CXX"

# 3. libclang for bindgen (librocksdb-sys). Prefer an already-set path,
#    otherwise probe common locations.
if [[ -z "${LIBCLANG_PATH:-}" ]]; then
    for d in /usr/lib/llvm-*/lib /usr/lib64 /usr/lib /usr/lib/x86_64-linux-gnu; do
        if compgen -G "$d/libclang.so*" >/dev/null 2>&1; then
            LIBCLANG_PATH="$d"
            break
        fi
    done
fi
[[ -n "${LIBCLANG_PATH:-}" ]] && echo ">> Using LIBCLANG_PATH=$LIBCLANG_PATH"

# 4. Per-target compiler/linker wiring for cargo, the cc crate, and bindgen.
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="$MUSL_CC"
export CC_x86_64_unknown_linux_musl="$MUSL_CC"
export CXX_x86_64_unknown_linux_musl="$MUSL_CXX"
export AR_x86_64_unknown_linux_musl="$MUSL_AR"
export LIBCLANG_PATH
# bindgen parses RocksDB headers with the host clang; give it the musl sysroot
# so the C++ standard headers resolve.
SYSROOT="$(dirname "$(dirname "$MUSL_CC")")/${TOOL_PREFIX}"
if [[ -d "$SYSROOT" ]]; then
    export BINDGEN_EXTRA_CLANG_ARGS="--sysroot=$SYSROOT ${BINDGEN_EXTRA_CLANG_ARGS:-}"
fi
# Statically link the C/C++ runtime into the binary.
export RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=+crt-static"

# 5. Build.
echo ">> Building rusts3 for $TARGET (release)"
cargo build --release --target "$TARGET" "$@"

BIN="target/$TARGET/release/rusts3"
echo ">> Built $BIN"
file "$BIN" 2>/dev/null || true
ldd "$BIN" 2>&1 | head -1 || true
