#!/usr/bin/env bash
#
# Build a static x86_64 musl binary of rusts3 with cargo — no Docker.
#
# The output is ALWAYS x86_64-unknown-linux-musl regardless of the host
# architecture. On an aarch64 machine that makes this a true cross-compile, so
# the script verifies the ELF header of the finished binary rather than trusting
# the toolchain to have done the right thing.
#
# RocksDB is compiled from source (bundled C++), so a *full* musl cross
# toolchain is required: musl-gcc from apt's musl-tools has no g++ and cannot
# build RocksDB. There are two ways to get one:
#
#   gcc  musl.cc's self-contained x86_64-linux-musl-cross toolchain. Preferred,
#        but musl.cc only publishes x86_64-*hosted* builds, so this path exists
#        only when building on x86_64.
#   zig  `zig cc -target x86_64-linux-musl`, which cross-compiles C and C++ from
#        any host and ships its own musl libc. This is what an aarch64 host uses.
#
# bindgen (librocksdb-sys) additionally needs libclang, which we point at the
# host clang and aim at the musl target explicitly — without that it would parse
# RocksDB's headers for the host arch and emit bindings for the wrong ABI.
#
#   ./musl.sh
#
# Environment overrides:
#   MUSL_CC / MUSL_CXX  your own musl gcc/g++ (both must be set to take effect)
#   MUSL_TOOLCHAIN      auto (default) | gcc | zig — force a strategy
#   MUSL_CROSS_CACHE    where fetched toolchains land (~/.cache/musl-cross)
#   ZIG_VERSION         zig release used by the zig path (see default below)
#
set -euo pipefail

TARGET="x86_64-unknown-linux-musl"
TOOL_PREFIX="x86_64-linux-musl"
ZIG_TARGET="x86_64-linux-musl"
HOST_ARCH="$(uname -m)"
CACHE_DIR="${MUSL_CROSS_CACHE:-$HOME/.cache/musl-cross}"
GCC_TOOLCHAIN_URL="https://musl.cc/${TOOL_PREFIX}-cross.tgz"
ZIG_VERSION="${ZIG_VERSION:-0.16.0}"
MUSL_TOOLCHAIN="${MUSL_TOOLCHAIN:-auto}"

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")"

echo ">> Host $HOST_ARCH -> target $TARGET"

# A cached toolchain copied over from another machine will pass -x but fail to
# exec, so probe by actually running it rather than by testing the file bit.
runnable() { [[ -x "$1" ]] && "$1" --version >/dev/null 2>&1; }
# zig spells it `zig version`, and rejects --version outright.
zig_runnable() { command -v "$1" >/dev/null 2>&1 && "$1" version >/dev/null 2>&1; }

# 1. Rust target.
if ! rustup target list --installed 2>/dev/null | grep -qx "$TARGET"; then
    echo ">> Adding rustup target $TARGET"
    rustup target add "$TARGET"
fi

# 2. musl C/C++ toolchain (must include a C++ compiler for RocksDB).
#    Sets TOOLCHAIN_KIND to gcc/zig/custom plus MUSL_CC/MUSL_CXX/MUSL_AR.
TOOLCHAIN_KIND=""
ZIG_BIN=""

resolve_gcc_toolchain() {
    # A full cross toolchain already on PATH.
    if runnable "${TOOL_PREFIX}-gcc" && runnable "${TOOL_PREFIX}-g++"; then
        MUSL_CC="$(command -v "${TOOL_PREFIX}-gcc")"
        MUSL_CXX="$(command -v "${TOOL_PREFIX}-g++")"
        return 0
    fi
    local root="$CACHE_DIR/${TOOL_PREFIX}-cross"
    # Already fetched, and its binaries run on this host.
    if runnable "$root/bin/${TOOL_PREFIX}-g++"; then
        MUSL_CC="$root/bin/${TOOL_PREFIX}-gcc"
        MUSL_CXX="$root/bin/${TOOL_PREFIX}-g++"
        export PATH="$root/bin:$PATH"
        return 0
    fi
    # musl.cc publishes x86_64-hosted tarballs only — on any other host the
    # download would produce binaries this machine cannot execute.
    if [[ "$HOST_ARCH" != "x86_64" ]]; then
        return 1
    fi
    echo ">> No musl C++ toolchain found; fetching $GCC_TOOLCHAIN_URL"
    mkdir -p "$CACHE_DIR"
    local tgz="$CACHE_DIR/${TOOL_PREFIX}-cross.tgz"
    rm -rf "$root"
    # set -e does not apply inside a function invoked in an if/|| context, so
    # every step here has to check itself or a failed fetch cascades into a
    # confusing tar error.
    if ! curl -fL --retry 3 -o "$tgz" "$GCC_TOOLCHAIN_URL"; then
        echo "error: failed to download $GCC_TOOLCHAIN_URL" >&2
        rm -f "$tgz"
        return 1
    fi
    if ! tar -xzf "$tgz" -C "$CACHE_DIR"; then
        echo "error: failed to extract $tgz" >&2
        rm -f "$tgz"
        return 1
    fi
    rm -f "$tgz"
    runnable "$root/bin/${TOOL_PREFIX}-g++" || return 1
    MUSL_CC="$root/bin/${TOOL_PREFIX}-gcc"
    MUSL_CXX="$root/bin/${TOOL_PREFIX}-g++"
    export PATH="$root/bin:$PATH"
}

resolve_zig() {
    # zig on PATH wins; it is the same compiler either way.
    if zig_runnable zig; then
        ZIG_BIN="$(command -v zig)"
        return 0
    fi
    local zarch
    case "$HOST_ARCH" in
        x86_64|amd64)   zarch="x86_64" ;;
        aarch64|arm64)  zarch="aarch64" ;;
        *)
            echo "error: no prebuilt zig for host arch $HOST_ARCH; install zig" \
                 "manually or set MUSL_CC/MUSL_CXX" >&2
            return 1 ;;
    esac
    local root="$CACHE_DIR/zig-${zarch}-linux-${ZIG_VERSION}"
    if ! zig_runnable "$root/zig"; then
        local url="https://ziglang.org/download/${ZIG_VERSION}/zig-${zarch}-linux-${ZIG_VERSION}.tar.xz"
        echo ">> No musl C++ toolchain found; fetching $url"
        mkdir -p "$CACHE_DIR"
        local txz="$CACHE_DIR/zig-${zarch}-linux-${ZIG_VERSION}.tar.xz"
        rm -rf "$root"
        if ! curl -fL --retry 3 -o "$txz" "$url"; then
            echo "error: failed to download $url (is ZIG_VERSION=$ZIG_VERSION a real release?)" >&2
            rm -f "$txz"
            return 1
        fi
        if ! tar -xJf "$txz" -C "$CACHE_DIR"; then
            echo "error: failed to extract $txz" >&2
            rm -f "$txz"
            return 1
        fi
        rm -f "$txz"
    fi
    zig_runnable "$root/zig" || return 1
    ZIG_BIN="$root/zig"
}

# cargo, the cc crate and rustc all want a plain compiler binary they can exec,
# so the -target flag has to be baked into wrapper scripts.
#
# The shims also have to *strip* any target the caller passes: cc-rs appends
# --target=x86_64-unknown-linux-musl (the Rust triple), and zig rejects that
# spelling outright — it reads the `unknown` vendor field as the OS and fails
# with "UnknownOperatingSystem". Dropping it and forcing zig's own triple both
# fixes the build and makes the x86_64 output impossible to override.
#
# They also drop -Wl,-O<n>: rustc hands the linker -Wl,-O1 on release builds and
# zig's lld answers "ignoring deprecated linker optimization setting '1'" on
# stderr, which rustc's linker_messages lint then reports as a build warning.
# The setting is ignored either way, so the only thing lost is the noise.
make_zig_shims() {
    local dir="$CACHE_DIR/zig-shim-${ZIG_TARGET}"
    mkdir -p "$dir"
    local tool
    for tool in cc c++; do
        cat >"$dir/zig-$tool" <<EOF
#!/usr/bin/env bash
args=()
while (( \$# )); do
    case "\$1" in
        --target=*|-target=*) shift ;;
        --target|-target)     if (( \$# >= 2 )); then shift 2; else shift; fi ;;
        -Wl,-O[0-9]*)         shift ;;
        *)                    args+=("\$1"); shift ;;
    esac
done
exec "$ZIG_BIN" $tool -target $ZIG_TARGET "\${args[@]}"
EOF
    done
    cat >"$dir/zig-ar" <<EOF
#!/bin/sh
exec "$ZIG_BIN" ar "\$@"
EOF
    cat >"$dir/zig-ranlib" <<EOF
#!/bin/sh
exec "$ZIG_BIN" ranlib "\$@"
EOF
    chmod +x "$dir/zig-cc" "$dir/zig-c++" "$dir/zig-ar" "$dir/zig-ranlib"
    MUSL_CC="$dir/zig-cc"
    MUSL_CXX="$dir/zig-c++"
    MUSL_AR="$dir/zig-ar"
    MUSL_RANLIB="$dir/zig-ranlib"
}

resolve_toolchain() {
    # Caller-supplied compilers win outright.
    if [[ -n "${MUSL_CC:-}" && -n "${MUSL_CXX:-}" ]]; then
        TOOLCHAIN_KIND="custom"
        return 0
    fi
    case "$MUSL_TOOLCHAIN" in
        gcc)
            resolve_gcc_toolchain || {
                echo "error: MUSL_TOOLCHAIN=gcc but no x86_64 musl gcc/g++ is" \
                     "available on a $HOST_ARCH host" >&2
                exit 1
            }
            TOOLCHAIN_KIND="gcc" ;;
        zig)
            resolve_zig || exit 1
            make_zig_shims
            TOOLCHAIN_KIND="zig" ;;
        auto)
            if resolve_gcc_toolchain; then
                TOOLCHAIN_KIND="gcc"
            else
                echo ">> No x86_64-hosted musl gcc usable on $HOST_ARCH; using zig cc"
                resolve_zig || exit 1
                make_zig_shims
                TOOLCHAIN_KIND="zig"
            fi ;;
        *)
            echo "error: MUSL_TOOLCHAIN must be auto, gcc or zig" >&2
            exit 1 ;;
    esac
}
resolve_toolchain

if [[ -z "${MUSL_AR:-}" ]]; then
    MUSL_AR="$(dirname "$MUSL_CC")/${TOOL_PREFIX}-ar"
    [[ -x "$MUSL_AR" ]] || MUSL_AR="ar"
fi

echo ">> Using toolchain: $TOOLCHAIN_KIND"
echo ">> Using CC=$MUSL_CC"
echo ">> Using CXX=$MUSL_CXX"

# 3. libclang for bindgen (librocksdb-sys). Prefer an already-set path,
#    otherwise probe common locations. The host's libclang is fine — it can
#    target any architecture, it just has to be told which one (step 4).
if [[ -z "${LIBCLANG_PATH:-}" ]]; then
    for d in /usr/lib/llvm-*/lib /usr/lib64 /usr/lib \
             "/usr/lib/${HOST_ARCH}-linux-gnu"; do
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
[[ -n "${MUSL_RANLIB:-}" ]] && export RANLIB_x86_64_unknown_linux_musl="$MUSL_RANLIB"
export LIBCLANG_PATH

# bindgen parses RocksDB's headers with the *host* clang, so the target has to
# be stated explicitly or a non-x86_64 host silently produces bindings for its
# own ABI. The include paths give it the musl C/C++ standard headers to match.
BINDGEN_TARGET_ARGS="--target=$TARGET"
case "$TOOLCHAIN_KIND" in
    zig)
        ZIG_LIB="$(dirname "$ZIG_BIN")/lib"
        [[ -d "$ZIG_LIB" ]] || ZIG_LIB="$("$ZIG_BIN" env | sed -n 's/.*"lib_dir"[^"]*"\([^"]*\)".*/\1/p' | head -1)"
        for inc in "$ZIG_LIB/libc/include/x86_64-linux-musl" \
                   "$ZIG_LIB/libc/include/generic-musl" \
                   "$ZIG_LIB/libc/include/any-linux-any" \
                   "$ZIG_LIB/libc/include/x86-linux-any" \
                   "$ZIG_LIB/libcxx/include"; do
            [[ -d "$inc" ]] && BINDGEN_TARGET_ARGS="$BINDGEN_TARGET_ARGS -I$inc"
        done ;;
    *)
        SYSROOT="$(dirname "$(dirname "$MUSL_CC")")/${TOOL_PREFIX}"
        [[ -d "$SYSROOT" ]] && BINDGEN_TARGET_ARGS="$BINDGEN_TARGET_ARGS --sysroot=$SYSROOT" ;;
esac
export BINDGEN_EXTRA_CLANG_ARGS="$BINDGEN_TARGET_ARGS ${BINDGEN_EXTRA_CLANG_ARGS:-}"

# Statically link the C/C++ runtime into the binary.
RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=+crt-static"
if [[ "$TOOLCHAIN_KIND" == "zig" ]]; then
    # zig ships its own musl startup files and libc. rustc would otherwise add
    # the copies bundled in its rustlib self-contained/ directory too, and the
    # link dies on duplicate _start. Let the C driver own the CRT.
    RUSTFLAGS="$RUSTFLAGS -C link-self-contained=no"
fi
export RUSTFLAGS

# 5. Build.
echo ">> Building rusts3 for $TARGET (release)"
cargo build --release --target "$TARGET" "$@"

BIN="${CARGO_TARGET_DIR:-target}/$TARGET/release/rusts3"

# 6. Prove the result really is a 64-bit x86_64 ELF. On a cross-build a
#    misconfigured toolchain happily emits a host-arch binary, and that mistake
#    is invisible until the binary is shipped. Read the ELF header directly so
#    the check does not depend on file(1) being installed:
#    byte 4 is EI_CLASS (2 = 64-bit), bytes 18-19 are e_machine (62 = x86-64).
read -r elf_class < <(od -An -tu1 -j4 -N1 "$BIN")
read -r m_lo m_hi < <(od -An -tu1 -j18 -N2 "$BIN")
elf_machine=$(( m_lo + m_hi * 256 ))
if (( elf_class != 2 || elf_machine != 62 )); then
    echo "error: $BIN is not a 64-bit x86_64 ELF" \
         "(EI_CLASS=$elf_class, e_machine=$elf_machine; wanted 2 and 62)" >&2
    exit 1
fi
echo ">> Verified x86_64 (e_machine=62), 64-bit ELF"

if command -v readelf >/dev/null 2>&1; then
    if readelf -l "$BIN" 2>/dev/null | grep -q INTERP; then
        echo "warning: $BIN has a PT_INTERP segment — it is not fully static" >&2
    else
        echo ">> Verified static (no PT_INTERP)"
    fi
fi

echo ">> Built $BIN"
file "$BIN" 2>/dev/null || true
