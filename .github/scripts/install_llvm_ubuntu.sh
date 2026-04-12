#!/usr/bin/env bash
set -eo pipefail

v=22
bins=(clang llvm-config lld ld.lld FileCheck)

# Detect distro codename.
. /etc/os-release
distro="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
case "$distro" in
    bullseye|bookworm|trixie|noble|jammy|focal) ;;
    *) distro="unstable" ;;
esac

# Add LLVM apt source.
apt-get update -qq
apt-get install -y --no-install-recommends wget gnupg ca-certificates
wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key \
    | gpg --dearmor -o /usr/share/keyrings/llvm-archive-keyring.gpg

if [ "$distro" = "unstable" ]; then
    repo="llvm-toolchain-$v"
else
    repo="llvm-toolchain-$distro-$v"
fi
echo "deb [signed-by=/usr/share/keyrings/llvm-archive-keyring.gpg] https://apt.llvm.org/$distro/ $repo main" \
    > /etc/apt/sources.list.d/llvm-$v.list

apt-get update -qq
apt-get install -y --no-install-recommends clang-$v llvm-$v-dev lld-$v

for bin in "${bins[@]}"; do
    if ! command -v "$bin-$v" &>/dev/null; then
        echo "Warning: $bin-$v not found" 1>&2
        continue
    fi
    ln -fs "$(which "$bin-$v")" "/usr/bin/$bin"
done

echo "LLVM $v installed:"
llvm-config --version
