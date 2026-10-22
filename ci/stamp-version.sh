#!/usr/bin/env bash
# Stamp the release version into Cargo.toml, in the CI runner only.
#
# ⚠ WHY THIS EXISTS (noetl/ai-meta#330, the release-pipeline fix).
#
# The release used to work like this: semantic-release bumped `version` in
# Cargo.toml and PUSHED that commit to `main`, so the tree at the tag already
# carried the right version. That push is what a required status check on
# `main` rejects — the commit is `[skip ci]`, so the check never runs on it and
# stays "expected" forever (GH006). A branch that is gated and a bot that
# pushes to it cannot both exist here, and the GitHub-native answer (a ruleset
# with the Actions app as a bypass actor) is a paid org feature.
#
# So the TAG is now authoritative and nothing is pushed to `main`. Cargo.toml's
# committed `version` is a floor, not the truth; the release workflow stamps the
# real version here before anything is built.
#
# THIS MATTERS BEYOND THE FILENAME. `CARGO_PKG_VERSION` is compiled into the
# binary and surfaces as `noetl_worker_build_info{version="..."}`. Skip the
# stamp and the image still builds, still deploys, still passes health checks —
# and reports the PREVIOUS version forever. A wrong version that looks right is
# worse than a failed build, which is why the verification below is an
# assertion and not a log line.
set -euo pipefail

VERSION="${1:?usage: stamp-version.sh <version-without-v-prefix>}"

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "stamp-version: refusing a version that is not semver: '${VERSION}'" >&2
  exit 1
fi

BEFORE="$(grep -E '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"

perl -0777 -i -pe "s/^version = \".*?\"/version = \"${VERSION}\"/m" Cargo.toml

AFTER="$(grep -E '^version = ' Cargo.toml | head -1 | cut -d'"' -f2)"
if [[ "${AFTER}" != "${VERSION}" ]]; then
  echo "stamp-version: FAILED — Cargo.toml still reads '${AFTER}', wanted '${VERSION}'" >&2
  exit 1
fi

# Keep Cargo.lock's own entry for this package in step with Cargo.toml: the
# Dockerfile builds with `--locked` (tests/release_image_uses_the_lock.rs), which
# refuses a lock whose root version disagrees with Cargo.toml.
# ⚠⚠ Stamp the ROOT package's entry in Cargo.lock by text, and nothing else
# (noetl/ai-meta#494). This used to be `cargo generate-lockfile`, which
# RE-RESOLVES THE WHOLE GRAPH against the registry at release time — so the
# Dockerfile's `cargo build --locked` (tests/release_image_uses_the_lock.rs, the
# 2026-10-10 E0308 fix) honoured a lock regenerated minutes earlier, not the one
# CI tested. Locally it rewrote 295 lines for a one-line version change. Cargo
# only needs the root entry's version to match Cargo.toml for `--locked` to
# accept the lock; every dependency stays exactly as committed.
NAME="$(awk '/^\[package\]/{p=1;next} /^\[/{p=0} p && /^name = /{gsub(/"/,"",$3); print $3; exit}' Cargo.toml)"
if [[ -z "${NAME}" ]]; then
  echo "stamp-version: could not read [package] name from Cargo.toml" >&2
  exit 1
fi
LOCK_BEFORE="$(mktemp)"
cp Cargo.lock "${LOCK_BEFORE}"
perl -0777 -i -pe "s/(\[\[package\]\]\nname = \"\Q${NAME}\E\"\nversion = )\"[^\"]*\"/\${1}\"${VERSION}\"/" Cargo.lock
LOCK_VERSION="$(awk -v n="${NAME}" '$0=="name = \""n"\"" {getline; gsub(/version = |"/,""); print; exit}' Cargo.lock)"
if [[ "${LOCK_VERSION}" != "${VERSION}" ]]; then
  echo "stamp-version: FAILED — Cargo.lock's ${NAME} entry reads '${LOCK_VERSION}', wanted '${VERSION}'" >&2
  exit 1
fi
# The lock may differ from the committed one in exactly ONE line: the root
# package's version. Anything more means something re-resolved, which is the
# failure this exists to prevent — so it is fatal, not a warning.
CHANGED="$(diff "${LOCK_BEFORE}" Cargo.lock | grep -c '^>' || true)"
rm -f "${LOCK_BEFORE}"
if [[ "${CHANGED}" -gt 1 ]]; then
  echo "stamp-version: FAILED — Cargo.lock changed in ${CHANGED} lines; only the ${NAME} version may change" >&2
  exit 1
fi

echo "stamp-version: Cargo.toml ${BEFORE} -> ${AFTER} (committed value is a floor; the tag is authoritative)"
