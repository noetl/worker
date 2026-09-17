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

# Keep Cargo.lock's own entry for this package in step with Cargo.toml. No
# `--locked` is used anywhere in this repo's build path, so cargo would fix this
# itself; doing it explicitly keeps the lock honest for anyone reading the build
# context, and makes a future `--locked` not a silent landmine.
cargo generate-lockfile --offline >/dev/null 2>&1 || cargo generate-lockfile >/dev/null

echo "stamp-version: Cargo.toml ${BEFORE} -> ${AFTER} (committed value is a floor; the tag is authoritative)"
