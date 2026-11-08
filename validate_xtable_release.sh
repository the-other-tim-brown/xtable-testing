#!/usr/bin/env bash
#
# Apache XTable Release Candidate Validator
# ==========================================
#
# Validates BOTH:
#
#   1. The Apache source release candidate from dist.apache.org
#   2. Maven artifacts staged in repository.apache.org
#
# Intended for validating Apache XTable release votes.
#
#
# ---------------------------------------------------------------------------
# QUICK START
# ---------------------------------------------------------------------------
#
# Requirements:
#
#   - Java 11
#   - curl
#   - gpg
#   - tar
#   - unzip
#   - shasum (macOS) or sha512sum/sha1sum (Linux)
#
# Run:
#
#   chmod +x validate-xtable-release.sh
#   ./validate-xtable-release.sh
#
#
# The defaults below validate XTable 0.4.0-incubating RC2:
#
#   Source RC:
#     https://dist.apache.org/repos/dist/dev/incubator/xtable/0.4.0-incubating-rc2/
#
#   Maven staging:
#     https://repository.apache.org/content/repositories/orgapachextable-1016/org/apache/xtable/
#
#
# You can override these:
#
#   RC_URL=https://... \
#   NEXUS_URL=https://... \
#   VERSION=0.4.0-incubating \
#   ./validate-xtable-release.sh
#
#
# ---------------------------------------------------------------------------
# OPTIONAL SETTINGS
# ---------------------------------------------------------------------------
#
# Skip building the source distribution:
#
#   BUILD_SOURCE=false ./validate-xtable-release.sh
#
# Skip Maven RAT:
#
#   RUN_RAT=false ./validate-xtable-release.sh
#
# Keep downloaded/extracted files:
#
#   WORK_DIR=/tmp/xtable-validation ./validate-xtable-release.sh
#
#
# ---------------------------------------------------------------------------
# HOW TO INTERPRET RESULTS
# ---------------------------------------------------------------------------
#
# PASS
#   Automated check succeeded.
#
# WARN / REVIEW
#   Not automatically release-blocking, but should be inspected manually.
#
#   The most important example is bundled/shaded third-party code.
#   If a JAR contains dependency classes, verify that its LICENSE/NOTICE
#   correctly account for those dependencies.
#
# FAIL
#   Release validation failed.
#
#   A FAIL should generally be resolved before casting a +1 vote.
#
#
# Exit codes:
#
#   0 = no automated failures (warnings may exist)
#   1 = one or more release validation failures
#   2 = validator/configuration problem
#
#
# IMPORTANT:
#
# This script automates mechanical release checks. It cannot prove that
# LICENSE/NOTICE perfectly account for every bundled third-party work.
#
# Any JAR reported as containing third-party classes should receive a
# manual licensing review.
#

set -u
set -o pipefail


###############################################################################
# Configuration
###############################################################################

VERSION="${VERSION:-0.4.0-incubating}"

RC_URL="${RC_URL:-https://dist.apache.org/repos/dist/dev/incubator/xtable/0.4.0-incubating-rc2}"
NEXUS_URL="${NEXUS_URL:-https://repository.apache.org/content/repositories/orgapachextable-1016/org/apache/xtable}"

KEYS_URL="${KEYS_URL:-https://dist.apache.org/repos/dist/release/incubator/xtable/KEYS}"

WORK_DIR="${WORK_DIR:-$(pwd)/xtable-release-validation}"

BUILD_SOURCE="${BUILD_SOURCE:-true}"
RUN_RAT="${RUN_RAT:-true}"


###############################################################################
# State
###############################################################################

FAILURES=0
WARNINGS=0
PASSES=0
JARS_CHECKED=0
POMS_CHECKED=0

REPORT_FILE="${WORK_DIR}/validation-report.txt"


###############################################################################
# Output helpers
###############################################################################

if [ -t 1 ]; then
  RED="$(printf '\033[31m')"
  YELLOW="$(printf '\033[33m')"
  GREEN="$(printf '\033[32m')"
  BLUE="$(printf '\033[34m')"
  RESET="$(printf '\033[0m')"
else
  RED=""
  YELLOW=""
  GREEN=""
  BLUE=""
  RESET=""
fi


log() {
  printf '%s\n' "$*" | tee -a "$REPORT_FILE"
}


pass() {
  PASSES=$((PASSES + 1))
  log "${GREEN}PASS${RESET}   $*"
}


warn() {
  WARNINGS=$((WARNINGS + 1))
  log "${YELLOW}REVIEW${RESET} $*"
}


fail() {
  FAILURES=$((FAILURES + 1))
  log "${RED}FAIL${RESET}   $*"
}


section() {
  log ""
  log "${BLUE}===============================================================================${RESET}"
  log "${BLUE}$*${RESET}"
  log "${BLUE}===============================================================================${RESET}"
}


subsection() {
  log ""
  log "${BLUE}--- $* ---${RESET}"
}


###############################################################################
# Utility functions
###############################################################################

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'ERROR: required command is missing: %s\n' "$1" >&2
    exit 2
  fi
}


download() {
  url="$1"
  output="$2"

  if [ -f "$output" ]; then
    return 0
  fi

  curl \
    --fail \
    --location \
    --silent \
    --show-error \
    "$url" \
    --output "$output"
}


url_exists() {
  curl \
    --fail \
    --location \
    --silent \
    --head \
    "$1" \
    >/dev/null 2>&1
}


sha512_file() {
  file="$1"

  if command -v sha512sum >/dev/null 2>&1; then
    sha512sum "$file" | awk '{print $1}'
  else
    shasum -a 512 "$file" | awk '{print $1}'
  fi
}


sha1_file() {
  file="$1"

  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum "$file" | awk '{print $1}'
  else
    shasum -a 1 "$file" | awk '{print $1}'
  fi
}


md5_file() {
  file="$1"

  if command -v md5sum >/dev/null 2>&1; then
    md5sum "$file" | awk '{print $1}'
  elif command -v md5 >/dev/null 2>&1; then
    md5 -q "$file"
  else
    return 1
  fi
}


normalize_hash_file() {
  #
  # Handles either:
  #
  #   abc123...
  #
  # or:
  #
  #   filename: abc123...
  #
  # or standard checksum syntax.
  #

  tr -d '\r' < "$1" |
    awk '{
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^[A-Fa-f0-9]+$/) {
          print tolower($i)
          exit
        }
      }
    }'
}


###############################################################################
# Prerequisite checks
###############################################################################

mkdir -p "$WORK_DIR"
: > "$REPORT_FILE"

section "Environment"

require_command curl
require_command gpg
require_command tar
require_command unzip
require_command grep
require_command sed
require_command awk
require_command find
require_command sort
require_command java

if ! command -v shasum >/dev/null 2>&1 &&
   ! command -v sha512sum >/dev/null 2>&1; then
  echo "Need either shasum or sha512sum." >&2
  exit 2
fi

JAVA_VERSION="$(java -version 2>&1 | head -1)"

log "Java:       ${JAVA_VERSION}"
log "Work dir:   ${WORK_DIR}"
log "Version:    ${VERSION}"
log "Source RC:  ${RC_URL}"
log "Nexus:      ${NEXUS_URL}"


###############################################################################
# Download and import Apache XTable signing keys
###############################################################################

section "PGP signing keys"

KEYS_FILE="${WORK_DIR}/KEYS"

if download "$KEYS_URL" "$KEYS_FILE"; then
  pass "Downloaded Apache XTable KEYS"
else
  fail "Could not download XTable KEYS from ${KEYS_URL}"
fi


#
# Use an isolated GPG home.
#
# This prevents the script from modifying the user's normal ~/.gnupg keyring.
#

GNUPGHOME="${WORK_DIR}/gnupg"
export GNUPGHOME

mkdir -p "$GNUPGHOME"
chmod 700 "$GNUPGHOME"

if gpg --batch --import "$KEYS_FILE" >>"$REPORT_FILE" 2>&1; then
  pass "Imported Apache XTable signing keys"
else
  fail "Could not import Apache XTable signing keys"
fi


###############################################################################
# Source RC validation
###############################################################################

section "1. Source release candidate"


#
# Discover the .src.tgz dynamically.
#

RC_LISTING="${WORK_DIR}/rc-index.html"

if ! download "${RC_URL}/" "$RC_LISTING"; then
  fail "Could not read source RC directory"
  exit 1
fi


SOURCE_NAME="$(
  grep -Eo 'href="[^"]+\.src\.tgz"' "$RC_LISTING" |
    sed 's/^href="//; s/"$//' |
    head -1
)"


if [ -z "$SOURCE_NAME" ]; then
  fail "Could not discover .src.tgz in RC directory"
else
  pass "Found source artifact: ${SOURCE_NAME}"
fi


SOURCE_FILE="${WORK_DIR}/${SOURCE_NAME}"
SOURCE_ASC="${SOURCE_FILE}.asc"
SOURCE_SHA512="${SOURCE_FILE}.sha512"


###############################################################################
# Download source + sidecars
###############################################################################

subsection "Download source release"

if download "${RC_URL}/${SOURCE_NAME}" "$SOURCE_FILE"; then
  pass "Downloaded ${SOURCE_NAME}"
else
  fail "Failed downloading ${SOURCE_NAME}"
fi


if download "${RC_URL}/${SOURCE_NAME}.asc" "$SOURCE_ASC"; then
  pass "Downloaded PGP signature"
else
  fail "Source release is missing .asc signature"
fi


if download "${RC_URL}/${SOURCE_NAME}.sha512" "$SOURCE_SHA512"; then
  pass "Downloaded SHA-512 checksum"
else
  fail "Source release is missing .sha512 checksum"
fi


###############################################################################
# Verify SHA-512
###############################################################################

subsection "Source SHA-512"

EXPECTED_SHA512="$(normalize_hash_file "$SOURCE_SHA512")"
ACTUAL_SHA512="$(sha512_file "$SOURCE_FILE")"

if [ "$EXPECTED_SHA512" = "$ACTUAL_SHA512" ]; then
  pass "Source SHA-512 matches"
else
  fail "Source SHA-512 mismatch"
  log "       Expected: ${EXPECTED_SHA512}"
  log "       Actual:   ${ACTUAL_SHA512}"
fi


###############################################################################
# Verify PGP signature
###############################################################################

subsection "Source PGP signature"

GPG_OUTPUT="${WORK_DIR}/source-gpg.txt"

if gpg \
  --batch \
  --status-fd=1 \
  --verify "$SOURCE_ASC" "$SOURCE_FILE" \
  >"$GPG_OUTPUT" 2>&1; then

  pass "Source PGP signature is valid"

  SIGNER="$(
    grep '^\[GNUPG:\] GOODSIG ' "$GPG_OUTPUT" |
      sed 's/^\[GNUPG:\] GOODSIG //' |
      head -1
  )"

  FINGERPRINT="$(
    grep '^\[GNUPG:\] VALIDSIG ' "$GPG_OUTPUT" |
      awk '{print $3}' |
      head -1
  )"

  [ -n "$SIGNER" ] &&
    log "       Signer:      ${SIGNER}"

  [ -n "$FINGERPRINT" ] &&
    log "       Fingerprint: ${FINGERPRINT}"

else
  fail "Source PGP signature verification failed"
  cat "$GPG_OUTPUT" >>"$REPORT_FILE"
fi


###############################################################################
# Extract source
###############################################################################

subsection "Source archive structure"

EXTRACT_DIR="${WORK_DIR}/source"

rm -rf "$EXTRACT_DIR"
mkdir -p "$EXTRACT_DIR"

if tar -xzf "$SOURCE_FILE" -C "$EXTRACT_DIR"; then
  pass "Source archive extracts successfully"
else
  fail "Source archive could not be extracted"
fi


#
# ASF source releases should normally have one top-level directory.
#

TOP_LEVEL_COUNT="$(
  tar -tzf "$SOURCE_FILE" |
    sed 's#^\./##' |
    cut -d/ -f1 |
    grep -v '^$' |
    sort -u |
    wc -l |
    tr -d ' '
)"


if [ "$TOP_LEVEL_COUNT" = "1" ]; then
  pass "Source archive has one top-level directory"
else
  warn "Source archive has ${TOP_LEVEL_COUNT} top-level entries"
fi


SOURCE_ROOT="$(
  find "$EXTRACT_DIR" \
    -mindepth 1 \
    -maxdepth 1 \
    -type d \
    | head -1
)"


if [ -z "$SOURCE_ROOT" ]; then
  fail "Could not determine extracted source root"
else
  log "       Source root: ${SOURCE_ROOT}"
fi


###############################################################################
# LICENSE / NOTICE
###############################################################################

subsection "Source LICENSE and NOTICE"

if [ -f "${SOURCE_ROOT}/LICENSE" ]; then
  pass "Source distribution contains LICENSE"
else
  fail "Source distribution is missing LICENSE"
fi


if [ -f "${SOURCE_ROOT}/NOTICE" ]; then
  pass "Source distribution contains NOTICE"
else
  fail "Source distribution is missing NOTICE"
fi


if [ -f "${SOURCE_ROOT}/LICENSE" ]; then
  if grep -qi "Apache License" "${SOURCE_ROOT}/LICENSE" &&
     grep -qi "Version 2.0" "${SOURCE_ROOT}/LICENSE"; then

    pass "LICENSE contains Apache License 2.0"

  else
    fail "LICENSE does not appear to contain Apache License 2.0"
  fi
fi


if [ -f "${SOURCE_ROOT}/NOTICE" ]; then
  if grep -qi "Apache XTable" "${SOURCE_ROOT}/NOTICE"; then
    pass "NOTICE identifies Apache XTable"
  else
    warn "NOTICE does not appear to identify Apache XTable"
  fi
fi


###############################################################################
# Unexpected binaries in source release
###############################################################################

subsection "Unexpected binaries in source distribution"

BINARY_LIST="${WORK_DIR}/source-binaries.txt"

find "$SOURCE_ROOT" \
  -type f \
  \( \
    -name '*.jar' \
    -o -name '*.war' \
    -o -name '*.class' \
    -o -name '*.exe' \
    -o -name '*.dll' \
    -o -name '*.so' \
    -o -name '*.dylib' \
  \) \
  >"$BINARY_LIST"


if [ -s "$BINARY_LIST" ]; then

  warn "Source distribution contains binary artifacts"

  while IFS= read -r file; do
    log "       ${file#$SOURCE_ROOT/}"
  done < "$BINARY_LIST"

else
  pass "No obvious compiled binaries found in source distribution"
fi


###############################################################################
# SNAPSHOT check
###############################################################################

subsection "Release version checks"

SNAPSHOT_RESULTS="${WORK_DIR}/snapshot-references.txt"

grep \
  -R \
  -n \
  --include='pom.xml' \
  --include='*.xml' \
  '0\.4\.0-SNAPSHOT\|0\.4\.0-incubating-SNAPSHOT' \
  "$SOURCE_ROOT" \
  >"$SNAPSHOT_RESULTS" 2>/dev/null || true


if [ -s "$SNAPSHOT_RESULTS" ]; then

  fail "Release source contains ${VERSION}-SNAPSHOT references"

  cat "$SNAPSHOT_RESULTS" >>"$REPORT_FILE"

else
  pass "No ${VERSION}-SNAPSHOT references detected in Maven metadata"
fi


###############################################################################
# RAT
###############################################################################

if [ "$RUN_RAT" = "true" ]; then

  subsection "Apache RAT"

  if [ -x "${SOURCE_ROOT}/mvnw" ]; then

    (
      cd "$SOURCE_ROOT" || exit 1
      ./mvnw apache-rat:check
    ) >"${WORK_DIR}/rat.log" 2>&1

    RAT_RESULT=$?

    if [ "$RAT_RESULT" -eq 0 ]; then
      pass "Apache RAT check passed"
    else
      fail "Apache RAT check failed"
      log "       See: ${WORK_DIR}/rat.log"
    fi

  else
    warn "mvnw is not executable; RAT was not run"
  fi

else
  warn "Apache RAT check skipped because RUN_RAT=false"
fi


###############################################################################
# Build from source
###############################################################################

if [ "$BUILD_SOURCE" = "true" ]; then

  subsection "Build source release"

  if [ -x "${SOURCE_ROOT}/mvnw" ]; then

    log "Running:"
    log "       ./mvnw clean package"
    log ""
    log "Build output:"
    log "       ${WORK_DIR}/source-build.log"

    (
      cd "$SOURCE_ROOT" || exit 1
      ./mvnw clean package
    ) >"${WORK_DIR}/source-build.log" 2>&1

    BUILD_RESULT=$?

    if [ "$BUILD_RESULT" -eq 0 ]; then
      pass "Source release builds successfully with ./mvnw clean package"
    else
      fail "Source build failed"
      log "       See: ${WORK_DIR}/source-build.log"
    fi

  else
    fail "Source distribution does not contain executable ./mvnw"
  fi

else
  warn "Source build skipped because BUILD_SOURCE=false"
fi


###############################################################################
# Nexus validation
###############################################################################

section "2. Maven staging repository"

NEXUS_DIR="${WORK_DIR}/nexus"
mkdir -p "$NEXUS_DIR"

VERSION="${VERSION:-0.4.0-incubating}"

RC_URL="${RC_URL:-https://dist.apache.org/repos/dist/dev/incubator/xtable/0.4.0-incubating-rc2}"
NEXUS_URL="${NEXUS_URL:-https://repository.apache.org/content/repositories/orgapachextable-1016/org/apache/xtable}"

KEYS_URL="${KEYS_URL:-https://dist.apache.org/repos/dist/release/incubator/xtable/KEYS}"

# Normalize directory URLs so later URL construction is predictable.
RC_URL="${RC_URL%/}"
NEXUS_URL="${NEXUS_URL%/}"

###############################################################################
# Discover Maven modules
###############################################################################

MODULE_LIST="${WORK_DIR}/modules.txt"

if ! curl \
  --fail \
  --location \
  --silent \
  --show-error \
  "${NEXUS_URL}/" |
  grep -Eo 'href="[^"]+/"' |
  sed 's/^href="//; s/"$//' |
  sed 's#/$##' |
  sed 's#.*/##' |
  grep '^xtable' |
  sort -u \
  > "$MODULE_LIST"
then
  fail "Could not read Maven module listing from ${NEXUS_URL}/"
  exit 1
fi

MODULE_COUNT="$(wc -l < "$MODULE_LIST" | tr -d ' ')"

if [ "$MODULE_COUNT" -gt 0 ]; then
  pass "Discovered ${MODULE_COUNT} Maven modules"
else
  fail "No Maven modules discovered"
  exit 1
fi

log ""
log "Discovered modules:"

while IFS= read -r module; do
  log "    ${module}"
done < "$MODULE_LIST"

MODULE_COUNT="$(wc -l < "$MODULE_LIST" | tr -d ' ')"

if [ "$MODULE_COUNT" -gt 0 ]; then
  pass "Discovered ${MODULE_COUNT} Maven modules"
else
  fail "No Maven modules discovered"
fi


while IFS= read -r module; do

  [ -z "$module" ] && continue

  subsection "Maven module: ${module}"

  MODULE_URL="${NEXUS_URL}/${module}/${VERSION}"
  MODULE_DIR="${NEXUS_DIR}/${module}"

  mkdir -p "$MODULE_DIR"

  INDEX_FILE="${MODULE_DIR}/index.html"

  if ! download "${MODULE_URL}/" "$INDEX_FILE"; then
    fail "${module}: could not read ${MODULE_URL}"
    continue
  fi


  ###########################################################################
  # Discover all JARs
  ###########################################################################

  JAR_LIST="${MODULE_DIR}/jars.txt"

  grep -Eo 'href="[^"]+\.jar"' "$INDEX_FILE" |
    sed 's/^href="//; s/"$//' |
    sed 's#.*/##' |
    sort -u \
    >"$JAR_LIST"


  if [ ! -s "$JAR_LIST" ]; then
    warn "${module}: no JAR artifacts found"
  fi


  ###########################################################################
  # Discover POM
  ###########################################################################

  POM_LIST="${MODULE_DIR}/poms.txt"

  grep -Eo 'href="[^"]+\.pom"' "$INDEX_FILE" |
    sed 's/^href="//; s/"$//' |
    sed 's#.*/##' |
    sort -u \
    >"$POM_LIST"


  ###########################################################################
  # Validate JARs
  ###########################################################################

  while IFS= read -r jar_name; do

    [ -z "$jar_name" ] && continue

    JARS_CHECKED=$((JARS_CHECKED + 1))

    log ""
    log "  Artifact: ${jar_name}"

    JAR_FILE="${MODULE_DIR}/${jar_name}"


    #########################################################################
    # Download JAR
    #########################################################################

    if download "${MODULE_URL}/${jar_name}" "$JAR_FILE"; then
      pass "${jar_name}: downloaded"
    else
      fail "${jar_name}: download failed"
      continue
    fi


    #########################################################################
    # Download checksum/signature sidecars
    #########################################################################

    for suffix in asc sha1 md5; do

      if download \
        "${MODULE_URL}/${jar_name}.${suffix}" \
        "${JAR_FILE}.${suffix}" \
        2>/dev/null; then
        :
      else
        rm -f "${JAR_FILE}.${suffix}"
      fi

    done


    #########################################################################
    # PGP
    #########################################################################

    if [ -f "${JAR_FILE}.asc" ]; then

      if gpg \
        --batch \
        --verify "${JAR_FILE}.asc" "$JAR_FILE" \
        >/dev/null 2>&1; then

        pass "${jar_name}: valid PGP signature"

      else
        fail "${jar_name}: invalid PGP signature"
      fi

    else
      fail "${jar_name}: missing .asc signature"
    fi


    #########################################################################
    # SHA-1
    #########################################################################

    if [ -f "${JAR_FILE}.sha1" ]; then

      EXPECTED="$(normalize_hash_file "${JAR_FILE}.sha1")"
      ACTUAL="$(sha1_file "$JAR_FILE")"

      if [ "$EXPECTED" = "$ACTUAL" ]; then
        pass "${jar_name}: SHA-1 matches"
      else
        fail "${jar_name}: SHA-1 mismatch"
      fi

    else
      warn "${jar_name}: no SHA-1 sidecar"
    fi


    #########################################################################
    # MD5
    #########################################################################

    if [ -f "${JAR_FILE}.md5" ]; then

      if ACTUAL="$(md5_file "$JAR_FILE")"; then

        EXPECTED="$(normalize_hash_file "${JAR_FILE}.md5")"

        if [ "$EXPECTED" = "$ACTUAL" ]; then
          pass "${jar_name}: MD5 matches"
        else
          fail "${jar_name}: MD5 mismatch"
        fi

      else
        warn "${jar_name}: MD5 exists but no local MD5 utility available"
      fi
    fi


    #########################################################################
    # JAR integrity
    #########################################################################

    if unzip -t "$JAR_FILE" >/dev/null 2>&1; then
      pass "${jar_name}: JAR/ZIP structure valid"
    else
      fail "${jar_name}: corrupt or invalid JAR"
      continue
    fi


    #########################################################################
    # META-INF/LICENSE and NOTICE
    #########################################################################

    CONTENTS="${MODULE_DIR}/${jar_name}.contents"

    unzip -Z1 "$JAR_FILE" >"$CONTENTS"


    LICENSE_PATH="$(
      grep -Ei '^META-INF/LICENSE(\.txt|\.md)?$' "$CONTENTS" |
        head -1
    )"


    NOTICE_PATH="$(
      grep -Ei '^META-INF/NOTICE(\.txt|\.md)?$' "$CONTENTS" |
        head -1
    )"


    if [ -n "$LICENSE_PATH" ]; then
      pass "${jar_name}: contains ${LICENSE_PATH}"

      if unzip -p "$JAR_FILE" "$LICENSE_PATH" |
         grep -qi "Apache License"; then

        pass "${jar_name}: LICENSE identifies Apache License"

      else
        fail "${jar_name}: LICENSE does not identify Apache License"
      fi

    else
      fail "${jar_name}: missing META-INF/LICENSE"
    fi


    if [ -n "$NOTICE_PATH" ]; then
      pass "${jar_name}: contains ${NOTICE_PATH}"

      if unzip -p "$JAR_FILE" "$NOTICE_PATH" |
         grep -qi "Apache XTable"; then

        pass "${jar_name}: NOTICE identifies Apache XTable"

      else
        warn "${jar_name}: NOTICE does not appear to mention Apache XTable"
      fi

    else
      fail "${jar_name}: missing META-INF/NOTICE"
    fi


    #########################################################################
    # SNAPSHOT metadata
    #########################################################################

    if unzip -p "$JAR_FILE" META-INF/MANIFEST.MF 2>/dev/null |
       grep -qi SNAPSHOT; then

      fail "${jar_name}: MANIFEST contains SNAPSHOT"

    else
      pass "${jar_name}: no SNAPSHOT in manifest"
    fi


    #########################################################################
    # Nested archives
    #########################################################################

    NESTED="$(
      grep -Ei '\.(jar|war|zip)$' "$CONTENTS" |
        grep -vF "$jar_name" ||
        true
    )"

    if [ -n "$NESTED" ]; then

      warn "${jar_name}: contains nested archive(s)"

      printf '%s\n' "$NESTED" |
        while IFS= read -r nested; do
          log "       ${nested}"
        done

    else
      pass "${jar_name}: no nested JAR/WAR/ZIP archives"
    fi


    #########################################################################
    # Bundled / shaded third-party classes
    #
    # This is deliberately a REVIEW rather than FAIL.
    #
    # Finding a dependency namespace means the JAR may contain third-party
    # code. Its LICENSE/NOTICE must then be reviewed against those contents.
    #########################################################################

    #
    # Skip this heuristic for source and javadoc JARs.
    #

    case "$jar_name" in

      *-sources.jar|*-javadoc.jar)
        ;;

      *)

        THIRD_PARTY_FILE="${MODULE_DIR}/${jar_name}.third-party-packages"

        : >"$THIRD_PARTY_FILE"

        for prefix in \
          "com/fasterxml/" \
          "com/github/" \
          "com/google/" \
          "com/sun/" \
          "io/netty/" \
          "org/apache/avro/" \
          "org/apache/commons/" \
          "org/apache/hadoop/" \
          "org/apache/hudi/" \
          "org/apache/iceberg/" \
          "org/apache/parquet/" \
          "org/codehaus/" \
          "org/slf4j/" \
          "software/amazon/awssdk/"

        do

          if grep -q "^${prefix}" "$CONTENTS"; then
            printf '%s\n' "$prefix" >>"$THIRD_PARTY_FILE"
          fi

        done


        if [ -s "$THIRD_PARTY_FILE" ]; then

          warn "${jar_name}: appears to bundle third-party classes"

          while IFS= read -r prefix; do
            log "       ${prefix}"
          done <"$THIRD_PARTY_FILE"

          log "       ACTION: verify these bundled works are accounted for"
          log "               by this JAR's LICENSE/NOTICE."

        else
          pass "${jar_name}: no obvious third-party namespaces detected"
        fi

        ;;

    esac


    #########################################################################
    # Extra legal metadata
    #########################################################################

    EXTRA_LEGAL="$(
      grep -Ei \
        '(^|/)(LICENSES|COPYING|COPYRIGHT|THIRD[-_.]?PARTY|DEPENDENCIES)(/|\.|$)' \
        "$CONTENTS" ||
        true
    )"

    if [ -n "$EXTRA_LEGAL" ]; then

      warn "${jar_name}: contains additional legal metadata"

      printf '%s\n' "$EXTRA_LEGAL" |
        while IFS= read -r entry; do
          log "       ${entry}"
        done
    fi


  done <"$JAR_LIST"


  ###########################################################################
  # Validate Maven POM signatures/checksums too.
  #
  # POMs are part of the Maven release and are worth checking even though
  # this validator's primary focus is the JARs.
  ###########################################################################

  while IFS= read -r pom_name; do

    [ -z "$pom_name" ] && continue

    POMS_CHECKED=$((POMS_CHECKED + 1))

    POM_FILE="${MODULE_DIR}/${pom_name}"

    if ! download "${MODULE_URL}/${pom_name}" "$POM_FILE"; then
      fail "${pom_name}: download failed"
      continue
    fi


    for suffix in asc sha1; do

      if download \
        "${MODULE_URL}/${pom_name}.${suffix}" \
        "${POM_FILE}.${suffix}" \
        2>/dev/null; then
        :
      else
        rm -f "${POM_FILE}.${suffix}"
      fi

    done


    if [ -f "${POM_FILE}.asc" ]; then

      if gpg \
        --batch \
        --verify "${POM_FILE}.asc" "$POM_FILE" \
        >/dev/null 2>&1; then

        pass "${pom_name}: valid PGP signature"

      else
        fail "${pom_name}: invalid PGP signature"
      fi

    else
      fail "${pom_name}: missing PGP signature"
    fi


    if grep -q '<version>.*SNAPSHOT.*</version>' "$POM_FILE"; then
      fail "${pom_name}: contains SNAPSHOT version"
    else
      pass "${pom_name}: no SNAPSHOT project version"
    fi


  done <"$POM_LIST"


done <"$MODULE_LIST"


###############################################################################
# Optional comparison with artifacts built from source
###############################################################################

section "3. Source-build vs Maven staging sanity check"

if [ "$BUILD_SOURCE" = "true" ] &&
   [ -d "$SOURCE_ROOT" ]; then

  BUILT_JARS="${WORK_DIR}/built-jars.txt"
  STAGED_MAIN_JARS="${WORK_DIR}/staged-main-jars.txt"

  find "$SOURCE_ROOT" \
    -path '*/target/*.jar' \
    -type f \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    ! -name 'original-*.jar' \
    -exec basename {} \; \
    | sort -u \
    >"$BUILT_JARS"


  find "$NEXUS_DIR" \
    -type f \
    -name '*.jar' \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    -exec basename {} \; \
    | sort -u \
    >"$STAGED_MAIN_JARS"


  if [ -s "$BUILT_JARS" ]; then
    pass "Found JARs produced by source build"
  else
    warn "No built JARs discovered under target/"
  fi


  log ""
  log "Built artifacts not found in Nexus staging:"

  MISSING_STAGED="$(
    comm -23 "$BUILT_JARS" "$STAGED_MAIN_JARS" ||
    true
  )"

  if [ -n "$MISSING_STAGED" ]; then

    warn "Some source-build JAR names are not present in Nexus"

    printf '%s\n' "$MISSING_STAGED" |
      while IFS= read -r jar; do
        log "       ${jar}"
      done

  else
    pass "All relevant source-build JAR names appear in Nexus staging"
  fi


  log ""
  log "Nexus main artifacts not produced by this source build:"

  EXTRA_STAGED="$(
    comm -13 "$BUILT_JARS" "$STAGED_MAIN_JARS" ||
    true
  )"

  if [ -n "$EXTRA_STAGED" ]; then

    warn "Some Nexus JAR names were not found in the local source build"

    printf '%s\n' "$EXTRA_STAGED" |
      while IFS= read -r jar; do
        log "       ${jar}"
      done

  else
    pass "No unexpected main Maven JAR names detected"
  fi

else
  warn "Source-build comparison skipped"
fi


###############################################################################
# Final result
###############################################################################

section "VALIDATION SUMMARY"

log "Source release:"
log "    ${SOURCE_NAME}"
log ""
log "Maven staging:"
log "    ${NEXUS_URL}"
log ""
log "Artifacts inspected:"
log "    JARs: ${JARS_CHECKED}"
log "    POMs: ${POMS_CHECKED}"
log ""
log "Results:"
log "    PASS:   ${PASSES}"
log "    REVIEW: ${WARNINGS}"
log "    FAIL:   ${FAILURES}"
log ""
log "Full report:"
log "    ${REPORT_FILE}"
log ""


if [ "$FAILURES" -gt 0 ]; then

  log "${RED}RESULT: RELEASE VALIDATION FAILED${RESET}"
  log ""
  log "At least one automated release check failed."
  log "Investigate these failures before casting a +1 release vote."

  exit 1

fi


if [ "$WARNINGS" -gt 0 ]; then

  log "${YELLOW}RESULT: AUTOMATED CHECKS PASSED, MANUAL REVIEW REQUIRED${RESET}"
  log ""
  log "No automated blocking failures were found."
  log ""
  log "Review every REVIEW item, especially JARs containing bundled or"
  log "shaded third-party classes. Confirm LICENSE and NOTICE accurately"
  log "describe the exact contents of those JARs before voting +1."

  exit 0

fi


log "${GREEN}RESULT: ALL AUTOMATED CHECKS PASSED${RESET}"
log ""
log "No automated failures or review items were detected."
log "You should still perform the normal human release review before voting."

exit 0
