#!/bin/sh
# ----------------------------------------------------------------------------
# Apache Maven Wrapper Executable for Linux/macOS
# ----------------------------------------------------------------------------

set -e

MAVEN_VERSION="3.9.6"
MAVEN_USER_HOME="${HOME}/.m2"
MAVEN_DIST_DIR="${MAVEN_USER_HOME}/wrapper/dists/apache-maven-${MAVEN_VERSION}-bin"
MAVEN_HOME="${MAVEN_DIST_DIR}/apache-maven-${MAVEN_VERSION}"
MVN_CMD="${MAVEN_HOME}/bin/mvn"

if [ -f "${MVN_CMD}" ]; then
    exec "${MVN_CMD}" "$@"
fi

echo "[INFO] Apache Maven ${MAVEN_VERSION} not found. Downloading..."
mkdir -p "${MAVEN_DIST_DIR}"
DIST_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.zip"
ZIP_FILE="${MAVEN_DIST_DIR}/maven.zip"

if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "${ZIP_FILE}" "${DIST_URL}"
elif command -v wget >/dev/null 2>&1; then
    wget -qO "${ZIP_FILE}" "${DIST_URL}"
else
    echo "[ERROR] Neither curl nor wget was found."
    exit 1
fi

unzip -qo "${ZIP_FILE}" -d "${MAVEN_DIST_DIR}"
rm -f "${ZIP_FILE}"
chmod +x "${MVN_CMD}"

exec "${MVN_CMD}" "$@"
