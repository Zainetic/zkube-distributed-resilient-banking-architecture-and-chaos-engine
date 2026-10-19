@REM ----------------------------------------------------------------------------
@REM Apache Maven Wrapper Executable for Windows
@REM ----------------------------------------------------------------------------
@echo off
setlocal

set "DIR=%~dp0"
set "MAVEN_VERSION=3.9.6"
set "MAVEN_USER_HOME=%USERPROFILE%\.m2"
set "MAVEN_DIST_DIR=%MAVEN_USER_HOME%\wrapper\dists\apache-maven-%MAVEN_VERSION%-bin"
set "MAVEN_HOME=%MAVEN_DIST_DIR%\apache-maven-%MAVEN_VERSION%"
set "MVN_CMD=%MAVEN_HOME%\bin\mvn.cmd"

if exist "%MVN_CMD%" goto runMvn

echo [INFO] Apache Maven %MAVEN_VERSION% not found in wrapper directory.
echo [INFO] Downloading Maven %MAVEN_VERSION%...

if not exist "%MAVEN_DIST_DIR%" mkdir "%MAVEN_DIST_DIR%"

set "DIST_URL=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/%MAVEN_VERSION%/apache-maven-%MAVEN_VERSION%-bin.zip"
set "ZIP_FILE=%MAVEN_DIST_DIR%\apache-maven-%MAVEN_VERSION%-bin.zip"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('%DIST_URL%', '%ZIP_FILE%'); Expand-Archive -Path '%ZIP_FILE%' -DestinationPath '%MAVEN_DIST_DIR%' -Force; Remove-Item '%ZIP_FILE%'"

if not exist "%MVN_CMD%" (
    echo [ERROR] Failed to download or extract Maven. Please ensure an active internet connection.
    exit /b 1
)

:runMvn
"%MVN_CMD%" %*
exit /b %ERRORLEVEL%
