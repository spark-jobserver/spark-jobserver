def currentVersion: String = ("git describe --tags" !!).trim

version in ThisBuild := currentVersion
