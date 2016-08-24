def currentVersion: String = ("git describe --tags --match v*" !!).trim.substring(1)

version in ThisBuild := currentVersion
