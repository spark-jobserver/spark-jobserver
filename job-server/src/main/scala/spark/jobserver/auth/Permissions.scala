package spark.jobserver.auth

case class Permission(name: String, parent: Option[Permission] = None)

object Permission {
  def apply(name: String, parent: Permission): Permission = Permission(name, Some(parent))
}

object Permissions {
  val ALLOW_ALL: Permission = Permission("*")

  val BINARIES: Permission = Permission("binaries")
  val BINARIES_READ: Permission = Permission("binaries:read", BINARIES)
  val BINARIES_UPLOAD: Permission = Permission("binaries:upload", BINARIES)
  val BINARIES_DELETE: Permission = Permission("binaries:delete", BINARIES)

  val CONTEXTS: Permission = Permission("contexts")
  val CONTEXTS_READ: Permission = Permission("contexts:read", CONTEXTS)
  val CONTEXTS_START: Permission = Permission("contexts:start", CONTEXTS)
  val CONTEXTS_DELETE: Permission = Permission("contexts:delete", CONTEXTS)
  val CONTEXTS_RESET: Permission = Permission("contexts:reset", CONTEXTS)

  val DATA: Permission = Permission("data")
  val DATA_READ: Permission = Permission("data:read", DATA)
  val DATA_UPLOAD: Permission = Permission("data:upload", DATA)
  val DATA_DELETE: Permission = Permission("data:delete", DATA)
  val DATA_RESET: Permission = Permission("data:reset", DATA)

  val JOBS: Permission = Permission("jobs")
  val JOBS_READ: Permission = Permission("jobs:read", JOBS)
  val JOBS_START: Permission = Permission("jobs:start", JOBS)
  val JOBS_DELETE: Permission = Permission("jobs:delete", JOBS)

  def apply(name: String): Option[Permission] = {
    val permissions = Seq(ALLOW_ALL,
      BINARIES, BINARIES_READ, BINARIES_UPLOAD, BINARIES_DELETE,
      CONTEXTS, CONTEXTS_READ, CONTEXTS_START, CONTEXTS_DELETE, CONTEXTS_RESET,
      DATA, DATA_READ, DATA_UPLOAD, DATA_DELETE, DATA_RESET,
      JOBS, JOBS_READ, JOBS_START, JOBS_DELETE)
    permissions.find(_.name == name)
  }
}
