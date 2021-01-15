import java.io.File

import scala.sys.process.Process

object PythonTasks {
  val ext : String = if (System.getProperty("os.name").indexOf("Win") >= 0) "cmd" else "sh"
  val pythonExecutable: String = sys.env.getOrElse("PYTHON_EXECUTABLE", "python3")

  def workingDirectory(baseDirectory: File): File =
    new File(baseDirectory.getAbsolutePath + Seq("src", "python")
      .mkString("/", "/", ""))

  def testPythonTask(baseDirectory: File): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(Seq(cwd.getAbsolutePath + "/run-tests." + ext, pythonExecutable), cwd).!
    if(exitCode != 0) {
      sys.error(s"Running python tests received non-zero exit code $exitCode")
    }
  }

  def buildPythonTask(baseDirectory: File, version: String): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(Seq(cwd.getAbsolutePath + "/build." + ext,
      version, pythonExecutable, "setup.py"), cwd).!
    if(exitCode != 0) {
      sys.error(s"Building python API received non-zero exit code $exitCode")
    }
  }

  def buildExamplesTask(baseDirectory: File, version: String): Unit = {
    val cwd = workingDirectory(baseDirectory)
    val exitCode = Process(Seq(cwd.getAbsolutePath + "/build." + ext, version, pythonExecutable,
      "setup-examples.py"), cwd).!
    if(exitCode != 0) {
      sys.error(s"Building python examples received non-zero exit code $exitCode")
    }
  }
}
