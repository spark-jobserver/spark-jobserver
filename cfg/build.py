# build scripts must define a class 'build'
import os
from os.path import join
from spi import BuildPlugin
import log
import subprocess
from xmake_exceptions import XmakeException
import shutil
import sys

class build(BuildPlugin):
    def __init__(self, build_cfg):
        self.build_cfg = build_cfg

        # -Dsbt.repository.config : used to specify the internal nexus repositories.
        # This configuration overrides the repositories defined by SJS.
        # These repositories are only used if -Dsbt.override.build.repos is set
        # to true.
        # -no-share: forces sbt to store all the dependencies inside the
        # project/.ivy folder.
        self._sbtCommonConfig = '-Dsbt.repository.config=proxy_repositories -Dsbt.override.build.repos=true  \'-Dhttp.nonProxyHosts=localhost|127.0.0.1|*.wdf.sap.corp|*.mo.sap.corp|*.sap.corp\' \'-Dhttps.nonProxyHosts=localhost|127.0.0.1|*.wdf.sap.corp|*.mo.sap.corp|*.sap.corp\''

    # Called when the actual build step is executed.
    def run(self):
        log.info("TRACE", "entering", "run")

        log.info(">>>>>> Component dir is " + self.build_cfg.component_dir())
        self.remove_old_sbt_artifacts(self.build_cfg.component_dir())

        self.importSbt()
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        commandOutput = self.buildSJS()
        if commandOutput > 0:
            raise XmakeException("sbt build failed. Command output is {}".format(str(commandOutput)))

    def importSbt(self):
        log.info("TRACE", "entering", "sbt")
        # Get SBT Path
        self._sbthome = os.path.join(self.build_cfg.tools()['SBT']['1.2.8'], "sbt")
        log.info(self._sbthome)

        # Set SBT_HOME
        os.environ["SBT_HOME"] = self._sbthome
        log.info("SBT_HOME", os.environ["SBT_HOME"])

        # Set SBT to path
        self._sbtbin=join(self._sbthome,'bin')
        os.environ["PATH"] = self._sbtbin + os.pathsep + os.environ["PATH"]
        log.info("PATH", os.environ["PATH"])

        # Output SBT version
        self._sbtexecutable = join(self._sbtbin,'sbt')
        log.info("TRACE", "exiting", "importSbt")

        log.info(">>>> Listing org.scala-sbt folder", os.listdir("/data/xtr/MS/org.scala-sbt/"))
        log.info(">>>> Listing sbt folder", os.listdir("/data/xtr/MS/org.scala-sbt/sbt/"))
        log.info(">>>> Listing 1.2.8 folder", os.listdir("/data/xtr/MS/org.scala-sbt/sbt/1.2.8/"))
        log.info(">>>> Listing sbt-1.2.8 folder", os.listdir("/data/xtr/MS/org.scala-sbt/sbt/1.2.8/sbt"))
        log.info(">>>> import dir ", self.build_cfg.import_dir())
        log.info(">>>> src dir ", self.build_cfg.src_dir())
        log.info(">>>> runtime dir ", self.build_cfg.runtime())
        log.info(">>>> component dir ", self.build_cfg.component_dir())
        log.info(">>>> component dir ", os.listdir(join(self.build_cfg.component_dir(), "project")))

        if os.path.isdir("~/.sbt/"):
            log.info(">>>> sbt ", os.listdir("~/.sbt/"))

        if os.path.isdir("~/.ivy2/"):
            log.info(">>>> ivy2 ", os.listdir("~/.ivy2/"))

        if os.path.isdir("~/.ivy/"):
            log.info(">>>> ivy ", os.listdir("~/.ivy/"))

    def buildSJS(self):
        log.info("TRACE", "entering", "buildSJS")
        log.info("INFO: building SJS")
        if os.environ['V_TEMPLATE_TYPE'] == "OD-downstream":
            return self.executeSbtCommand('clean job-server-extras/assembly')
        else:
            return self.executeSbtCommand('clean test job-server-extras/assembly')

    def executeSbtCommand(self, sbtCommand):
        log.info("TRACE", "entering", "executeSbtCommand")
        sbt_args = [self._sbtexecutable, self._sbtCommonConfig]
        sbt_args.append(sbtCommand)
        command = ' '.join(sbt_args)
        log.info("INFO: executing command", command)

        result = os.system(command)
        log.info("INFO: result of sbt command is", result)

        log.info("TRACE", "exiting", "executeSbtCommand")
        return result

    def remove_old_sbt_artifacts(self, project_dir):
        self.remove_directory(join(project_dir, "project/target/"))
        self.remove_directory(join(project_dir, "project/.sbtboot/"))
        self.remove_directory(join(project_dir, "project/.boot/"))
        self.remove_directory(join(project_dir, "project/.ivy/"))
        self.remove_directory(join(project_dir, "project/.ivy2/"))
        self.remove_directory(join(project_dir, "project/.sbt/boot/"))
        self.remove_directory(join(project_dir, "project/project/"))
        self.remove_directory('~/.sbt/')
        self.remove_directory('~/.ivy2/')
        self.remove_directory('~/.ivy/')


    def remove_directory(self, path):
        log.info(">>> Checking path for deleting " + path)
        if os.path.isdir(path):
            log.info(">>> DELETING directory " + path)
            shutil.rmtree(path)
