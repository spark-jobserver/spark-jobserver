#build scripts must define a class 'build'
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

        self._sbtCommonConfig = '-Dsbt.repository.config=proxy_repositories -Dsbt.override.build.repos=true'

    # Called when the actual build step is executed.
    def run(self):
        log.info("TRACE", "entering", "run")
        self.importSbt()
        return self.buildSJS()

    def importSbt(self):
        # Get SBT tool path
        log.info("TRACE", "entering", "sbt")
        self._sbthome = os.path.join(self.build_cfg.tools()['SBT']['0.13.12'], "sbt")
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
        #os.system(' '.join([self._sbtexecutable, self._sbtCommonConfig, 'sbtVersion']))
        log.info("TRACE", "exiting", "importSbt")

    def buildSJS(self):
        log.info("TRACE", "entering", "buildSJS")
        log.info("INFO: building SJS")

        sbt_args = [self._sbtexecutable, self._sbtCommonConfig, 'job-server/assembly', '--debug']
        command = ' '.join(sbt_args)
        log.info("INFO: executing command", command)

        result = os.system(command)
        log.info("INFO: result of SJS build", result)

        log.info("TRACE", "exiting", "buildSJS")
        return result
