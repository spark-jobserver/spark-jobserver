#build scripts must define a class 'build'
import os
import log
from xmake_exceptions import XmakeException
import spi

class build(spi.JavaBuildPlugin):
    def __init__(self, build_cfg):
        spi.JavaBuildPlugin.__init__(self, build_cfg)

        self.build_cfg = build_cfg

        # -Dsbt.repository.config : used to specify the internal nexus repositories.
        # This configuration overrides the repositories defined by SJS.
        # These repositories are only used if -Dsbt.override.build.repos is set
        # to true.
        self._sbt_launcher_config = ['-Dsbt.repository.config=proxy_repositories', \
                                     '-Dsbt.override.build.repos=true', \
                                     '-Dsbt.global.base=project/.sbtboot', \
                                     '-Dsbt.boot.directory=project/.boot', \
                                     '-Dsbt.ivy.home=project/.ivy', \
                                     '-Dhttp.nonProxyHosts=localhost|127.0.0.1|*.wdf.sap.corp|*.mo.sap.corp|*.sap.corp', \
                                     '-Dhttps.nonProxyHosts=localhost|127.0.0.1|*.wdf.sap.corp|*.mo.sap.corp|*.sap.corp']

        self._sbt_group_artifact = 'com.typesafe.sbt:sbt-launcher'
        self._sbt_version = '0.13.6'
        self._sbt_launcher_jar_name = 'sbt-launcher-0.13.6.jar'

    def plugin_imports(self):
        ''' This is called by the JavaPlugin to determine further dependencies.
            Here, we tell it to use the sbt-lanucher
        '''
        result = {
            'default': [
                '{ga}:jar:{v}'.format(ga=self._sbt_group_artifact, v=self._sbt_version)
            ]
        }

        return result

    def java_run(self):
        log.info("TRACE", "entering", "java_run")
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

        launcher_dir = self.get_sbt_launcher_dir()
        command_output = self.build_sjs(launcher_dir)

        if command_output > 0:
            raise XmakeException("sbt build failed. Command output is {}".format(str(command_output)))

    def get_sbt_launcher_dir(self):
        log.info("Import directory is:", self.build_cfg.import_dir())
        log.info("Contents of import directory are: ", os.listdir(self.build_cfg.import_dir()))

        sbt_launcher_dir = os.path.join(self.build_cfg.import_dir(), self._sbt_launcher_jar_name)
        log.info("Sbt Launcher directory full path is: " + sbt_launcher_dir)
        return sbt_launcher_dir

    def build_sjs(self, launcher_dir):
        log.info("TRACE", "entering", "build_sjs")
        if os.environ['V_TEMPLATE_TYPE'] == "OD-downstream":
            return self.execute_sbt_command(launcher_dir, ['clean', 'job-server-extras/assembly'])
        else:
            return self.execute_sbt_command(launcher_dir, ['clean', 'test', 'job-server-extras/assembly'])

    def execute_sbt_command(self, launcher_dir, sbt_command):
        log.info("TRACE", "entering", "execute_sbt_command")

        sbt_launcher_args = self._sbt_launcher_config + ['-jar', launcher_dir] + sbt_command
        launcher_command = ' '.join(sbt_launcher_args)

        log.info("Launch command is: " + launcher_command)

        rc = self.java_log_execute(sbt_launcher_args, handler=None)
        return rc
