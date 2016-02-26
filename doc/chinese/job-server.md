spark-jobserver
===

Spark-jobserver 提供了一个 RESTful 接口来提交和管理 spark 的 jobs、jars 和 job contexts。这个项目包含了完整的 Spark job server 的项目，包括单元测试和项目部署脚本。

## 特性

- “Spark as Service”：针对 job 和 contexts 的各个方面提供了 REST 风格的 api 接口进行管理
- 支持 SparkSQL、Hive、Streaming Contexts/jobs 以及定制 job contexts！具体参考[Contexts](doc/contexts.md)
- 通过集成 Apache Shiro 来支持 LDAP 权限验证
- 支持亚秒级别低延迟的任务通过长期运行的 job contexts
- 可以通过结束 context 来停止运行的作业(job)
- 分割 jar 上传步骤以提高 job 的启动
- 异步和同步的 job API，其中同步 API 对低延时作业非常有效
- 支持 Standalone Spark 和 Mesos、yarn
- Job 和 jar 信息通过一个可插拔的 DAO 接口来持久化
- 命名 RDD 以缓存，并可以通过该名称获取 RDD。这样可以提高作业间 RDD 的共享和重用
- 支持 Scala 2.10 版本和 2.11 版本

## 版本信息

请参考官方文档：[版本信息](https://github.com/spark-jobserver/spark-jobserver#version-information)

## 部署

1. 拷贝 `conf/local.sh.template` 文件到 `local.sh` 。备注：如果需要编译不同版本的Spark，请修改 `SPARK_VERSION` 属性。
2. 拷贝 `config/shiro.ini.template` 文件到 `shiro.ini`。备注: 仅需 `authentication = on`时执行这一步。
3. 拷贝 `config/local.conf.template` 到 `<environment>.conf`。
4. `bin/server_deploy.sh <environment>`，这一步将job-server以及配置文件打包，并一同推送到配置的远程服务器上。
5. 在远程服务器上部署的文件目录下通过执行 `server_start.sh` 启动服务，如需关闭服务可执行 `server_stop.sh`。
