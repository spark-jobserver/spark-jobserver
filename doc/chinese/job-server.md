spark-jobserver
    提供了一个RESTful接口来提交和管理spark的jobs,jars和job contexts。
    特性：
        1.针对job 和 contexts的各个方面提供了REST风格的api接口进行管理
        2.支持SparkSQL，Hive，Streaming Contexts/jobs 以及定制job contexts!
        3.支持压秒级别低延迟的任务通过长期运行的job contexts
        4.可以通过结束context来停止运行的作业(job)
        5.分割jar上传步骤以提高job的启动
        6.异步和同步的job API，其中同步API对低延时作业非常有效
        7.支持Standalone Spark和Mesos,yarn
        8.Job和jar信息通过一个可插拔的DAO接口来持久化
        9.命名RDD以缓存，并可以通过该名称获取RDD。这样可以提高作业间RDD的共享和重用
    安装:
        1.拷贝spark-jobserver 目录下的conf/local.sh.template文件为local.sh，cp conf/local.sh.template conf/local.sh.
        2.配置conf/local.sh文件，主要是以下三个参数SPARK_VERISON / SPARK_HOME/ DRIVER_MEMORY
        3.然后执行bin/server_deploy.sh,config/local开始部署配置环境，这个时候可能会提醒你找不到local.sh文件，你可以根据错误提示信息把local.sh文件拷贝到相应的路径下。
        4.在执行完server_deploy.sh文件后，可以执行server_start.sh文件进行启动jobserver。可能在启动的过程提示log4j找不到，这个时候需要把conf目录下的log4j文件拷贝到指定的位置。同时如果提示：spark-jobserver-master/bin/spark-job-server.jar does not exist，则个时候需要找一下当前路径下的spark-job-server.jar文件在哪个路径下，然后把该文件拷贝到bin目录下。
        5.启动没有报错的情况下，可以用ps -ef	grep job来查看一下当前是否存在jobserver的进程。
        6.正确启动后就可以通过浏览器访问该主机的8090端口，例如：192.168.1.100:8090
        7.可以在该主机上执行：curl –data-binary @job-server-tests/target/job-server-tests-$VER.jar localhost:8090/jars/test，把测试jar包放到jobserver上。然后可以在Spark上执行任务：curl -d “input.string = a b c a b see” ‘localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample’，执行完后可以查看执行的结果：curl localhost:8090/jobs/5453779a-f004-45fc-a11d-a39dae0f9bf4jobserver的安装就完成。具体如何让编写的spark应用程序可以被jobserver调用，这需要创建一个jobserver的工程。
