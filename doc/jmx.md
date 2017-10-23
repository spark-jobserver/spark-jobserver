#### JMX
To read JMX metrics, you can use [jconsole](http://docs.oracle.com/javase/7/docs/technotes/guides/management/jconsole.html) if you have an option of GUI. Otherwise you can use a cli utility named `jmxcli`. Here are the steps

- `wget https://github.com/downloads/vladimirvivien/jmx-cli/jmxcli-0.1.2-bin.zip`
- `unzip jmxcli-0.1.2-bin.zip -d <folder>`
- `cd <folder>`
- execute `java -jar cli.jar`
- Do `ps` to list all the JVMs
- Connect with JVM using `connect pid:<pid_id>`
- Use `list` to see all the possible mbeans
- To get the current value of a metric use for example
```exec bean:"\"spark.jobserver\":name=\"job-cache-size\",type=\"JobCacheImpl\"" get:Value```
