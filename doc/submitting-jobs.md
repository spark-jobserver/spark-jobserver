<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Submitting new jobs](#submitting-new-jobs)
  - [Uploading binaries for the job](#uploading-binaries-for-the-job)
  - [Posting jobs](#posting-jobs)
  - [Callback handling](#callbackUrl)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Submitting new jobs

## Uploading binaries for the job

Jar and Egg files can be uploaded to jobserver using `POST /binaries/<binary_name>` requests.

Sample POST request for jar file:
```
curl -X POST localhost:8090/binaries/test -H "Content-Type: application/java-archive" --data-binary @path/to/your/binary.jar
OK
```

To upload an egg file `Content-Type` should be changed to `application/python-archive`.
So far jobserver supports only `application/python-archive` and `application/java-archive` content types for uploaded binaries.


## Posting jobs

Uploaded binaries can be used to start the job (note, that recent version of jobserver allows to start jobs just from file uri, though only limited number of protocols are supported).

```
POST /jobs

Parameters:
|      name      |   type  |                              description                                     |
|----------------|---------|------------------------------------------------------------------------------|
|    appName     | String  | the name under which the application binary was uploaded to jobserver        |
|   classPath    | String  | the fully qualified class path for the job                                   |
|  callbackUrl   |   Url   | an optional callback url that is invoked once the job has finished           |
|     sync       | Boolean | if "true", then wait for and return results, otherwise return job            |
|    timeout     |   Int   | the number of seconds to wait for sync results to come back                  |

Also user impersonation for an already Kerberos authenticated user is supported via the `spark.proxy.user` query parameter.

Response:
{
    StatusKey -> "OK" | "ERROR",
    ResultKey -> "either the job id, or a result"
}
```

Sample `POST` request:
```
    curl -d "input.string = a b c a b see" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample"
    {
      "duration": "Job not done yet",
      "classPath": "spark.jobserver.WordCountExample",
      "startTime": "2016-06-19T16:27:12.196+05:30",
      "context": "b7ea0eb5-spark.jobserver.WordCountExample",
      "status": "STARTED",
      "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4"
    }
```


In Jobserver **version > 0.9.0** an alternative to `classPath` and `appName` is introduced:

```
|      name      |   type  |                                              description                                                        |
|----------------|---------|-----------------------------------------------------------------------------------------------------------------|
|       cp       | String  | list of binary names and uris (http, ftp, local and file protocols are supported) required for the application  |
|   mainClass    | String  | the fully qualified class path for the job                                                                      |
```
`POST /jobs/appName=..&classPath..` still can be used as in previous versions of Jobserver, but the new combination of parameters allows the submission
of multiple jars without the need of packing everything into a single main application jar.
 
From now on a job can be also submitted without uploading binary to Jobserver, just specifying URI with supported protocol is enough. Please note,
that for Python jobs **only one egg file can be used**.

Sample `POST` request using new parameters:
```
    curl -d "input.string = a b c a b see" "localhost:8090/jobs?cp=binName,http://path/to/jar&mainClass=some.main.Class"
    {
      "duration": "Job not done yet",
      "classPath": "some.main.Class",
      "startTime": "2019-10-21T10:32:08.567+08:39",
      "context": "someContextName",
      "status": "STARTED",
      "jobId": "898742a-a005-56hc-1245-b39sfd3dfdgs"
    }
```

## CallbackUrl
If a `callbackUrl` is provided for a (async/sync) job, the result of the job is uploaded to the url via an HTTP Post
request. The uploaded result is similar to the output produced by a synchronous job, namely
```
{
  jobId -> "the unique job id",
  ResultKey -> "either the result, or an exception"
}
```

Sample `POST` request using a callback url:
```
    curl -d "input.string = a b c a b see" "localhost:8090/jobs?appName=test&classPath=spark.jobserver.WordCountExample&callbackUrl=http%3A%2F%2Flocalhost%3A6666"
    {
      "duration": "Job not done yet",
      "classPath": "spark.jobserver.WordCountExample",
      "startTime": "2016-06-19T16:27:12.196+05:30",
      "context": "b7ea0eb5-spark.jobserver.WordCountExample",
      "status": "STARTED",
      "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4"
    }
```
Sample output received at the callback url:
```
{
  "jobId": "5453779a-f004-45fc-a11d-a39dae0f9bf4",
  "result": { "a": 2, "b": 2, "see":1, "c": 1 }
}
```

In case of a streaming result, the actual result is substituted by `<Stream result>` and the actual result has to be
manually fetched via `GET /jobs/<jobId>`. If the job is manually cancelled via `DELETE /jobs/<jobId>` or
`DELETE /contexts/<contextName>` the callback is *not* invoked. The callback handler uses a simple retry mechanism with
a few seconds delay to ensuring that the callback is completed successfully. However, if all retries are exhausted,
the callback will be abandoned and never be invoked.