<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [LocalClusterSupervisor (context-per-jvm=false)](#localclustersupervisor-context-per-jvmfalse)
  - [Jar routes](#jar-routes)
  - [Context routes](#context-routes)
  - [Job routes](#job-routes)
- [AkkaClusterSupervisor (context-per-jvm=true)](#akkaclustersupervisor-context-per-jvmtrue)
  - [Context routes](#context-routes-1)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

(use http://websequencediagrams.com/ to visualize sequence diagrams)

LocalClusterSupervisor (context-per-jvm=false)
==========

Context routes
----------
- get a list of all known contextNames

        user->WebApi: GET /contexts
        WebApi->LocalContextSupervisor: ListContexts
        LocalContextSupervisor->WebApi: Seq(contextName)
        WebApi->user: 200 + JSON

- create a context with given contextName and configuration parameters.

        user->WebApi: POST /contexts/<contextName>?numCores=<nInt>&memPerNode=512m
        WebApi->LocalContextSupervisor: AddContext(contextName)
        opt if contexts contains contextName
          LocalContextSupervisor->WebApi: ContextAlreadyExists
          WebApi->user: 400
        end
        note over LocalContextSupervisor: CREATE JobManager(JobDao, contextName, sparkMaster, contextConfig(CPU, Mem))
        LocalContextSupervisor->JobManager: Initialize
        note over JobManager: CREATE RddManager(createContextFromConfig())
        note over JobManager: createContextFromConfig(sparkMaster, contextName)
        note over JobManager: CREATE SparkContext
        JobManager->LocalContextSupervisor: Initialized(JobResultActor)
        opt If JobManager times out
        LocalContextSupervisor->WebApi: ContextInitError
        WebApi->user: failWith(error)
        end
        LocalContextSupervisor->WebApi: ContextInitialized
        WebApi->user: 200

- delete a context with given contextName

        user->WebApi: DELETE /contexts/<contextName>
        WebApi->LocalContextSupervisor: StopContext(contextName)
        opt If no such context
          LocalContextSupervisor->WebApi: NoSuchContext
          WebApi->user: 404
        end
        LocalContextSupervisor->JobManager: PoisonPill
        LocalContextSupervisor->WebApi: ContextStopped
        WebApi->user: 200


Job routes
----------
- get a list of JobInfo(jobId, contextName, JarInfo, classPath, startTime, Option(endTime), Option(Throwable)) of all known jobs

        user->WebApi: GET /jobs
        WebApi->JobInfoActor: GetJobStatuses
        note over JobInfoActor: JobDao.getJobInfos...
        JobInfoActor->WebApi: Seq[JobInfo]
        WebApi->user: 200 + JSON

- get job result with jobId

        user->WebApi: GET /jobs/<jobId>
        WebApi->JobInfoActor: GetJobResult(jobId)
        note over JobInfoActor: JobDao.getJobInfos.get(jobId)
        opt if jobId not found:
          JobInfoActor->WebApi: NoSuchJobId
          WebApi->user: 404
        end
        opt if job is running or error out:
          JobInfoActor->WebApi: JobInfo
          WebApi->user: 200 + "RUNNING" | "ERROR"
        end
        JobInfoActor->LocalContextSupervisor:GetContext(contextName)
        opt if no such context:
          LocalContextSupervisor->JobInfoActor: NoSuchContext
          note over JobInfoActor: NOT HANDLED
        end
        LocalContextSupervisor->JobInfoActor: (JobManager, JobResultActor)
        JobInfoActor->JobResultActor: GetJobResult(jobId)
        opt if jobId not in cache:
            JobResultActor->JobInfoActor: NoSuchJobId
            JobInfoActor->WebApi: NoSuchJobId
            WebApi->user: 404
        end
        JobResultActor->JobInfoActor: JobResult(jobId, Any)
        JobInfoActor->WebApi: JobResult(jobId, Any)
        WebApi->user: 200 + resultToTable(result)

- submit a job

        user->WebApi: POST /jobs/<appName, classPath, contextName, sync> configString
        WebApi->LocalContextSupervisor: GetContext(contextName)
        opt if no such context:
          LocalContextSupervisor->WebApi: NoSuchContext
          WebApi->user: 404
        end
        LocalContextSupervisor->WebApi: (JobManager, JobResultActor)
        WebApi->JobManager: StartJob(appName, clasPatch, userConfig, asyncEvents | syncEvents)
        note over JobManager: JobDao.getLastUploadTime(appName)
        opt if no such appName:
          JobManager->WebApi: NoSuchApplication
          WebApi->user: 404
        end
        note over JobManager: CREATE unique jobID
        note over JobManager: JobCache.getSparkJob(appName, uploadTime, classPath)
        opt if no such jar or classPath
          JobManager->WebApi: NoSuchClass
          WebApi->user: 404
        end
        note over JobManager: JobJarInfo(SparkJob, jarFilePath, classLoader)
        JobManager->JobStatusActor: Subscribe(jobId, WebApi, asyncEvents | syncEvents)
        JobManager->JobResultActor: Subscribe(jobId, WebApi, asyncEvents | syncEvents)
        note over JobManager: getJobFuture(jobId, JobJarInfo, JobInfo, jobConfig, sender)
        opt if too many running jobs:
          JobManager->WebApi: NoJobSlotsAvailable
          WebApi->user: 503
        end
        JobManager->JobFuture: future{}
        note over JobFuture: set up classloader
        JobFuture->JobStatusActor: JobInit
        opt if jobId known already:
          JobStatusActor->JobFuture: JobInitAlready
          note over JobFuture: NOT HANDLED
        end
        opt if Job validation fails
          JobFuture->JobStatusActor: JobValidationFailed
          JobStatusActor->WebApi: JobValicationFailed
          WebApi->user: 400
        end
        JobFuture->JobStatusActor: JobStarted
        opt if async job
          JobStatusActor->WebApi: JobStarted
          WebApi->user: 202 + jobId
        end
        note over JobFuture: SparkJob.runJob
        opt if SparkJob fails:
          JobFuture->JobStatusActor: JobErroredOut
          JobStatusActor->WebApi: JobErroredOut
          WebApi->user: "ERROR"
        end
        JobFuture->JobStatusActor: JobFinished(jobId, now)
        JobFuture->JobResultActor: JobResult(jobId, result)
        note over JobResultActor: cacheResult(jobId, result)
        opt if sync job
          JobResultActor->WebApi: JobResult(jobId, result)
          WebApi->user: 200 + JSON
        end
        note over JobResultActor: subscribers.remove(jobId)
        JobFuture->JobStatusActor: Unsubscribe(jobId, WebApi)
        JobFuture->JobResultActor: Unsubscribe(jobId, WebApi)

- kill a job with jobId

        user->WebApi: DELETE /jobs/<jobId>
        WebApi->JobInfoActor: GetJobResult(jobId)
        note over JobInfoActor: JobDao.getJobInfos.get(jobId)
        opt if jobId not found:
          JobInfoActor->WebApi: NoSuchJobId
          WebApi->user: 404
        end
        opt if job is running:
          WebApi->JobManager: KillJob(jobId)
          JobManager->WebApi: future{}
          WebApi->user: 200 + "KILLED"
        end
        opt if job has error out:
           JobInfoActor->WebApi: JobInfo
           WebApi->user: 200 + "ERROR"
        end

AkkaClusterSupervisor (context-per-jvm=true)
==========

Context routes
----------

- Context create route

        title POST /contexts

        user->WebApi: POST /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: AddContext(contextName, config)
        AkkaClusterSupervisorActor->JobDAOActor: GetContextInfoByName
        JobDAOActor->AkkaClusterSupervisorActor: ContextResponse
        opt context already there
        AkkaClusterSupervisorActor->WebApi: ContextAlreadyExists
        WebApi->user: 400 "Context <contextName> exists"
        end
        AkkaClusterSupervisorActor->ManagerLauncher: launcher.start()
        opt Exception
        AkkaClusterSupervisorActor->JobDAOActor: SaveContextInfo(Error)
        AkkaClusterSupervisorActor->WebApi: ContextInitError
        WebApi->user:  500 "CONTEXT INIT ERROR"
        end
        ManagerLauncher->JobManagerActor:initialize
        AkkaClusterSupervisorActor->JobDAOActor: SaveContextInfo(Started)
        ClusterDaemon->AkkaClusterSupervisorActor: MemberUp
        AkkaClusterSupervisorActor->JobManagerActor: Identify
        JobManagerActor->AkkaClusterSupervisorActor: ActorIdentity
        AkkaClusterSupervisorActor->JobDAOActor: GetContextInfo
        JobDAOActor->AkkaClusterSupervisorActor: ContextResponse
        note over AkkaClusterSupervisorActor: Restart logic is contained here
        AkkaClusterSupervisorActor->JobManagerActor: Initialize
        JobManagerActor->SparkBackend: makeContext
        JobManagerActor->AkkaClusterSupervisorActor: Initialized
        opt InitError or other possible failures
        AkkaClusterSupervisorActor->JobManagerActor: PoisonPill
        AkkaClusterSupervisorActor->JobDAOActor: SaveContextInfo(Error)
        AkkaClusterSupervisorActor->WebApi: ContextInitError
        WebApi->user:  500 "CONTEXT INIT ERROR"
        end
        AkkaClusterSupervisorActor->JobDAOActor: SaveContextInfo(Running)
        AkkaClusterSupervisorActor->WebApi: ContextInitialized
        WebApi->user: 200 "Context initialized"

- Context delete route (Normal flow)

        title DELETE /contexts (Normal flow)

        user->WebApi: DELETE /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: StopContext(contextName)
        note right of AkkaClusterSupervisorActor:set context state=STOPPING
        AkkaClusterSupervisorActor->JobManagerActor: StopContextAndShutdown
        JobManagerActor->JobManagerActor: ContextStopScheduledMsgTimeout
        JobManagerActor->SparkContext: sc.stop()
        SparkContext -> JobManagerActor: onApplicationEnd
        JobManagerActor ->JobManagerActor: SparkContextStopped
        JobManagerActor -> JobStatusActor: watch
        JobManagerActor ->JobStatusActor: stop
        JobStatusActor ->JobStatusActor: postStop
        JobStatusActor -> JobDaoActor: SaveJobInfo
        DeathWatch ->JobManagerActor: Terminated(statusActor)
        JobManagerActor->JobManagerActor: ContextStopScheduledMsgTimeout.cancel()
        JobManagerActor ->AkkaClusterSupervisorActor: SparkContextStopped
        JobManagerActor ->JobManagerActor: PoisonPill
        AkkaClusterSupervisorActor ->WebApi: ContextStopped
        DeathWatch ->AkkaClusterSupervisorActor: Terminated
        note right of AkkaClusterSupervisorActor:set context state=FINISHED
        DeathWatch->ProductionReaper: Terminated
        ProductionReaper->ActorSystem: shutdown
        WebApi ->user: 200

- Context delete route (time out flow)

        title DELETE /contexts (stop context timed out)

        user->WebApi: DELETE /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: StopContext(contextName)
        note right of AkkaClusterSupervisorActor:set context state=STOPPING
        AkkaClusterSupervisorActor->JobManagerActor: StopContextAndShutdown
        JobManagerActor->Akka Scheduler: schedule(ContextStopScheduledMsgTimeout, timeout)
        JobManagerActor->SparkContext: sc.stop()

        space
        space
        space
        Akka Scheduler ->JobManagerActor: ContextStopScheduledMsgTimeout
        JobManagerActor ->AkkaClusterSupervisorActor: ContextStopInProgress
        AkkaClusterSupervisorActor ->WebApi: ContextStopInProgress
        WebApi ->user: 202 & Location Header

        space
        ==User request to url which is in location header to get the state of stop==

        user->WebApi: GET /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: GetSparkContexData(contextName)
        AkkaClusterSupervisorActor->JobManagerActor: GetContexData

        opt if context is running:
        JobManagerActor->SparkContext: applicationId/webUrl
        SparkContext ->JobManagerActor:
        JobManagerActor->AkkaClusterSupervisorActor: ContexData
        AkkaClusterSupervisorActor->WebApi: SparkContexData(ctxInfo, appId, webUrl)
        end

        opt if context is not alive:
        JobManagerActor->SparkContext: applicationId/webUrl
        JobManagerActor->JobManagerActor: Exception
        JobManagerActor->AkkaClusterSupervisorActor: SparkContextDead
        AkkaClusterSupervisorActor->WebApi:SparkContexData(ctxInfo, None, None)
        end

        WebApi->user: 200 & json with current state

        space

        space
        ==User can send more requests when context stop is in progress==
        space

        user->WebApi: DELETE /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: StopContext(contextName)
        AkkaClusterSupervisorActor->JobManagerActor: StopContextAndShutdown
        JobManagerActor ->AkkaClusterSupervisorActor: ContextStopInProgress
        AkkaClusterSupervisorActor ->WebApi: ContextStopInProgress
        WebApi ->user: 202 & Location Header

        space
        ==Finally when context will stop, the following flow will be followed==
        space

        SparkContext -> JobManagerActor: onApplicationEnd
        JobManagerActor ->JobManagerActor: SparkContextStopped
        JobManagerActor -> JobStatusActor: watch
        JobManagerActor ->JobStatusActor: stop
        JobStatusActor ->JobStatusActor: postStop
        JobStatusActor -> JobDaoActor: SaveJobInfo
        DeathWatch ->JobManagerActor: Terminated(statusActor)
        JobManagerActor ->JobManagerActor: PoisonPill
        DeathWatch ->AkkaClusterSupervisorActor: Terminated

        note right of AkkaClusterSupervisorActor:set context state=FINISHED
        DeathWatch->ProductionReaper: Terminated
        ProductionReaper->ActorSystem: shutdown

        space
        ==Further requests will fail==
        space

        user->WebApi: GET /contexts/<contextName>
        WebApi->AkkaClusterSupervisorActor: GetSparkContexData(contextName)
        AkkaClusterSupervisorActor->WebApi: NoSuchContext
        WebApi->user: 404


- Adhoc Context Stop

        title Adhoc contexts stop (Normal flow)

        user->WebApi: POST /job/<params>
        WebApi->AkkaClusterSupervisorActor: StartAdHocContext(classPath, contextConfig)
        AkkaClusterSupervisorActor->WebApi: ActorRef
        WebApi->JobManagerActor: StartJob(...)
        space
        note right of JobManagerActor:Normal flow of starting a job
        space
        space
        note right of JobManagerActor:Job finished
        JobManagerActor->JobStatusActor: JobFinished
        JobStatusActor->WebApi: JobResult
        WebApi->user: 200
        JobManagerActor->JobManagerActor: StopContextAndShutdown
        JobManagerActor->JobDAOActor: SaveContextInfo(..., STOPPING)
        JobManagerActor->SparkContext: sc.stop()
        SparkContext -> JobManagerActor: onApplicationEnd
        JobManagerActor ->JobManagerActor: SparkContextStopped
        JobManagerActor ->JobManagerActor: PoisonPill
        DeathWatch ->AkkaClusterSupervisorActor: Terminated
        note right of AkkaClusterSupervisorActor:set context state=FINISHED
        DeathWatch->ProductionReaper: Terminated
        ProductionReaper->ActorSystem: shutdown
