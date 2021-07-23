For full job server flow, see job-server-flow.md.

user->WebApi: POST /jobs/<appName, classPath, contextName, sync> configString
WebApi->LocalContextSupervisor: GetContext(contextName)
LocalContextSupervisor->WebApi: (JobManager)
WebApi->JobManager: StartJob(appName, clasPatch, userConfig, asyncEvents | syncEvents)
JobManager->JobStatusActor: Subscribe(jobId, WebApi, asyncEvents | syncEvents)
JobManager->JobFuture: future{}
JobFuture->JobStatusActor: JobInit
JobFuture->JobStatusActor: JobStarted
opt if async job
  JobStatusActor->WebApi: JobStarted
  WebApi->user: 202 + jobId
end
note over JobFuture: SparkJob.runJob
JobFuture->JobStatusActor: JobFinished(jobId, now)
JobFuture->JobDAOActor: SaveJobResult(jobId, result)
opt if sync job
  JobStatusActor->WebApi: JobFinished(jobId, endTime)
  WebApi->JobDAOActor: GetJobResult(jobId)
  JobDAOActor->WebApi: JobResult(jobId)
  WebApi->user: result
end
JobFuture->JobStatusActor: Unsubscribe(jobId, WebApi)
