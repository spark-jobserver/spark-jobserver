title POST /jobs start new job workflow with AdHocJobManagerActor

user->WebApi: POST /jobs

WebApi->LocalContextSupervisor: GetAdHocContext
LocalContextSupervisor->WebApi: (AdHocJobManager)

WebApi->AdHocJobManagerActor: StartJob(event for JobStatusActor)

note over AdHocJobManagerActor: validate appName, className

opt if Job validation fails
  AdHocJobManagerActor->WebApi: ERROR
  WebApi->user: 400
end

AdHocJobManagerActor->JobStatusActor: Subscribe(jobId, WebApi, event)
AdHocJobManagerActor->JobFuture: CreateJob

JobFuture->JobStatusActor: JobInit(info)

note over JobFuture: SparkJob.validate()

opt if validation fails
  JobFuture->JobStatusActor: ValidationFailed
  JobStatusActor->WebApi: ValidationFailed
  WebApi->user: 400
end

JobFuture->JobStatusActor: JobStarted(jobId)

opt if async job
  JobStatusActor->WebApi: JobStarted(jobId)
  WebApi->user: 202
end

note over JobFuture: SparkJob.runJob()

JobFuture->JobStatusActor: JobFinish(jobId)
JobFuture->JobStatusActor: Unsubscribe(jobId, WebApi)

JobFuture->JobDAOActor: SaveJobResult(jobId, result)

JobFuture->AdHocJobManagerActor: JobFinish(jobId)

note over JobFuture: Terminate

opt if sync job
  JobStatusActor->WebApi: JobFinished(jobId, endTime)
  WebApi->JobDAOActor: GetJobResult(jobId)
  JobDAOActor->WebApi: JobResult(jobId)
  WebApi->user: result
end
