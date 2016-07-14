class JobEnvironment:
    def __init__(self, job_id, named_objects, context_config):
        self.jobId = job_id
        self.namedObjects = named_objects
        self.contextConfig = context_config

class ValidationProblem:
    def __init__(self, problem):
        self.problem = problem

def build_problems(problems):
    """
    A helper method for converting a list of string problems into instances
    of the validation problem class. It is important to return a list of the correct
    type since otherwise it cannot be differentiated from a list of job data.
    :param problems: a list of strings describing the problems
    :return: a list of ValidationProblem objects, one for each string in the input
    """
    return [ValidationProblem(p) for p in problems]

class SparkJob:

    def __init__(self):
        pass

    def validate(self, context, runtime, config):
        """
        This method is called by the job server to allow jobs to validate their input and reject
        invalid job requests.
        :param context: the context to be used for the job.
        Could be a sparkcontext, sqlcontext, hivecontext etc. May be reused across jobs
        :param runtime: the JobEnvironment containing run time information pertaining to the job and context.
        :param config: the HOCON config object passed into the job request
        :return: either JobData, which is parsed from config, or a list of validation problems.
        """
        raise NotImplementedError("Concrete implementations should override validate")

    def run_job(self, context, runtime, data):
        """
        Entry point for the execution of a job
        :param context: the context to be used for the job.
        Could be a sparkcontext, sqlcontext, hivecontext etc. May be reused across jobs
        :param runtime: the JobEnvironment containing run time information pertaining to the job and context.
        :param data: the JobData returned by the validate method
        :return: the job result
        """
        raise NotImplementedError("Concrete implementations should override run_job")

    def build_context(self, gateway, jvmContext, sparkConf):
        """
        For custom context types, the Python job needs to implement this method to provide a method
        for converting the jvm context into its Python equivalent. For jobs designed to work with
        JavaSparkContext, SQLContext and HiveContext it is not necessary to implement this method
        since the subprocess can handle those out of the box.
        :param gateway: The Py4J gateway object
        :param jvmContext: the JVM context object to be converted (usually wrapped) into a Python context object.
        :param sparkConf: The python form of the SparkConf object
        :return: Should return a python context object of the appropriate type. Will return None if not overridden
        """
        return None

#NamedObjects not currently supported in Python, but below is a skeleton for a possible interface.
class NamedObject:
    def __init__(self, obj, forceComputation, storage_level):
        self.obj = obj
        self.forceComputation = forceComputation
        self.storage_level = storage_level

class NamedObjects:
    def __init__(self):
        pass

    def get(self, name):
        """
        Gets an named object (NObj) with the given name if it already exists and is cached.
        If the NObj does not exist, None is returned.

        Note that a previously-known name object could 'disappear' if it hasn't been used for a while, because
        for example, the SparkContext garbage-collects old cached RDDs.

        :param name: the unique name of the NObj. The uniqueness is scoped to the current SparkContext.
        :return: the NamedObject with the given name.
        """
        raise NotImplementedError("Concrete implementations should override get")

    def get_or_else_create(self, name, obj_gen):
        """
        Gets a named object (NObj) with the given name, or creates it if one doesn't already exist.

        If the given NObj has already been computed by another job and cached in memory, this method will return
        a reference to the cached NObj. If the NObj has never been computed, then the generator will be called
        to compute it, and the result will be cached and returned to the caller.

        :param name: the unique name of the NObj. The uniqueness is scoped to the current SparkContext.
        :param obj_gen: a 0-ary function which will generate the NObj if it doesn't already exist.
        :return: the NamedObject with the given name.
        """
        raise NotImplementedError("Concrete implementations should override get_or_else_create")

    def update(self, name, obj_gen):
        """
        Replaces an existing named object (NObj) with a given name with a new object.
        If an old named object for the given name existed,
        it is un-persisted (non-blocking) and destroyed.

        :param name: The unique name of the object.
        :param obj_gen: a 0-ary function which will be called to generate the object.
        :return: the NamedObject with the given name.
        """
        raise NotImplementedError("Concrete implementations should override update")

    def forget(self, name):
        """
        Removes the named object with the given name, if one existed, from the cache
        Has no effect if no named object with this name exists.

        The persister is not (!) asked to unpersist the object, use destroy instead if that is desired

        :param name: the unique name of the object. The uniqueness is scoped to the current SparkContext.
        :return: nothing
        """
        raise NotImplementedError("Concrete implementations should override forget")

    def destroy(self, name):
        """
        Destroys the named object with the given name, if one existed. The reference to the object
        is removed from the cache and the persister is asked asynchronously to unpersist the
        object iff it was found in the list of named objects.
        Has no effect if no named object with this name is known to the cache.

        :param name: the unique name of the object. The uniqueness is scoped to the current SparkContext.
        :return: nothing
        """
        raise NotImplementedError("Concrete implementations should override destroy")

    def get_names(self):
        """
        Returns the names of all named object that are managed by the named objects implementation.

        Note: this returns a snapshot of object names at one point in time. The caller should always expect
        that the data returned from this method may be stale and incorrect.
        :return: a list of string names representing objects managed by the NamedObjects implementation.
        """
        raise NotImplementedError("Concrete implementations should override get_names")
