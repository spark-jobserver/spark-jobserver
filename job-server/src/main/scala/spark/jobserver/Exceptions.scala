package spark.jobserver

class ContextNotReadyException extends Exception("context not yet ready, try again soon")
class ContextNotFoundException extends Exception("context does not exist")
