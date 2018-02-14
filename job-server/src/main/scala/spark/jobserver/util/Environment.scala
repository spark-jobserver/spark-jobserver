package spark.jobserver.util

import scala.collection.mutable.HashMap

trait Environment {
    def get(key: String, default: String): String
    def set(key: String, value: String)
    def clear()
}

class SystemEnvironment extends Environment {
    def get(key: String, default: String): String = {
      sys.env.get(key).getOrElse(default)
    }

    def set(key: String, value: String) {
      throw new UnsupportedOperationException
    }

    def clear() {
      throw new UnsupportedOperationException
    }
}