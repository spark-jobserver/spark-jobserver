/**
 * (c) Copyright [2015] Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.jobserver.util

import com.datastax.driver.core.{ResultSet, Session}
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config

import scala.util.{Success, Failure, Try}

/**
 * The spark master node that has been elected can be determined by
 * cql: select address from dse_system.real_leaders;
 *
 * Created by ffarozan on 21/7/15.
 */
class DseSparkMasterProvider extends SparkMasterProvider {

  val logger = LoggerFactory.getLogger(getClass)

  val sparkContextSettings = "spark.context-settings"
  val cassandraConnectorConf = "(spark.cassandra.*)".r
  val cassandraHostsParam = sparkContextSettings + ".spark.cassandra.connection.host"

  val sparkMasterIPQuery = "SELECT datacenter, address FROM dse_system.real_leaders"

  override def getSparkMaster(config: Config): String = {

    def getSparkNodesIPs(): List[String] = {
      config.getString(cassandraHostsParam).split(",").map(_.trim).toList
    }

    val sparkNodesIPs: List[String] = getSparkNodesIPs()

    val sparkConf = getSparkConfForCassandraConnector(config)

    def getSparkMasterIP(): Option[String] = {
      CassandraConnector(sparkConf).withSessionDo {
        session => {
          getSparkDCName(session) match {
            case Some(datacenter) => {
              /**
               * Logic is to get all rows from real_leaders table and filter for that DC.
               * Instead if we need to use where clause, we need army name as input.
               * The intent is to avoid army name in configuration &
               * to have as less configuration as possible.
               */
              val queryResult = Try(session.execute(sparkMasterIPQuery))
              val result = queryResult match {
                case Success(s) => {
                  parseRealLeadersTableRows(datacenter, s) match {
                    case Some(ip) => {
                      logger.info("Spark master IP -- %s".format(ip))
                      Some(ip)
                    }
                    case None => {
                      logger.error("No matching DC name in real leaders table")
                      None
                    }
                  }
                }
                case Failure(e) => {
                  // return empty string
                  logger.error("Querying spark master ip failed", e)
                  None
                }
              }
              result
            }
            case None => {
              logger.error("No matching host found in Cluster")
              None
            }
          }
        }
      }
    }

    def getSparkDCName(session: Session): Option[String] = {
      val metadata = session.getCluster.getMetadata
      val hosts = metadata.getAllHosts().asScala
      hosts.filter(
        host => sparkNodesIPs.contains(host.getAddress.getHostAddress)
      ).map(
        _.getDatacenter
      ).headOption
    }

    def parseRealLeadersTableRows(datacenter: String, rs: ResultSet): Option[String] = {
      rs.all().asScala.filter(row =>
        datacenter == row.getString("datacenter")
      ).map(
        _.getInet("address").getHostAddress
      ).headOption
    }

    def fallbackToDefaultMaster(config: Config): String = {
      // Fallback - Go with the default spark master url
      logger.error("Going with fallback option")
      config.getString("spark.master")
    }

    getSparkMasterIP() match {
      case Some(ip) => {
        val sparkMasterUrl = """spark://%s:7077""".format(ip)
        logger.info("Spark Master URL -- %s".format(sparkMasterUrl))
        sparkMasterUrl
      }
      case None => fallbackToDefaultMaster(config)
    }
  }

  /**
   * Get the spark conf with spark.cassandra properties configured
   * This will be used to connecting natively to Cassandra
   * @param config
   * @return
   */
  def getSparkConfForCassandraConnector(config: Config): SparkConf = {
    val sparkCtxSettings = config.getConfig(sparkContextSettings)
    val sparkConf = new SparkConf()
    sparkCtxSettings.entrySet().asScala.foreach { eachConfig =>
      val key = eachConfig.getKey
      key match {
        case cassandraConnectorConf(key) => sparkConf.set(key, sparkCtxSettings.getString(key))
        case _ => ;
      }
    }
    sparkConf
  }

}
