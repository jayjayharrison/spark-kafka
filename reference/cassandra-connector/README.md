https://hadoopsters.com/2019/06/27/how-to-load-data-from-cassandra-into-hadoop-using-spark/

"com.datastax.spark" %% "spark-cassandra-connector" % "2.0.9"


```
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

object CassandraLoader extends Serializable {

  /** Representative of the some_keyspace.some_table table. */
  case class MyCassandraTable(user_id: String, `type`: Int, key: String, value: String)

  def main(args: Array[String]) { 

    val spark = SparkSession
      .builder()
      .config("hive.merge.orcfile.stripe.level", "false")
      .appName("Cassandra Data Loader")
      .enableHiveSupport()
      .getOrCreate()

    // Implicits Allow us to Use .as[CaseClass]
    import spark.implicits._

    val user = Map("spark.cassandra.auth.username" -> "some_username")
    val pwd = Map("spark.cassandra.auth.password" -> "some_password")

    val hosts = "cassandra001.yourcompany.com,cassandra002.yourcompany.com,cassandra003.yourcompany.com"
    val port = "9042"


    spark.setCassandraConf(
      CassandraConnectorConf.KeepAliveMillisParam.option(1000) ++
        CassandraConnectorConf.ConnectionHostParam.option(hosts) ++
        CassandraConnectorConf.ReadTimeoutParam.option(240000) ++
        CassandraConnectorConf.ConnectionPortParam.option(port) ++
        user ++ pwd)


    val table = Map("keyspace" -> "some_keyspace", "table" -> "some_table_in_that_keyspace")


    val data = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(table)
      .load()
      .as[MyCassandraTable]

    // write to hdfs
    data
      .write
      .option("orc.compress", "snappy")
      .mode(SaveMode.Overwrite)
      .orc("/some/location/in/hdfs/")
  }
}


```
