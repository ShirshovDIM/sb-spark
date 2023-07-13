package data_mart

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.postgresql.Driver
import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col, sum, explode, lit}

import java.net.{URL, URLDecoder}
import scala.util.Try







object data_assembler {

  private def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {

    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost.replace("www.", "")

    }
      .getOrElse("")
  })

  private def getAgeGroup: UserDefinedFunction = udf((age: Integer) => {

    age match {
      case a if a >= 18 && a <= 24 => "18-24"
      case a if a >= 25 && a <= 34 => "25-34"
      case a if a >= 35 && a <= 44 => "35-44"
      case a if a >= 45 && a <= 54 => "45-54"
      case a if a >= 55 => ">=55"
    }
  })

  private def webUdf: UserDefinedFunction = udf((web: String) => {

    "web_" + web

  })
  private def catUdf: UserDefinedFunction = udf((cat: String) => {

    "shop_" + cat.toLowerCase.split("-").mkString("_")

  })

  private def createTableWithGrant(shop_cols: Array[Any],
                           web_cols: Array[Any]): Unit = {

    val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
    val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
    val url = "jdbc:postgresql://10.0.0.31:5432/dmitriy_shirshov?user=dmitriy_shirshov&password=qR4cxBAf"
    val connection: Connection = DriverManager.getConnection(url)
    val statement: Statement = connection.createStatement()

    val query: String = ("create table if not exists clients (uid varchar(40), gender varchar(1), age_cat varchar(5), "
      + shop_cols.mkString(s" int, ") + " int, "
      + web_cols.mkString(s" int, ") + " int);")
    val bool_0: Boolean = statement.execute(query)

    val bool_1: Boolean = statement.execute("grant select on clients to labchecker2;")
    connection.close()
  }

  def data_assembler(): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .appName("Shirshov scala k test")
      .getOrCreate()

    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    val logs: DataFrame = spark.read
      .json("hdfs:///labs/laba03/weblogs.json")

    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")

    val shop_cols: Array[Any] = {
      visits
        .select("category")
        .distinct
        .withColumn("shop_item",
          catUdf(col("category")))
        .sort("shop_item")
        .rdd
        .map(v => v{1})
        .collect()
    }

    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "dmitriy_shirshov")
      .option("password", "qR4cxBAf")
      .option("driver", "org.postgresql.Driver")
      .load()

    val web_cols: Array[Any] = {
      cats
        .withColumn("web_category", webUdf(col("category")))
        .select("web_category")
        .distinct
        .sort("web_category")
        .rdd
        .map(v => v{0})
        .collect()
    }

    createTableWithGrant(shop_cols, web_cols)

    val logsFiltered: DataFrame = logs
      .withColumn("info", explode(col("visits")))
      .select("uid", "info")
      .select("uid", "info.*")
      .withColumn("target_domain", decodeUrlAndGetDomain(col("url")))
      .drop("timestamp", "url")
      .filter("target_domain != ''")

    val clientsGrouped: DataFrame = clients
      .withColumn("age_cat", getAgeGroup(col("age")))
      .drop("age")

    val logsCategory: DataFrame = logsFiltered
      .join(cats, col("domain") === col("target_domain"))
      .select("uid", "category")
      .withColumn("cat", webUdf(col("category")))
      .drop("category")

    val visitsCategory: DataFrame = visits
      .filter("uid is not null")
      .withColumn("cat", catUdf(col("category")))
      .select("uid", "cat")

    val catsCategory: DataFrame = visitsCategory
      .union(logsCategory)

    val writeStage: DataFrame = clientsGrouped
      .join(catsCategory, "uid")
      .withColumn("counter", lit(1))
      .groupBy("uid", "gender", "age_cat")
      .pivot("cat")
      .agg(sum(col("counter")))
      .drop("null")

    writeStage.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/dmitriy_shirshov")
      .option("dbtable", "clients")
      .option("user", "dmitriy_shirshov")
      .option("password", "qR4cxBAf")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()

    spark.stop

  }
}

data_assembler.data_assembler()