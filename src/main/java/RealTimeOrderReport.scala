import ApplicationConfig._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import ApplicationConfig._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.redislabs.provider.redis._
import redis.clients.jedis.Jedis

object RealTimeOrderReport {
  def reportAmtTotal(streamDataFrame: DataFrame): Unit = {
    import streamDataFrame.sparkSession.implicits._
    val resultStreamDF = streamDataFrame
      .agg(sum($"money").as("total_amt"))
      .withColumn("total", lit("global"))

    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.coalesce(1)
          .groupBy()
          .pivot($"total").sum("total_amt")
          .withColumn("type", lit("total"))
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("spark.redis.host", ApplicationConfig.REDIS_HOST)
          .option("spark.redis.port", ApplicationConfig.REDIS_PORT)
          .option("spark.redis.dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:money")
          .option("key.column", "type")
          .save()

        // Logging for debugging
        println("Batch written to Redis for total amount")
      }
      .start()
      .awaitTermination()
  }
  def reportAmtCity(streamDataFrame: DataFrame): Unit = {
    val session: SparkSession = streamDataFrame.sparkSession
    import session.implicits._
    val resultStreamDF: Dataset[Row] = streamDataFrame
      .filter($"city".isin("北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"))
    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        batchDF.coalesce(1)
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("spark.redis.host", ApplicationConfig.REDIS_HOST)
          .option("spark.redis.port", ApplicationConfig.REDIS_PORT)
          .option("spark.redis.dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:citymoney")
          .option("key.column", "city")
          .save()

        // Logging for debugging
        println("Batch written to Redis for selected city amounts")
      }
      .start()
      .awaitTermination()
  }

  def reportAmtProvince(streamDataFrame: DataFrame): Unit = {
    import streamDataFrame.sparkSession.implicits._
    val resultStreamDF = streamDataFrame
      .groupBy($"city")
      .agg(sum($"money").as("total_amt"))
      .withColumn("total", lit("global"))
    resultStreamDF.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.coalesce(1)
          .groupBy("total")
          .pivot($"city").sum("total_amt")
          .write
          .mode(SaveMode.Append)
          .format("org.apache.spark.sql.redis")
          .option("spark.redis.host", ApplicationConfig.REDIS_HOST)
          .option("spark.redis.port", ApplicationConfig.REDIS_PORT)
          .option("spark.redis.dbNum", ApplicationConfig.REDIS_DB)
          .option("table", "orders:citymoney")
          .option("key.column", "total")
          .save()

        println("Batch written to Redis for total amount")
      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("RealTimeOrderReport")
      .config("spark.master", "local[*]")
      .getOrCreate()
    import spark.implicits._
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    val streamDataFrame: DataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(record => null != record && record.trim.split(",").length > 0)
      .select(
        get_json_object($"value", "$.orderMoney").cast(DoubleType).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )
    import streamDataFrame.sparkSession.implicits._
    import spark.implicits._
    // 将处理后的数据写入控制台进行输出

    // 实时报表统计：总销售额、各省份销售额及重点城市销售额
//    reportAmtTotal(streamDataFrame)
//    reportAmtProvince(streamDataFrame)
    reportAmtCity(streamDataFrame)
  }
}
