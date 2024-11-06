import ApplicationConfig._
//import SparkUtils._ ,StreamingUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
/**
 *实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，结果实时存储Redis数据库，维度如下：
 *- 第一、总销售额：sum
 *- 第二、各省份销售额：province
 *- 第三、重点城市销售额：city
 */
object RealTimeOrderReport {
  /** 实时统计：总销售额，使用sum函数 */
  def reportAmtTotal(streamDataFrame: DataFrame): Unit = {
    //导入隐式转换
    import streamDataFrame.sparkSession.implicits._
    //业务计算
    val resultStreamDF: Dataset[Row] = streamDataFrame
      // 累加统计订单销售额总额
      .agg(sum($"money").as("total_amt"))
      .withColumn("total", lit("global"))
    //输出Redis及启动流式应用
    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      // 结果输出到Redis
      .foreachBatch{ (batchDF: DataFrame, _: Long) => batchDF
        // 降低分区数目
        .coalesce(1)
        // TODO: 行转列
        .groupBy()
        .pivot($"total").sum("total_amt")
        // 添加一列, 统计类型
        .withColumn("type", lit("total"))
        .write
        .mode(SaveMode.Append)
        .format("org.apache.spark.sql.redis")
        .option("host", ApplicationConfig.REDIS_HOST)
        .option("port", ApplicationConfig.REDIS_PORT)
        .option("dbNum", ApplicationConfig.REDIS_DB)
        .option("table", "orders:money")
        .option("key.column", "type")
        .save()
      }
      // 流式应用，需要启动start
      .start()
  }
  /** 实时统计：各省份销售额，按照province省份分组 */
  def reportAmtProvince(streamDataFrame: DataFrame): Unit = {
    //导入隐式转换
    import streamDataFrame.sparkSession.implicits._
    //业务计算
    val resultStreamDF: Dataset[Row] = streamDataFrame
      // 按照省份province分组，求和
      .groupBy($"province")
      .agg(sum($"money").as("total_amt"))
    // c. 输出Redis及启动流式应用
    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-province")
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_PROVINCE_CKPT)
      // 流式应用，需要启动start
      .start()
  }
  /** 实时统计：重点城市cities销售额，按照city城市分组 */ def reportAmtCity(streamDataFrame: DataFrame): Unit = {
    //导入隐式转换
    val session: SparkSession = streamDataFrame.sparkSession
    import session.implicits._
    // 重点城市：9个城市
    val cities: Array[String] = Array(
      "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
    )
    //业务计算
    val resultStreamDF: Dataset[Row] = streamDataFrame
    //输出Redis及启动流式应用
    resultStreamDF
      .writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      // 流式应用，需要启动start
      .start() }
  def main(args: Array[String]): Unit = {
    // 1. 获取SparkSession实例对象
    val spark: SparkSession =SparkSession.builder()
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    // 2. 从KAFKA读取消费数据
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    // 3. 提供数据字段
    val orderStreamDF: DataFrame = kafkaStreamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 转换为Dataset类型
      .as[String]
      // 过滤数据：通话状态为success
      .filter(record => null != record && record.trim.split(",").length > 0)
      // 提取字段：orderMoney、province和city
      .select(
        get_json_object($"value", "$.orderMoney").cast(DoubleType).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )
    // 4. 实时报表统计：总销售额、各省份销售额及重点城市销售额
    reportAmtTotal(orderStreamDF)
    reportAmtProvince(orderStreamDF)
    reportAmtCity(orderStreamDF)
    // 5. 定时扫描HDFS文件，关闭停止StreamingQuery spark.streams.active.foreach{query =>
 //   StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)
  }

}