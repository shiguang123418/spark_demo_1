import ApplicationConfig._
//import SparkUtils._
//import StreamingUtils._
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{get_json_object, struct, to_json, udf}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}
import org.apache.spark.sql.functions.to_json
/**
 * 对流式数据StreamDataFrame进行ETL过滤清洗转换操作
 */
object RealTimeOrderETL extends Logging{
  def streamingProcess(streamDF: DataFrame): DataFrame = {
    val session =  SparkSession.builder()
      .appName("Simple Application")
      .config("spark.master", "local[*]")
      .getOrCreate()
    import session.implicits._
    import org.apache.spark.sql.functions._
    // 对数据进行ETL操作，获取订单状态为0(打开)及转换IP地址为省份和城市
    // 1. 获取订单记录Order Record数据
    val recordStreamDS: Dataset[String] = streamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 转换为Dataset类型
      .as[String]
      // 过滤数据：通话状态为success
      .filter(record => null != record && record.trim.split(",").length > 0)
    // 自定义UDF函数，解析IP地址为省份和城市
    session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)
    val ip_to_province =udf(
      (ip: String) => {
        val dbSearcher = new DbSearcher(new DbConfig(), "ip2region.db")
        // 依据IP地址解析
        val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
        // 中国|0|海南省|海口市|教育网
        val region: String = dataBlock.getRegion
        // 分割字符串，获取省份和城市
        val Array(_, _, province, city, _) = region.split("\\|")
        // 返回Region对象
        province
      })
    val ip_to_city=udf(
      (ip: String) => {
        val dbSearcher = new DbSearcher(new DbConfig(), "ip2region.db")
        // 依据IP地址解析
        val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
        // 中国|0|海南省|海口市|教育网
        val region: String = dataBlock.getRegion
        // 分割字符串，获取省份和城市
        val Array(_, _, province, city, _) = region.split("\\|")
        // 返回Region对象
        city
      })
    // 2. 其他订单字段，按照订单状态过滤和转换IP地址
    val resultStreamDF: DataFrame = recordStreamDS
      // 提取订单字段
      .select(
        get_json_object($"value", "$.orderId").as("orderId"),
        get_json_object($"value", "$.userId").as("userId"),
        get_json_object($"value", "$.orderTime").as("orderTime"),
        ip_to_province(get_json_object($"value","$.ip")).as("province"),
        ip_to_city(get_json_object($"value","$.ip")).as("city"),
        get_json_object($"value", "$.orderMoney").as("orderMoney"),
        get_json_object($"value", "$.orderStatus").as("orderStatus")
      )
    resultStreamDF
  }
  def main(args: Array[String]): Unit = {
    // 1. 获取SparkSession实例对象
    val spark: SparkSession =
//      SparkUtils.createSparkSession(this.getClass)
      SparkSession.builder()
      .appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()
    // 2. 从KAFKA读取消费数据
    import spark.implicits._
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_SOURCE_TOPICS)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    // 3. 数据ETL操作
    val etlStreamDF: DataFrame = streamingProcess(kafkaStreamDF)
    // 4. 针对流式应用来说，输出的是流
    //
    import org.apache.spark.sql.functions._
    val query: StreamingQuery = etlStreamDF.select(to_json(struct(etlStreamDF.columns.map(col(_)):_*)) as "value").writeStream
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("topic", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_ETL_CKPT)
      //          .option("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      //          .option("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      // 流式应用，需要启动start
      .start()
//    val query1: StreamingQuery = etlStreamDF
//      .select(to_json(struct(etlStreamDF.columns.map(col(_)):_*)) as "value")
//      .writeStream
//      .outputMode(OutputMode.Append())
//      .format("console") // 控制台输出
//      .option("truncate", "false") // 不截断内容
//      .start()
    query.awaitTermination()
//    query1.awaitTermination()
    query.stop()
  }
}