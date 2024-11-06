import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.streaming.StreamingContext
class ApplicationConfig
object ApplicationConfig {
  private val config:Config=ConfigFactory.load("config.properties")
  /*
  运行模式，开发测试为本地模式，测试生产通过--master传递
  */
  lazy val APP_LOCAL_MODE: Boolean = config.getBoolean("app.is.local")
  lazy val APP_SPARK_MASTER: String = config.getString("app.spark.master")
  /*
  Kafka 相关配置信息
  */
  lazy val KAFKA_BOOTSTRAP_SERVERS: String = config.getString("kafka.bootstrap.servers")
  lazy val KAFKA_AUTO_OFFSET_RESET: String = config.getString("kafka.auto.offset.reset")
  lazy val KAFKA_SOURCE_TOPICS: String = config.getString("kafka.source.topics")
  lazy val KAFKA_ETL_TOPIC: String = config.getString("kafka.etl.topic")
  lazy val KAFKA_MAX_OFFSETS: String = config.getString("kafka.max.offsets.per.trigger")
  lazy val STREAMING_ETL_GROUP_ID: String = config.getString("streaming.etl.group.id")
  //Streaming流式应用，检查点目录
  lazy val STREAMING_ETL_CKPT: String = config.getString("streaming.etl.ckpt")
  lazy val STREAMING_AMT_TOTAL_CKPT: String = config.getString("streaming.amt.total.ckpt")
  lazy val STREAMING_AMT_PROVINCE_CKPT: String = config.getString("streaming.amt.province.ckpt")
  lazy val STREAMING_AMT_CITY_CKPT: String = config.getString("streaming.amt.city.ckpt")
  //Streaming流式应用，停止文件
  lazy val STOP_ETL_FILE: String = config.getString("stop.etl.file")
  lazy val STOP_STATE_FILE: String = config.getString("stop.state.file")
  //Redis 数据库
  lazy val REDIS_HOST: String = config.getString("redis.host")
  lazy val REDIS_PORT: String = config.getString("redis.port")
  lazy val REDIS_DB: String = config.getString("redis.db")
  // 解析IP地址字典数据文件存储路径
  lazy val IPS_DATA_REGION_PATH: String = config.getString("ipdata.region.path")
 // 每个属性变量前使用lazy，表示懒加载初始化，当第一次使用变量时，才会进行初始化。

}
