import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

object StreamingUtils {
  /**
   *获取StreamingContext流式上下文实例对象
   *@param clazz Spark Application字节码Class对象
   *@param batchInterval 每批次时间间隔
   */
  def createStreamingContext(clazz: Class[_], batchInterval: Int): StreamingContext = {
    // 构建对象实例
    val context: StreamingContext =
      StreamingContext.getOrCreate( () => {
        // 1. 构建SparkConf对象
        val sparkConf: SparkConf = new SparkConf()
          .setAppName(clazz.getSimpleName.stripSuffix("$"))
          .set("spark.debug.maxToStringFields", "2000")
          .set("spark.sql.debug.maxToStringFields", "2000")
          .set("spark.streaming.stopGracefullyOnShutdown", "true")
        // 2. 判断应用是否本地模式运行，如果是设置值
        if(ApplicationConfig.APP_LOCAL_MODE){ sparkConf
          .setMaster(ApplicationConfig.APP_SPARK_MASTER)


        }
