import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 11:40
 * Description 
 */
object demo_sample {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10)

    val rdd2 = rdd.sample(false,0.5)
    rdd2.foreach(println)

    //使用parallelize创建rdd
    sc.stop()
  }

}
