import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/7 21:02
 * Description 
 */
object demo_map {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 4,2)
    val mapRdd = rdd.map(_*2)
    mapRdd.collect().foreach(println)

    //使用parallelize创建rdd
    sc.stop()
  }

}
