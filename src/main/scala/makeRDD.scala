import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/7 18:07
 * Description 
 */
object makeRDD {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //使用parallelize创建rdd
    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
    rdd.foreach(println)
    println(rdd.partitions.size)
    sc.stop()

  }

}
