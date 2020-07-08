/**
 * Author talent2333
 * Date 2020/7/7 14:46
 * Description 
 */
import org.apache.spark.{SparkConf,SparkContext}
object demo_parallelize {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[8]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //使用parallelize创建rdd
    val rdd = sc.parallelize((Array(1,2,3,4,5,6,7,8)))
    rdd.foreach(println)
    println("分区数："+rdd.partitions.size)
    sc.stop()
  }

}
