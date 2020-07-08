import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Author talent2333
 * Date 2020/7/7 18:13
 * Description 从外部存储系统创建数据集
 */
object demo_outRDD {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //使用parallelize创建rdd
    val lineRDD: RDD[String] = sc.textFile("D:\\workspace\\sparkCoreTest\\src\\input\\1.txt")
//    val lineRDD = sc.textFile("hdfs://hadoop102:9820/input")
    lineRDD.foreach(println(_))
    println("partitions:"+lineRDD.partitions.size)
    sc.stop()
  }

}
