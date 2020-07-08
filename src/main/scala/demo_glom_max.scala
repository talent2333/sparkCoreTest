import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 9:46
 * Description 
 */
object demo_glom_max {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3 )


    val rdd2: RDD[Array[Int]] = rdd.glom()
    val rdd3: RDD[Int] = rdd2.map(_.max)
    val res: Double = rdd3.sum()

    println(s"result = $res")


    //使用parallelize创建rdd
    sc.stop()
  }

}
