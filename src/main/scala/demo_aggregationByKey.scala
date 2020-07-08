import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 16:33
 * Description 按key分区，区内和区间运算
 */
object demo_aggregationByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2),
      ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val value = rdd.aggregateByKey(5)(math.min,_+_)
    value.collect().foreach(println)

    //使用parallelize创建rdd
    sc.stop()
  }

}
