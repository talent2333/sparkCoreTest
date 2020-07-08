import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/7 18:23
 * Description RDD源码从集合中创建
 * 规则
 * （（分区序号*数据长度）/分区数 -> （（分区序号+1）*数据长度）/分区数）
 * (0,1)  (1,3) (3,5)
 */
object demo_fromArrays {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //查看makeRDD源码
    val rdd = sc.makeRDD(Array(1,2,3,4,5),3)
    rdd.saveAsTextFile("src/output")
  }

}
