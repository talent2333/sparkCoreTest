import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/7 19:21
 * Description 
 */
object demo_fromFiles {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //查看textFile源码
    val rdd = sc.textFile("D:\\workspace\\sparkCoreTest\\src\\input\\1.txt",3)
    rdd.saveAsTextFile("output")

    //使用parallelize创建rdd
    sc.stop()
  }

}
