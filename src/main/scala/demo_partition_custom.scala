import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 16:54
 * Description 
 */
object demo_partition_custom {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    //3.2 自定义分区
    val rdd3: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    //4 打印查看对应分区数据
    val indexRdd: RDD[(Int, String)] = rdd3.mapPartitionsWithIndex(
      (index, datas) => {
        // 打印每个分区数据，并带分区号
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        // 返回分区的数据
        datas
      }
    )

    indexRdd.collect()


    //使用parallelize创建rdd
    sc.stop()
  }

}
class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case i: Int =>
        val keyInt: Int = i
        if (keyInt % 2 == 0) {
          0
        }
        else
          1
      case _ => 0
    }
  }
}