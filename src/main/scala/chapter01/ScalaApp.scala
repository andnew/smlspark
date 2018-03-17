package chapter01

import org.apache.spark.SparkContext

object ScalaApp {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[5]", "First Spark App")
    val filePath = this.getClass.getResource("/").toString.concat("UserPurchaseHistory.csv")

   case class Order(user: String, product: String, price: Double)

    val data = sc.textFile(filePath)
      .map(line => line.split(","))
      .map(record => new Order(record(0), record(1), record(2).toDouble))

    val numPurchases = data.count()

    val uniqueUsers = data.map { order => order.user }.distinct().count()

    val totalRevenue = data.map { order => order.price }.sum()

    val productByPopularity = data.map { order => (order.product, 1) }
      .reduceByKey(_ + _)
      .collect().sortBy(-_._2)

    val mostPopular = productByPopularity(0)

    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers)
    println("Total revenue: " + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

    sc.stop()

  }
}
