import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

case class MyData(id: String, name: String, age: Int, city: String)

object RDDToJsonToHBase {
  def main(args: Array[String]): Unit = {
    // Spark configuration
    val sparkConf = new SparkConf().setAppName("RDD to JSON to HBase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // HBase configuration
    val hbaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf("your_table_name")
    val columnFamily = Bytes.toBytes("your_column_family")

    // Create an RDD with sample data (replace this with your data source)
    val data: RDD[MyData] = sc.parallelize(Seq(
      MyData("1", "Alice", 30, "New York"),
      MyData("2", "Bob", 25, "San Francisco"),
      MyData("3", "Charlie", 35, "Los Angeles")
    ))

    // Convert RDD to JSON
    val jsonData: RDD[String] = data.map(data => write(data)(DefaultFormats))

    // Use foreachPartition for improved parallelism
    jsonData.foreachPartition(jsonIterator => {
      // Create a connection for each partition to improve parallelism
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: Table = connection.getTable(tableName)

      jsonIterator.foreach(json => {
        val parsedJson = parse(json)
        val id = (parsedJson \ "id").extract[String]
        val put = new Put(Bytes.toBytes(id))
        put.addColumn(columnFamily, Bytes.toBytes("data"), Bytes.toBytes(compact(render(parsedJson))))
        table.put(put)
      })

      // Close the connection for this partition
      connection.close()
    })

    println("Data inserted into HBase table successfully.")

    // Stop Spark context
    sc.stop()
  }
}