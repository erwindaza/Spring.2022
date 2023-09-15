import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DeleteFromHBase {
  def main(args: Array[String]): Unit = {
    // Spark configuration
    val sparkConf = new SparkConf().setAppName("Delete from HBase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // HBase configuration
    val hbaseConf = HBaseConfiguration.create()
    val tableName = TableName.valueOf("your_table_name")
    val columnFamily = Bytes.toBytes("your_column_family")

    // Create an RDD with row keys to delete (replace this with your row keys)
    val rowKeys: RDD[String] = sc.parallelize(Seq("row_key1", "row_key2", "row_key3"))

    // Use foreachPartition for improved parallelism
    rowKeys.foreachPartition(keyIterator => {
      // Create a connection for each partition to improve parallelism
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: Table = connection.getTable(tableName)

      keyIterator.foreach(rowKey => {
        val delete = new Delete(Bytes.toBytes(rowKey))
        table.delete(delete)
      })

      // Close the connection for this partition
      connection.close()
    })

    println("Data deleted from HBase table successfully.")

    // Stop Spark context
    sc.stop()
  }
}
Este código Scala eliminará las filas con las claves especificadas de la tabla HBase.

Ahora, puedes empaquetar el código Scala en un archivo JAR. Asegúrate de incluir todas las dependencias necesarias en el JAR.

Luego, puedes ejecutar el trabajo Spark utilizando spark-submit para eliminar los datos de HBase. Ejecuta el siguiente comando en la línea de comandos:

shell
Copy code
spark-submit --class DeleteFromHBase --master local path/al/jar/DeleteFromHBase.jar
Asegúrate de reemplazar path/al/jar/DeleteFromHBase.jar con la ubicación real de tu archivo JAR.

Este trabajo Spark eliminará las filas especificadas de la tabla HBase utilizando las claves proporcionadas en el RDD rowKeys. Asegúrate de reemplazar "your_table_name" y "your_column_family" con los nombres reales de tu tabla y familia de columnas en HBase, y modifica la lista de claves en el RDD según tus requisitos de eliminación.





