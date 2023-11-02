import org.apache.spark.sql.DataFrame

object Implicits {
  implicit class DataFrameOps(df: DataFrame) {
    def writeToCsv(fullPath: String): Unit = {
      df.coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .mode("overwrite")
        .csv(fullPath)
    }
  }
}
