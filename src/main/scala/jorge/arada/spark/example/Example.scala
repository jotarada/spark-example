package jorge.arada.spark.example

import jorge.arada.spark.example.entities.AirInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}


object Example {


  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Example App")
      .getOrCreate()

    //exampleRdd(sparkSession)
    //exampleDF(sparkSession)
    //exampleDS(sparkSession)
    //medAvail_seat_km_per_week(sparkSession)
    //medAvail_seat_km_per_weekRdd(sparkSession)
    medAvail_seat_km_per_weekSQL(sparkSession)
  }

  def exampleRdd(sparkSession: SparkSession): Unit = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    val rdd: RDD[String] = sparkSession.sparkContext.textFile(filePath)
    rdd.foreach(f => println(f))
  }

  def exampleDF(sparkSession: SparkSession): Unit = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    import sparkSession.implicits._
    val df: DataFrame =
      sparkSession
        .read
        .options(Map("header" -> "true"))
        .csv(filePath)
    df.show()

    df.map(x => x.getString(x.fieldIndex("airline")).toUpperCase).show
  }

  def exampleDS(sparkSession: SparkSession): Unit = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    import sparkSession.implicits._
    val schema = Encoders.product[AirInfo].schema
    val ds: Dataset[AirInfo] =
      sparkSession
        .read
        .options(Map("header" -> "true"))
        .schema(schema)
        .csv(filePath).as[AirInfo]
    ds.show()

    val ds1 = ds.map(x => x.airline.toUpperCase)
    ds1.show
  }

  def medAvail_seat_km_per_week(sparkSession: SparkSession) = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    val df: DataFrame =
      sparkSession
        .read
        .options(Map("header" -> "true"))
        .csv(filePath)
    val df2 = df.agg("avail_seat_km_per_week" -> "avg")
    val df3 = df2
      .withColumn("avail_seat_km_per_week", df2.col("avg(avail_seat_km_per_week)")
        .cast(DecimalType(18, 2)))
      .drop(df2.col("avg(avail_seat_km_per_week)"))

    df3.show
  }

  def medAvail_seat_km_per_weekRdd(sparkSession: SparkSession) = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    val rdd = sparkSession.sparkContext.textFile(filePath).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    val rdd1 = rdd.map(line => line.split(","))
    val rdd2 = rdd1.map(words => words(1).toLong)
    val count = rdd2.count()
    val sum = rdd2.reduce((a, b) => a + b)
    println(sum / count)
  }

  def medAvail_seat_km_per_weekSQL(sparkSession: SparkSession) = {
    val filePath = getClass.getClassLoader.getResource("airlines.csv").getPath
    val df: DataFrame =
      sparkSession
        .read
        .options(Map("header" -> "true"))
        .csv(filePath)

    df.createOrReplaceTempView("airlines")
    val df2 = sparkSession.sql("select avg (avail_seat_km_per_week) as avg  from airlines ")

    val df3 = df2
      .withColumn("avail_seat_km_per_week", df2.col("avg")
        .cast(DecimalType(18, 2)))
      .drop(df2.col("avg"))

    df3.show

  }
}
