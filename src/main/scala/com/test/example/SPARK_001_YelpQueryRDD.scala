package com.test.example

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.mapr.db.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SPARK_001_YelpQueryRDD {

  def main(args: Array[String]): Unit = {

    val tableName: String = args(0)
    println("Table Name" + tableName)

    val sparkSession = SparkSession
      .builder()
      .appName("SPARK_001_YelpQueryRDD")
      // .config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate()

    val df = sparkSession.loadFromMapRDB(tableName): DataFrame

    df.schema
    df.show

  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Business (@JsonProperty("_id") id: String,
                   @JsonProperty("name") name: String,
                   @JsonProperty("review_count") review_count: Int,
                   @JsonProperty("stars") stars: Float,
                   @JsonProperty("address") address: String,
                   @JsonProperty("city") city: String,
                   @JsonProperty("state") state: String)