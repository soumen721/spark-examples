package com.ericsson.insite

import java.util.UUID

import com.mapr.db.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}

object sleepingcell {

  def main(args: Array[String]): Unit = {
    val utcStart: String = args(0)
    val utcEnd: String = args(1)

    println("UTC Start - " + utcStart)
    println("UTC End - " + utcEnd)

    val props: Config =  ConfigFactory.parseResources("job-details.properties")

    val dfFDDCCloumns = props.getString("fddc-columns-name").split(",").map(_.trim)
    val dfFDDRCloumns = props.getString("fddr-columns-name").split(",").map(_.trim)
    val dfFDDPCloumns = props.getString("fddp-columns-name").split(",").map(_.trim)
    val dfTDDCloumns = props.getString("tdd-columns-name").split(",").map(_.trim)
    println("1st DF Column Name : "+ dfFDDCCloumns.mkString(","))

    val sparkSession = SparkSession
      .builder()
      .appName("Sleeping_Cell")
      .getOrCreate()

    //val filePath: String = "/mapr/ike/prod/cnda/bharti/pm/4g/enodeb/maprdb/"

    val dfFDDC = sparkSession.loadFromMapRDB(props.getString("fddc-table-name"))
      .filter("_id>='" + utcStart + "' and _id <= '" + utcEnd + "'")
      .selectExpr(dfFDDCCloumns: _*): DataFrame

    println("1st DF created")
    val dfFDDR = sparkSession.loadFromMapRDB(props.getString("fddr-table-name"))
      .filter("_id>='" + utcStart + "' and _id <= '" + utcEnd + "'")
      .selectExpr(dfFDDRCloumns: _*): DataFrame

    val dfFDDP = sparkSession.loadFromMapRDB(props.getString("fddp-table-name"))
      .filter("_id>='" + utcStart + "' and _id <= '" + utcEnd + "'")
      .selectExpr(dfFDDPCloumns: _*): DataFrame

    val dfTDD = sparkSession.loadFromMapRDB(props.getString("tdd-table-name"))
      .filter("_id>='" + utcStart + "' and _id <= '" + utcEnd + "'")
      .selectExpr(dfTDDCloumns: _*): DataFrame

    println("...before joining.....")

    val joinedFDD = dfFDDC.join(dfFDDR, "_id").join(dfFDDP, "_id")

    joinedFDD.createOrReplaceTempView("temp_table_1")

    sparkSession.udf.register("toManagedElement", (_id: String) => _id.split("\\|")(3).split(",")(0).split("=")(1))
    sparkSession.udf.register("toENodeBFunction", (_id: String) => _id.split("\\|")(3).split(",")(1).split("=")(1))
    sparkSession.udf.register("toCellValue", (_id: String) => _id.split("\\|")(3).split(",")(2).split("=")(1))
    sparkSession.udf.register("toUtcdate", (_id: String) => _id.split("\\|")(1))

    val outputQuery = props.getString("output-table-columns")
    val query1 = outputQuery + " temp_table_1"
    println("Query1:: "+ query1)
    val finalFDD = joinedFDD.sqlContext.sql(query1)

    dfTDD.createOrReplaceTempView("temp_table_2")
    val query2 = outputQuery + " temp_table_2"
    println("Query2:: "+ query2)
    val finalTDD = joinedFDD.sqlContext.sql(query2)

    val outputTableName = "testTable_" + UUID.randomUUID().toString

    finalFDD.show(5)
    finalFDD
      .insertToMapRDB(outputTableName, createTable = true)
    finalTDD.show(5)
    finalTDD
      .insertToMapRDB(outputTableName, createTable = false)
  }
}
