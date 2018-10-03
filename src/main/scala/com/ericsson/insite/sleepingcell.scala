package com.ericsson.insite

import java.util.UUID

import com.mapr.db.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}

object sleepingcell {

  def main(args: Array[String]): Unit = {
    val utcstart: String = args(0)
    val utcend: String = args(1)

    println("UTC Start - " + utcstart)
    println("UTC End - " + utcend)

    val sparkSession = SparkSession
      .builder()
      .appName("Sleeping_Cell")
      .getOrCreate()

    val filePath: String = "/mapr/ike/prod/cnda/bharti/pm/4g/enodeb/maprdb/"

    val dfFDDC = sparkSession.loadFromMapRDB(filePath + "EUtranCellFDD_C_rop")
      .filter("_id>='" + utcstart + "' and _id <= '" + utcend + "'")
      .select("_id", "motype", "pmCellDowntimeMan", "pmCellDowntimeAuto"): DataFrame

    val dfFDDR = sparkSession.loadFromMapRDB(filePath + "EUtranCellFDD_R_rop")
      .filter("_id>='" + utcstart + "' and _id <= '" + utcend + "'")
      .select("_id", "pmRaSuccCbra", "pmRaAttCbra", "pmRaSuccCfra", "pmRaAttCfra", "pmRrcConnEstabSucc", "pmRrcConnEstabAtt"): DataFrame

    val dfFDDP = sparkSession.loadFromMapRDB(filePath + "EUtranCellFDD_P_rop")
      .filter("_id>='" + utcstart + "' and _id <= '" + utcend + "'")
      .select("_id", "pmPdcpVolDlDrb", "pmPdcpVolDlSrb", "pmPdcpVolUlDrb", "pmPdcpVolUlSrb"): DataFrame

    val dfTDD = sparkSession.loadFromMapRDB(filePath + "EUtranCellTDD_rop").filter("_id>='" + utcstart + "' and _id <= '" + utcend + "'"): DataFrame

    print("...before joining.....")

    val joinedFDD = dfFDDC.join(dfFDDR, "_id").join(dfFDDP, "_id")

    joinedFDD.createOrReplaceTempView("temp_table_1")

    sparkSession.udf.register("toManagedElement", (_id: String) => ((_id.split("\\|")(3).split(",")(0).split("=")(1))))
    sparkSession.udf.register("toENodeBFunction", (_id: String) => ((_id.split("\\|")(3).split(",")(1).split("=")(1))))
    sparkSession.udf.register("toCellValue", (_id: String) => ((_id.split("\\|")(3).split(",")(2).split("=")(1))))
    sparkSession.udf.register("toUtcdate", (_id: String) => ((_id.split("\\|")(1))))

    val finalFDD = joinedFDD.sqlContext.sql("select _id, toUtcdate(_id) AS utcDate, toManagedElement(_id) AS managedElement, " +
      " toENodeBFunction(_id) AS eNodeBFunction, motype, toCellValue(_id) AS cellValue, pmCellDowntimeMan, pmCellDowntimeAuto, pmRaSuccCbra, " +
      " pmRaAttCbra, pmRaSuccCfra, pmRaAttCfra, pmRrcConnEstabSucc, pmRrcConnEstabAtt, pmPdcpVolDlDrb, pmPdcpVolDlSrb, pmPdcpVolUlDrb," +
      " pmPdcpVolUlSrb from temp_table_1")

    //pmCellDowntimeMan,pmCellDowntimeAuto,pmRaSuccCbra,pmRaAttCbra,pmRaSuccCfra,pmRaAttCfra,pmRrcConnEstabSucc,pmRrcConnEstabAtt,pmPdcpVolDlDrb,pmPdcpVolDlSrb,
    //pmPdcpVolUlDrb,pmPdcpVolUlSrb FROM

    dfTDD.createOrReplaceTempView("temp_table_2")

    sparkSession.udf.register("toManagedElement", (_id: String) => ((_id.split("\\|")(3).split(",")(0).split("=")(1))))
    sparkSession.udf.register("toENodeBFunction", (_id: String) => ((_id.split("\\|")(3).split(",")(1).split("=")(1))))
    sparkSession.udf.register("toCellValue", (_id: String) => ((_id.split("\\|")(3).split(",")(2).split("=")(1))))
    sparkSession.udf.register("toUtcdate", (_id: String) => ((_id.split("\\|")(1))))

    val finalTDD = dfTDD.sqlContext.sql("select _id, toUtcdate(_id) AS utcDate, toManagedElement(_id) AS managedElement, " +
      " toENodeBFunction(_id) AS eNodeBFunction, motype, toCellValue(_id) AS cellValue, pmCellDowntimeMan, pmCellDowntimeAuto, pmRaSuccCbra, " +
      " pmRaAttCbra, pmRaSuccCfra, pmRaAttCfra, pmRrcConnEstabSucc, pmRrcConnEstabAtt, pmPdcpVolDlDrb, pmPdcpVolDlSrb, pmPdcpVolUlDrb," +
      " pmPdcpVolUlSrb from temp_table_2")

    val outputTableName = "testTable_" + UUID.randomUUID().toString()

    finalFDD
      .insertToMapRDB(outputTableName, createTable = true)

    finalTDD
      .insertToMapRDB(outputTableName, createTable = false)
  }
}
