package com.tw.apps

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


class OverwriteCSVSink(sqlContext: SQLContext,
                       parameters: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)
      .repartition(1)
      .withColumn("year", year(current_date))
      .withColumn("month", lpad(month(current_date), 2, "0"))
      .withColumn("day", lpad(dayofmonth(current_date), 2, "0"))
      .withColumn("hour", lpad(hour(current_timestamp), 2, "0"))
      .write
      .partitionBy("year", "month", "day", "hour")
      .mode(SaveMode.Append)
      .format("csv")
      .option("header", parameters.get("header").orNull)
      .option("truncate", parameters.get("truncate").orNull)
      .option("checkpointLocation", parameters.get("checkpointLocation").orNull)
      .option("path", parameters.get("path").orNull)
      .insertInto("station_mart")
  }
}

class SinkProvider extends StreamSinkProvider
  with DataSourceRegister {
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    new OverwriteCSVSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "overwriteCSV"
}
