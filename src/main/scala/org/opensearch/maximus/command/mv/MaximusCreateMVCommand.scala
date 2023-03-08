package org.opensearch.maximus.command.mv

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

case class MaximusCreateMVCommand(
    dbName: Option[String],
    mvName: String,
    queryString: String)
  extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    log.info(s"Creating MV $mvName")
    Seq.empty
  }
}
