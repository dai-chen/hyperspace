package org.opensearch.maximus.command

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

import com.microsoft.hyperspace.Hyperspace

case class MaximusDropIndexCommand(indexName: String)
  extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    log.info(s"Dropping index $indexName")

    val hyperspace = new Hyperspace(sparkSession)
    hyperspace.deleteIndex(indexName)
    hyperspace.vacuumIndex(indexName)
    Seq.empty
  }
}
