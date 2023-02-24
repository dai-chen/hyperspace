package org.opensearch.maximus.parser

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

import com.microsoft.hyperspace.Hyperspace

case class MaximusCreateIndexCommand(
    dbName: Option[String],
    indexName: String,
    tableName: String,
    columnNames: Seq[String],
    indexProviderName: String)
  extends RunnableCommand {

  // val hyperspace = new Hyperspace(SparkSession.getActiveSession.get)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // hyperspace.createIndex()
    Seq.empty
  }
}
