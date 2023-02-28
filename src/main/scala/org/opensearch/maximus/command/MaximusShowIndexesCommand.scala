package org.opensearch.maximus.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

import com.microsoft.hyperspace.Hyperspace

case class MaximusShowIndexesCommand(
    dbName: Option[String],
    tableName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hyperspace = new Hyperspace(sparkSession)
    val indexes = hyperspace.indexes
    indexes.show()
    Seq.empty // TODO: error if return indexes.collect()
  }
}
