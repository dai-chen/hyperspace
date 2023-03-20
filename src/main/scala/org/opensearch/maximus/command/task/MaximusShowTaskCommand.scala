package org.opensearch.maximus.command.task

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StringType

case class MaximusShowTaskCommand() extends RunnableCommand {

  override def output: Seq[Attribute] = {
      Seq(
        AttributeReference("Id", StringType, nullable = false)(),
        AttributeReference("Name", StringType, nullable = true)(),
        AttributeReference("Progress", StringType, nullable = true)()
      )
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.streams.active
      .map(stream => Row(
        stream.id.toString,
        stream.name,
        allValidProgress(stream)
      ))
      .toSeq
  }

  private def allValidProgress(stream: StreamingQuery): String = {
    stream.recentProgress
      .filter(progress => progress.numInputRows > 0)
      .map(progress => progress.prettyJson)
      .mkString("\n")
  }
}
