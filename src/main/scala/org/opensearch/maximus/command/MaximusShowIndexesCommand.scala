package org.opensearch.maximus.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StringType

import com.microsoft.hyperspace.Hyperspace

case class MaximusShowIndexesCommand(
    dbName: Option[String],
    tableName: String)
  extends RunnableCommand {

  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("Name", StringType, nullable = true)(),
      AttributeReference("indexedColumns",
                         ArrayType(StringType, true),
                         nullable = true)(),
      AttributeReference("indexLocation", StringType, nullable = true)(),
      AttributeReference("State", StringType, nullable = true)(),
      AttributeReference("additionalStats",
                         MapType(StringType, StringType, true),
                         nullable = true)()
    )
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hyperspace = new Hyperspace(sparkSession)
    hyperspace.indexes.collect.toSeq
  }
}
