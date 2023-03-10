package org.opensearch.maximus.command.mv

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.hyperspace.utils.{dataFrameToLogicalPlan, logicalPlanToDataFrame}

case class MaximusCreateMVCommand(
    dbName: Option[String],
    mvName: String,
    queryString: String)
  extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    log.info(s"Creating MV $mvName")

    val dataFrame = sparkSession.sql(queryString)
    val streamingDf = convertToStreaming(dataFrame)
    val streamingQuery =
      streamingDf
        .withWatermark("time", "10 minutes")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/") // Required and hardcoding for now
        .toTable(mvName)

    log.info("MV refreshing job started")
    streamingQuery.explain()

    Seq.empty
  }

  private def convertToStreaming(dataFrame: DataFrame): DataFrame = {
    val sparkSession = dataFrame.sparkSession
    val batchPlan = dataFrameToLogicalPlan(dataFrame)
    val streamingPlan = batchPlan transform {
      case relation: LogicalRelation =>
        // StreamingRelation will be wrapped by a SubqueryAlias node
        // whenever SessionCatalog is requested to find a table or view in catalogs
        val subqueryAlias = dataFrameToLogicalPlan(
          sparkSession
            .readStream
            .schema(relation.schema)
            .format("delta")
            .table(relation.catalogTable.get.qualifiedName)
        ).asInstanceOf[SubqueryAlias]

        // Remove SubqueryAlias node outside and only keep required StreamingRelation node
        val streamRelation = subqueryAlias.child.asInstanceOf[StreamingRelation]

        // Since SparkSQL analyzer will match the UUID in attribute,
        // create a new StreamRelation and re-use the same attribute from LogicalRelation
        StreamingRelation(streamRelation.dataSource, streamRelation.sourceName, relation.output)

      case otherNode: LogicalPlan => otherNode
    }

    logicalPlanToDataFrame(sparkSession, streamingPlan)
  }
}
