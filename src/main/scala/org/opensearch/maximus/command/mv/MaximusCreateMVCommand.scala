package org.opensearch.maximus.command.mv

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hyperspace.utils.logicalPlanToDataFrame
import org.apache.spark.unsafe.types.UTF8String
import org.opensearch.maximus.function.TumbleFunction

case class MaximusCreateMVCommand(
    dbName: Option[String],
    mvName: String,
    queryString: String)
  extends RunnableCommand with Logging {

  // Hardcoding watermark delay for now
  private val watermarkDelay = UTF8String.fromString("0 Minute")

  /*
  private val attributeMap = new util.HashMap[String, ExprId]()
  private val watermarkAttrMetadataMap = new util.HashMap[String, Metadata]()
   */

  override def run(sparkSession: SparkSession): Seq[Row] = {
    log.info(s"Creating MV $mvName")

    val streamingDf = buildStreamingJob(sparkSession)
    val streamingQuery =
      streamingDf
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/delta/_checkpoints/") // Required and hardcoding for now
        .toTable(mvName)

    log.info("MV refreshing job started")
    streamingQuery.explain()

    Seq.empty
  }

  def buildStreamingJob(sparkSession: SparkSession): DataFrame = {
    val dataFrame = sparkSession.sql(queryString)
    val streamingPlan = dataFrame.queryExecution.logical transform {

      // Insert watermark operator between Aggregate and its child
      case Aggregate(grouping, agg, child) =>
        val timeCol = grouping.collect {
          case UnresolvedFunction(identifier, args, _, _)
            if identifier.funcName.toLowerCase() == TumbleFunction.identifier.funcName
              => args.head
        }

        if (timeCol.isEmpty) {
          throw new IllegalStateException(
            "Windowing function is required for streaming aggregation")
        }
        Aggregate(grouping, agg,
          EventTimeWatermark(
            timeCol.head.asInstanceOf[Attribute],
            IntervalUtils.stringToInterval(watermarkDelay),
            child))

      // Reset isStreaming flag in relation to true
      case UnresolvedRelation(multipartIdentifier, options, _) =>
        UnresolvedRelation(multipartIdentifier, options, isStreaming = true)
    }

    logicalPlanToDataFrame(dataFrame.sparkSession, streamingPlan)
  }

  // Unnecessary complicated logic because DataFrame can accept AST (parsed plan)
  /*
  private def convertToStreaming(dataFrame: DataFrame): DataFrame = {
    val sparkSession = dataFrame.sparkSession
    val batchPlan = dataFrameToLogicalPlan(dataFrame)
    val tempPlan = batchPlan transform {
      // StreamingRelation will be wrapped by a SubqueryAlias node
      // whenever SessionCatalog is requested to find a table or view in catalogs
      case subquery: SubqueryAlias if !subquery.isStreaming => // Transform continues recursively
        val relation = subquery.child.asInstanceOf[LogicalRelation]
        val newNode = dataFrameToLogicalPlan(
          sparkSession
            .readStream
            .schema(relation.schema)
            .format("delta")
            .table(relation.catalogTable.get.qualifiedName)
            .withWatermark("time", "10 minutes")
        ).asInstanceOf[EventTimeWatermark]

        newNode.collect {
          case relation: StreamingRelation =>
            relation.output.map { attr =>
              attributeMap.put(attr.name, attr.exprId)
            }
        }

        // Override what's collected in streaming relation with attributes in watermark operator
        // because it has meta info which is required by groupingExpression in Aggregate
        newNode.collect {
          case watermark: EventTimeWatermark =>
            watermark.output.map { attr =>
              watermarkAttrMetadataMap.put(attr.name, attr.metadata)
            }
        }
        newNode

        // Remove SubqueryAlias node outside and only keep required StreamingRelation node
        // val streamRelation = subqueryAlias.child.asInstanceOf[StreamingRelation]

        // Since SparkSQL analyzer will match the UUID in attribute,
        // create a new StreamRelation and re-use the same attribute from LogicalRelation
        // StreamingRelation(streamRelation.dataSource, streamRelation.sourceName, relation.output)

      case otherNode: LogicalPlan => otherNode
    }

    val streamingPlan = tempPlan transform {
      case Project(projectList: Seq[NamedExpression], child) =>
        val newProjectList = projectList.map { expr =>
          replaceAttribute(expr).asInstanceOf[NamedExpression]
        }
        Project(newProjectList, child)

      case Filter(condition: Expression, child) =>
        Filter(replaceAttribute(condition), child)

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val newGroupingExpressions = groupingExpressions.map { expr =>
          replaceAttribute(expr)
        }
        val newAggregateExpressions = aggregateExpressions.map { expr =>
          replaceAttribute(expr).asInstanceOf[NamedExpression]
        }
        Aggregate(newGroupingExpressions, newAggregateExpressions, child)
    }

    logicalPlanToDataFrame(sparkSession, streamingPlan)
  }

  // replace attribute with the collected ExprId
  private def replaceAttribute(expr: Expression): Expression = expr transform {
    case attribute: Attribute =>
      val exprId: ExprId = attributeMap.get(attribute.name)
      if (exprId != null) {
        if (exprId.id != attribute.exprId.id) {
          val newAttr = AttributeReference(
            attribute.name, attribute.dataType, attribute.nullable,
            attribute.metadata)(exprId, attribute.qualifier)
          if (watermarkAttrMetadataMap.containsKey(attribute.name)) {
            newAttr.withMetadata(watermarkAttrMetadataMap.get(attribute.name))
          } else {
            newAttr
          }
        } else {
          attribute
        }
      } else {
        attribute
      }
  }
  */
}
