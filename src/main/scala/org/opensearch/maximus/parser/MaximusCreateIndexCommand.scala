package org.opensearch.maximus.parser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.dataskipping.DataSkippingIndexConfig
import com.microsoft.hyperspace.index.dataskipping.sketches.BloomFilterSketch

case class MaximusCreateIndexCommand(
    dbName: Option[String],
    indexName: String,
    tableName: String,
    columnNames: Seq[String],
    indexProviderName: String)
  extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    log.info(s"Create index $indexName for table $tableName on columns $columnNames")

    val hyperspace = new Hyperspace(sparkSession)
    val table = sparkSession.sqlContext.table(tableName)
    val indexConfig = DataSkippingIndexConfig(indexName,
      BloomFilterSketch(columnNames.head, 0.01, 10))
    hyperspace.createIndex(table, indexConfig)
    Seq.empty
  }
}
