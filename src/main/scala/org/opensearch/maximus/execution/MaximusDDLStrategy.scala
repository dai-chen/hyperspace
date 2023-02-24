package org.opensearch.maximus.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

object MaximusDDLStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    /*
    plan match {
      case _: None
    }
    */
    Seq()
  }
}
