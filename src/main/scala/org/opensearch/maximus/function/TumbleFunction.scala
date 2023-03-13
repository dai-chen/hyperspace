package org.opensearch.maximus.function

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.functions.window

object TumbleFunction {

  val identifier: FunctionIdentifier = FunctionIdentifier("tumble")

  val exprInfo: ExpressionInfo = new ExpressionInfo(classOf[Column].getCanonicalName, "window")

  val functionBuilder: Seq[Expression] => Expression =
    (children: Seq[Expression]) => {
      val timeColumn = children.head.asInstanceOf[AttributeReference]
      val windowDuration = children(1).asInstanceOf[Literal]
      window(new Column(timeColumn), windowDuration.toString()).expr
    }
}
