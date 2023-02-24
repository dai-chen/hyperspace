package org.opensearch.maximus.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import scala.language.implicitConversions
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

class MaximusSqlParser extends StandardTokenParsers with PackratParsers {

  protected val AS = keyword("AS")
  protected val CREATE = keyword("CREATE")
  protected val INDEX = keyword("INDEX")
  protected val ON = keyword("ON")
  protected val TABLE = keyword("TABLE")

  protected lazy val root: Parser[LogicalPlan] = ddlCommand

  protected lazy val ddlCommand: Parser[LogicalPlan] = indexCommands

  protected lazy val indexCommands: Parser[LogicalPlan] =
    createIndex

  override val lexical = {
    val sqlLex = new StdLexical()
    sqlLex
  }

  /**
   * CREATE INDEX index_name
   * ON TABLE [db_name.]table_name (column_name, ...)
   * AS carbondata/bloomfilter/lucene
   */
  protected lazy val createIndex: Parser[LogicalPlan] =
    CREATE ~> INDEX ~ ident ~
      ontable ~
      ("(" ~> repsep(ident, ",") <~ ")") ~
      (AS ~> stringLit) <~ opt(";") ^^ {
        case indexName ~ table ~ cols ~ indexProvider =>
          val tableCols = cols.map(f => f.toLowerCase())
          MaximusCreateIndexCommand(indexProvider)
    }

  def parse(input: String): LogicalPlan = synchronized {
    phrase(root)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failure => null // throw new AnalysisException(failure.toString)
    }
  }

  protected lazy val ontable: Parser[TableIdentifier] =
    ON ~> TABLE.? ~ (ident <~ ".").? ~ ident ^^ {
      case ignored ~ dbName ~ tableName => TableIdentifier(tableName, dbName)
    }

}
