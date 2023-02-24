package org.opensearch.maximus.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

class MaximusSqlParser extends StandardTokenParsers with PackratParsers {

  protected val AS = maximusKeyword("AS")
  protected val CREATE = maximusKeyword("CREATE")
  protected val INDEX = maximusKeyword("INDEX")
  protected val ON = maximusKeyword("ON")
  protected val TABLE = maximusKeyword("TABLE")

  protected lazy val root: Parser[LogicalPlan] = ddlCommand

  protected lazy val ddlCommand: Parser[LogicalPlan] = indexCommands

  protected lazy val indexCommands: Parser[LogicalPlan] =
    createIndex

  override val lexical = {
    val sqlLex = new MaximusSqlLexical
    // sqlLex.initialize(newReservedWords)
    sqlLex
  }

  import lexical.Identifier

  implicit def regexToParser(regex: Regex): Parser[String] = {
    acceptMatch(
      s"identifier matching regex ${ regex }",
      { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
    )
  }

  def maximusKeyword(keys: String): Regex = {
    ("(?i)" + keys).r
  }

  /**
   * CREATE INDEX index_name
   * ON TABLE [db_name.]table_name (column_name, ...)
   * AS bloomfilter/lucene
   */
  protected lazy val createIndex: Parser[LogicalPlan] =
    CREATE ~> INDEX ~> ident ~
      ontable ~
      ("(" ~> repsep(ident, ",") <~ ")") ~
      (AS ~> stringLit) <~ opt(";") ^^ {
        case indexName ~ table ~ cols ~ indexProvider =>
          val dbName = table.database
          val tableName = table.table.toLowerCase()
          val tableCols = cols.map(f => f.toLowerCase())
          MaximusCreateIndexCommand(dbName, indexName, tableName, tableCols, indexProvider)
    }

  def parse(input: String): LogicalPlan = synchronized {
    phrase(root)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failure => throw new IllegalStateException(failure.toString)
    }
  }

  protected lazy val ontable: Parser[TableIdentifier] =
    ON ~> TABLE.? ~ (ident <~ ".").? ~ ident ^^ {
      case ignored ~ dbName ~ tableName => TableIdentifier(tableName, dbName)
    }

}
