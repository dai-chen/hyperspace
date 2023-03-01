package org.opensearch.maximus.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.opensearch.maximus.command._
import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

class MaximusSqlParser extends StandardTokenParsers with PackratParsers {

  protected val AS = maximusKeyword("AS")
  protected val CREATE = maximusKeyword("CREATE")
  protected val DROP = maximusKeyword("DROP")
  protected val INDEX = maximusKeyword("INDEX")
  protected val INDEXES = maximusKeyword("INDEXES")
  protected val ON = maximusKeyword("ON")
  protected val REFRESH = maximusKeyword("REFRESH")
  protected val SHOW = maximusKeyword("SHOW")
  protected val TABLE = maximusKeyword("TABLE")

  protected lazy val root: Parser[LogicalPlan] = ddlCommand

  protected lazy val ddlCommand: Parser[LogicalPlan] = indexCommands

  protected lazy val indexCommands: Parser[LogicalPlan] =
    createIndex | refreshIndex | dropIndex | showIndexes

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

  /**
   * REFRESH INDEX index_name
   * ON [db_name.]table_name
   */
  protected lazy val refreshIndex: Parser[LogicalPlan] =
    REFRESH ~> INDEX ~>  ident ~
      ontable <~ opt(";") ^^ {
      case indexName ~ parentTable =>
        MaximusRefreshIndexCommand(indexName.toLowerCase(), parentTable)
    }

  protected lazy val showIndexes: Parser[LogicalPlan] =
    SHOW ~> INDEXES ~> ontable <~ opt(";") ^^ {
      case table =>
        MaximusShowIndexesCommand(table.database, table.table)
    }

  protected lazy val dropIndex: Parser[LogicalPlan] =
    DROP ~> INDEX ~> ident ~
      ontable <~ opt(";") ^^ {
      case indexName ~ table =>
        MaximusDropIndexCommand(indexName.toLowerCase())
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
