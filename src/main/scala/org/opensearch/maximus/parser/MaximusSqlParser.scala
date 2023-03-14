package org.opensearch.maximus.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.command.ExplainCommand
import org.opensearch.maximus.command._
import org.opensearch.maximus.command.mv._
import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

class MaximusSqlParser(
    sparkSession: SparkSession
) extends StandardTokenParsers with PackratParsers {

  protected val AS = maximusKeyword("AS")
  protected val CREATE = maximusKeyword("CREATE")
  protected val DROP = maximusKeyword("DROP")
  protected val EXPLAIN = maximusKeyword("EXPLAIN")
  protected val INDEX = maximusKeyword("INDEX")
  protected val INDEXES = maximusKeyword("INDEXES")
  protected val MATERIALIZED = maximusKeyword("MATERIALIZED")
  protected val EXTENDED = maximusKeyword("EXTENDED")
  protected val ON = maximusKeyword("ON")
  protected val REFRESH = maximusKeyword("REFRESH")
  protected val SHOW = maximusKeyword("SHOW")
  protected val TABLE = maximusKeyword("TABLE")
  protected val VIEW = maximusKeyword("VIEW")
  protected val VIEWS = maximusKeyword("VIEWS")

  protected lazy val root: Parser[LogicalPlan] =
    ddlCommand | explainPlan

  protected lazy val ddlCommand: Parser[LogicalPlan] =
    indexCommands | mvCommands

  protected lazy val indexCommands: Parser[LogicalPlan] =
    createIndex | refreshIndex | dropIndex | showIndexes

  protected lazy val mvCommands: Parser[LogicalPlan] =
    createMV

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

  // For index
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
    REFRESH ~> INDEX ~> ident ~
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

  // For materialized view
  /**
   * CREATE MATERIALIZED VIEW mv_name
   * AS mv_query_statement
   */
  private lazy val createMV: Parser[LogicalPlan] =
    CREATE ~> MATERIALIZED ~> VIEW ~> (ident <~ ".").? ~ ident ~
      AS ~ query <~ opt(";") ^^ {
      case dbName ~ mvName ~ _ ~ query =>
        MaximusCreateMVCommand(dbName, mvName, query)
    }

  protected lazy val explainPlan: Parser[LogicalPlan] =
    (EXPLAIN ~> opt(EXTENDED)) ~ root ^^ {
      case modeStr ~ logicalPlan =>
        val mode = ExplainMode.fromString(modeStr.getOrElse("simple")) // TODO: support other modes
        val planToExplain = logicalPlan match {
          case cmd: MaximusCreateMVCommand =>
            cmd.buildStreamingJob(sparkSession).queryExecution.logical
          case otherPlan => otherPlan
        }
        ExplainCommand(planToExplain, mode)
    }

  // Returns the rest of the input string that are not parsed yet
  private lazy val query: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
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
