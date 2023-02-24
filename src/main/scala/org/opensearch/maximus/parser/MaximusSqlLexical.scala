package org.opensearch.maximus.parser

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.input.CharArrayReader.EofCh

class MaximusSqlLexical extends StdLexical {
  case class FloatLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string */
  def normalizeKeyword(str: String): String = str.toLowerCase()

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    ( identChar ~ (identChar | digit).* ^^
      { case first ~ rest => processIdent((first :: rest).mkString) }
      | digit.* ~ identChar ~ (identChar | digit).* ^^
      { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
      | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
      case i ~ None => NumericLit(i.mkString)
      case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
    }
      | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
      | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
      | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )

  override def identChar: Parser[Elem] = letter | elem('_')

  override def whitespace: Parser[Any] =
    ( whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
      | '#' ~ chrExcept(EofCh, '\n').*
      | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
      | '/' ~ '*' ~ failure("unclosed comment")
      ).*

}
