#pragma once

#include <string>
#include <vector>
#include <set>

namespace tinydb
{

  // Token types
  enum class TokenType
  {
    IDENTIFIER,
    KEYWORD,
    STRING_LITERAL,
    NUMBER_LITERAL,
    OPERATOR,
    PUNCTUATION,
    WHITESPACE,
    END_OF_FILE,
    UNKNOWN
  };

  // Token structure
  struct Token
  {
    TokenType type;
    std::string value;
    size_t line{1};
    size_t column{1};
  };

  /**
   * The Lexer class is responsible for tokenizing input strings into a sequence of tokens.
   * It is used in the parsing phase of SQL query processing.
   */
  class Lexer
  {
  public:
    explicit Lexer(const std::string &input);

    std::vector<Token> tokenize();

    bool had_error() const { return m_has_error_; }
    const std::string &error_message() const { return m_error_message_; }

  private:
    std::string m_input_;
    std::size_t m_position_{0};
    std::size_t m_line_{1};
    std::size_t m_column_{1};

    bool m_has_error_{false};
    std::string m_error_message_;
    std::set<std::string> m_keywords_{
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE",
        "CREATE", "DROP", "ALTER", "TABLE", "VALUES", "SET",
        "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
        "AS", "DISTINCT", "ORDER", "BY", "GROUP", "HAVING",
        "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT",
        "FULL", "ON", "USING", "UNION", "EXCEPT",
        "ALL", "ANY", "SOME", "EXISTS", "NULL", "TRUE", "FALSE",
        "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
        "INDEX", "AUTO_INCREMENT", "DEFAULT", "CHECK",
        "INTEGER", "INT", "TEXT", "VARCHAR", "CHAR", "BOOLEAN",
        "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIME", "TIMESTAMP"};

    char peek() const;
    char consume();

    Token readWord();
    Token readStringLiteral();
    Token readNumberLiteral();
    Token readOperator();
    Token readPunctuation();

    void skipWhitespace();
    bool isEnd() const;
    bool isPunctuation(char c) const;
    bool isOperator(char c) const;
  };
} // namespace tinydb