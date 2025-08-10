#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include "parser/lexer.h"

namespace tinydb
{

  // Forward declarations
  struct Expression;
  struct Statement;

  /**
   * Base class for all AST nodes
   */
  struct ASTNode
  {
    virtual ~ASTNode() = default;
  };

  /**
   * Base class for all SQL statements
   */
  struct Statement : public ASTNode
  {
    enum class Type
    {
      CREATE_TABLE,
      INSERT,
      SELECT,
      DELETE,
      UPDATE
    };

    virtual Type get_type() const = 0;
  };

  /**
   * Base class for all expressions
   */
  struct Expression : public ASTNode
  {
    enum class Type
    {
      LITERAL,
      IDENTIFIER,
      BINARY_OP,
      UNARY_OP
    };

    virtual Type get_type() const = 0;
  };

  /**
   * Column definition for CREATE TABLE
   */
  struct ColumnDefinition
  {
    std::string name;
    std::string type;
    bool is_nullable{true};
    bool is_primary_key{false};
  };

  /**
   * CREATE TABLE statement AST node
   */
  struct CreateTableStatement : public Statement
  {
    std::string table_name;
    std::vector<ColumnDefinition> columns;

    Type get_type() const override { return Type::CREATE_TABLE; }
  };

  /**
   * INSERT statement AST node
   */
  struct InsertStatement : public Statement
  {
    std::string table_name;
    std::vector<std::string> columns;                             // Optional column list
    std::vector<std::vector<std::unique_ptr<Expression>>> values; // Multiple rows

    Type get_type() const override { return Type::INSERT; }
  };

  /**
   * SELECT statement AST node
   */
  struct SelectStatement : public Statement
  {
    std::vector<std::unique_ptr<Expression>> select_list; // SELECT columns/expressions
    std::string from_table;                               // FROM table
    std::unique_ptr<Expression> where_clause;             // WHERE condition (optional)

    Type get_type() const override { return Type::SELECT; }
  };

  /**
   * DELETE statement AST node
   */
  struct DeleteStatement : public Statement
  {
    std::string table_name;
    std::unique_ptr<Expression> where_clause; // WHERE condition (optional)

    Type get_type() const override { return Type::DELETE; }
  };

  struct UpdateStatement : public Statement
  {
    std::string table_name;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> set_clauses; // column = value pairs
    std::unique_ptr<Expression> where_clause;                                     // WHERE condition (optional)

    Type get_type() const override { return Type::UPDATE; }
  };

  /**
   * Literal expression (string, number, boolean, null)
   */
  struct LiteralExpression : public Expression
  {
    enum class LiteralType
    {
      STRING,
      NUMBER,
      BOOLEAN,
      NULL_VALUE
    };

    LiteralType literal_type;
    std::string value;

    Type get_type() const override { return Type::LITERAL; }
  };

  /**
   * Identifier expression (column names, table names)
   */
  struct IdentifierExpression : public Expression
  {
    std::string name;

    Type get_type() const override { return Type::IDENTIFIER; }
  };

  /**
   * Binary operation expression (=, <, >, AND, OR, etc.)
   */
  struct BinaryOpExpression : public Expression
  {
    enum class Operator
    {
      EQUAL,
      NOT_EQUAL,
      LESS_THAN,
      LESS_EQUAL,
      GREATER_THAN,
      GREATER_EQUAL,
      AND,
      OR,
      PLUS,
      MINUS,
      MULTIPLY,
      DIVIDE
    };

    Operator op;
    std::unique_ptr<Expression> left;
    std::unique_ptr<Expression> right;

    Type get_type() const override { return Type::BINARY_OP; }
  };

  /**
   * Unary operation expression (NOT, -, etc.)
   */
  struct UnaryOpExpression : public Expression
  {
    enum class Operator
    {
      NOT,
      MINUS
    };

    Operator op;
    std::unique_ptr<Expression> operand;

    Type get_type() const override { return Type::UNARY_OP; }
  };

  /**
   * Parse result containing either a successful AST or error information
   */
  struct ParseResult
  {
    std::unique_ptr<Statement> statement;
    bool success{false};
    std::string error_message;
    size_t error_line{0};
    size_t error_column{0};
  };

  /**
   * The Parser class is responsible for parsing a sequence of tokens into an Abstract Syntax Tree (AST).
   * It implements a recursive descent parser for our SQL grammar.
   */
  class Parser
  {
  public:
    /**
     * Construct a parser with a vector of tokens
     */
    explicit Parser(std::vector<Token> tokens);

    /**
     * Parse the tokens into an AST
     */
    ParseResult parse();

  private:
    std::vector<Token> m_tokens_;
    size_t m_current_{0};
    std::string m_error_message_;
    size_t m_error_line_{0};
    size_t m_error_column_{0};

    // Token management
    const Token &current_token() const;
    const Token &peek_token(size_t offset = 1) const;
    bool is_at_end() const;
    void advance();
    bool match(TokenType type);
    bool match(TokenType type, const std::string &value);
    bool consume(TokenType type, const std::string &error_message);
    bool consume(TokenType type, const std::string &value, const std::string &error_message);
    bool expect(TokenType type) const;
    bool expect(TokenType type, const std::string &value) const;

    // Error handling
    void set_error(const std::string &message);
    void set_error(const std::string &message, const Token &token);

    // Statement parsing
    std::unique_ptr<Statement> parse_statement();
    std::unique_ptr<CreateTableStatement> parse_create_table();
    std::unique_ptr<InsertStatement> parse_insert();
    std::unique_ptr<SelectStatement> parse_select();
    std::unique_ptr<DeleteStatement> parse_delete();
    std::unique_ptr<UpdateStatement> parse_update();

    // Expression parsing (recursive descent with precedence)
    std::unique_ptr<Expression> parse_expression();
    std::unique_ptr<Expression> parse_or_expression();
    std::unique_ptr<Expression> parse_and_expression();
    std::unique_ptr<Expression> parse_equality_expression();
    std::unique_ptr<Expression> parse_comparison_expression();
    std::unique_ptr<Expression> parse_term_expression();
    std::unique_ptr<Expression> parse_factor_expression();
    std::unique_ptr<Expression> parse_unary_expression();
    std::unique_ptr<Expression> parse_primary_expression();

    // Helper methods
    ColumnDefinition parse_column_definition();
    std::vector<std::unique_ptr<Expression>> parse_expression_list();
    std::vector<std::string> parse_identifier_list();

    // Utility methods
    BinaryOpExpression::Operator token_to_binary_operator(const Token &token);
    UnaryOpExpression::Operator token_to_unary_operator(const Token &token);
    bool is_comparison_operator(const Token &token);
    bool is_equality_operator(const Token &token);
  };

} // namespace tinydb