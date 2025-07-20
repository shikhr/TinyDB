#include "parser/parser.h"
#include <algorithm>
#include <stdexcept>

namespace tinydb
{

  Parser::Parser(std::vector<Token> tokens) : m_tokens_(std::move(tokens))
  {
    // Remove whitespace tokens for easier parsing
    m_tokens_.erase(
        std::remove_if(m_tokens_.begin(), m_tokens_.end(),
                       [](const Token &token)
                       { return token.type == TokenType::WHITESPACE; }),
        m_tokens_.end());
  }

  ParseResult Parser::parse()
  {
    ParseResult result;
    m_current_ = 0;
    m_error_message_.clear();
    m_error_line_ = 0;
    m_error_column_ = 0;

    try
    {
      auto statement = parse_statement();
      if (statement && m_error_message_.empty())
      {
        result.statement = std::move(statement);
        result.success = true;
      }
      else
      {
        result.success = false;
        result.error_message = m_error_message_;
        result.error_line = m_error_line_;
        result.error_column = m_error_column_;
      }
    }
    catch (const std::exception &e)
    {
      result.success = false;
      result.error_message = e.what();
      result.error_line = m_error_line_;
      result.error_column = m_error_column_;
    }

    return result;
  }

  const Token &Parser::current_token() const
  {
    if (m_current_ >= m_tokens_.size())
    {
      static const Token eof_token{TokenType::END_OF_FILE, "", 0, 0};
      return eof_token;
    }
    return m_tokens_[m_current_];
  }

  const Token &Parser::peek_token(size_t offset) const
  {
    size_t index = m_current_ + offset;
    if (index >= m_tokens_.size())
    {
      static const Token eof_token{TokenType::END_OF_FILE, "", 0, 0};
      return eof_token;
    }
    return m_tokens_[index];
  }

  bool Parser::is_at_end() const
  {
    return m_current_ >= m_tokens_.size() || current_token().type == TokenType::END_OF_FILE;
  }

  void Parser::advance()
  {
    if (!is_at_end())
    {
      ++m_current_;
    }
  }

  bool Parser::match(TokenType type)
  {
    if (current_token().type == type)
    {
      advance();
      return true;
    }
    return false;
  }

  bool Parser::match(TokenType type, const std::string &value)
  {
    if (current_token().type == type && current_token().value == value)
    {
      advance();
      return true;
    }
    return false;
  }

  bool Parser::consume(TokenType type, const std::string &error_message)
  {
    if (current_token().type == type)
    {
      advance();
      return true;
    }
    set_error(error_message, current_token());
    return false;
  }

  bool Parser::consume(TokenType type, const std::string &value, const std::string &error_message)
  {
    if (current_token().type == type && current_token().value == value)
    {
      advance();
      return true;
    }
    set_error(error_message, current_token());
    return false;
  }

  bool Parser::expect(TokenType type) const
  {
    return current_token().type == type;
  }

  bool Parser::expect(TokenType type, const std::string &value) const
  {
    return current_token().type == type && current_token().value == value;
  }

  void Parser::set_error(const std::string &message)
  {
    m_error_message_ = message;
    const auto &token = current_token();
    m_error_line_ = token.line;
    m_error_column_ = token.column;
  }

  void Parser::set_error(const std::string &message, const Token &token)
  {
    m_error_message_ = message;
    m_error_line_ = token.line;
    m_error_column_ = token.column;
  }

  std::unique_ptr<Statement> Parser::parse_statement()
  {
    if (is_at_end())
    {
      set_error("Unexpected end of input");
      return nullptr;
    }

    const Token &token = current_token();
    if (token.type == TokenType::KEYWORD)
    {
      std::string keyword = token.value;
      std::transform(keyword.begin(), keyword.end(), keyword.begin(), ::toupper);

      if (keyword == "CREATE")
      {
        return parse_create_table();
      }
      else if (keyword == "INSERT")
      {
        return parse_insert();
      }
      else if (keyword == "SELECT")
      {
        return parse_select();
      }
      else if (keyword == "DELETE")
      {
        return parse_delete();
      }
      else if (keyword == "UPDATE")
      {
        set_error("UPDATE statement is not yet implemented");
        return nullptr;
      }
      else
      {
        set_error("Unsupported SQL statement: " + keyword);
        return nullptr;
      }
    }

    set_error("Expected SQL statement");
    return nullptr;
  }

  std::unique_ptr<CreateTableStatement> Parser::parse_create_table()
  {
    auto stmt = std::make_unique<CreateTableStatement>();

    // Consume CREATE keyword
    if (!consume(TokenType::KEYWORD, "Expected 'CREATE'"))
      return nullptr;

    // Consume TABLE keyword
    if (!match(TokenType::KEYWORD, "TABLE"))
    {
      set_error("Expected 'TABLE' after 'CREATE'");
      return nullptr;
    }

    // Get table name
    if (!expect(TokenType::IDENTIFIER))
    {
      set_error("Expected table name");
      return nullptr;
    }
    stmt->table_name = current_token().value;
    advance();

    // Consume opening parenthesis
    if (!consume(TokenType::PUNCTUATION, "(", "Expected '(' after table name"))
      return nullptr;

    // Parse column definitions
    do
    {
      auto column = parse_column_definition();
      if (m_error_message_.empty())
      {
        stmt->columns.push_back(std::move(column));
      }
      else
      {
        return nullptr;
      }

      if (match(TokenType::PUNCTUATION, ","))
      {
        continue; // More columns
      }
      else
      {
        break; // End of column list
      }
    } while (!is_at_end());

    // Consume closing parenthesis
    if (!consume(TokenType::PUNCTUATION, ")", "Expected ')' after column definitions"))
      return nullptr;

    return stmt;
  }

  std::unique_ptr<InsertStatement> Parser::parse_insert()
  {
    auto stmt = std::make_unique<InsertStatement>();

    // Consume INSERT keyword
    if (!consume(TokenType::KEYWORD, "Expected 'INSERT'"))
      return nullptr;

    // Consume INTO keyword
    if (!match(TokenType::KEYWORD, "INTO"))
    {
      set_error("Expected 'INTO' after 'INSERT'");
      return nullptr;
    }

    // Get table name
    if (!expect(TokenType::IDENTIFIER))
    {
      set_error("Expected table name");
      return nullptr;
    }
    stmt->table_name = current_token().value;
    advance();

    // Mandatory column list (PostgreSQL style)
    if (!consume(TokenType::PUNCTUATION, "(", "Expected '(' before column list"))
      return nullptr;

    stmt->columns = parse_identifier_list();
    if (!consume(TokenType::PUNCTUATION, ")", "Expected ')' after column list"))
      return nullptr;

    // Consume VALUES keyword
    if (!match(TokenType::KEYWORD, "VALUES"))
    {
      set_error("Expected 'VALUES'");
      return nullptr;
    }

    // Parse value lists
    do
    {
      if (!consume(TokenType::PUNCTUATION, "(", "Expected '(' before values"))
        return nullptr;

      auto values = parse_expression_list();
      if (m_error_message_.empty())
      {
        stmt->values.push_back(std::move(values));
      }
      else
      {
        return nullptr;
      }

      if (!consume(TokenType::PUNCTUATION, ")", "Expected ')' after values"))
        return nullptr;

      if (match(TokenType::PUNCTUATION, ","))
      {
        continue; // More value lists
      }
      else
      {
        break; // End of value lists
      }
    } while (!is_at_end());

    return stmt;
  }

  std::unique_ptr<SelectStatement> Parser::parse_select()
  {
    auto stmt = std::make_unique<SelectStatement>();

    // Consume SELECT keyword
    if (!consume(TokenType::KEYWORD, "Expected 'SELECT'"))
      return nullptr;

    // Parse select list
    stmt->select_list = parse_expression_list();
    if (!m_error_message_.empty())
      return nullptr;

    // Consume FROM keyword
    if (!match(TokenType::KEYWORD, "FROM"))
    {
      set_error("Expected 'FROM'");
      return nullptr;
    }

    // Get table name
    if (current_token().type != TokenType::IDENTIFIER)
    {
      set_error("Expected table name after 'FROM'");
      return nullptr;
    }
    stmt->from_table = current_token().value;
    advance();

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE"))
    {
      stmt->where_clause = parse_expression();
      if (!stmt->where_clause)
        return nullptr;
    }

    return stmt;
  }

  std::unique_ptr<DeleteStatement> Parser::parse_delete()
  {
    auto stmt = std::make_unique<DeleteStatement>();

    // Consume DELETE keyword
    if (!consume(TokenType::KEYWORD, "Expected 'DELETE'"))
      return nullptr;

    // Consume FROM keyword
    if (!match(TokenType::KEYWORD, "FROM"))
    {
      set_error("Expected 'FROM' after 'DELETE'");
      return nullptr;
    }

    // Get table name
    if (current_token().type != TokenType::IDENTIFIER)
    {
      set_error("Expected table name");
      return nullptr;
    }
    stmt->table_name = current_token().value;
    advance();

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE"))
    {
      stmt->where_clause = parse_expression();
      if (!stmt->where_clause)
        return nullptr;
    }

    return stmt;
  }

  std::unique_ptr<Expression> Parser::parse_expression()
  {
    return parse_or_expression();
  }

  std::unique_ptr<Expression> Parser::parse_or_expression()
  {
    auto expr = parse_and_expression();
    if (!expr)
      return nullptr;

    while (match(TokenType::KEYWORD, "OR"))
    {
      auto right = parse_and_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = BinaryOpExpression::Operator::OR;
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_and_expression()
  {
    auto expr = parse_equality_expression();
    if (!expr)
      return nullptr;

    while (match(TokenType::KEYWORD, "AND"))
    {
      auto right = parse_equality_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = BinaryOpExpression::Operator::AND;
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_equality_expression()
  {
    auto expr = parse_comparison_expression();
    if (!expr)
      return nullptr;

    while (is_equality_operator(current_token()))
    {
      auto op_token = current_token();
      advance();
      auto right = parse_comparison_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = token_to_binary_operator(op_token);
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_comparison_expression()
  {
    auto expr = parse_term_expression();
    if (!expr)
      return nullptr;

    while (is_comparison_operator(current_token()))
    {
      auto op_token = current_token();
      advance();
      auto right = parse_term_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = token_to_binary_operator(op_token);
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_term_expression()
  {
    auto expr = parse_factor_expression();
    if (!expr)
      return nullptr;

    while (current_token().value == "+" || current_token().value == "-")
    {
      auto op_token = current_token();
      advance();
      auto right = parse_factor_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = token_to_binary_operator(op_token);
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_factor_expression()
  {
    auto expr = parse_unary_expression();
    if (!expr)
      return nullptr;

    while (current_token().value == "*" || current_token().value == "/")
    {
      auto op_token = current_token();
      advance();
      auto right = parse_unary_expression();
      if (!right)
        return nullptr;

      auto binary_op = std::make_unique<BinaryOpExpression>();
      binary_op->op = token_to_binary_operator(op_token);
      binary_op->left = std::move(expr);
      binary_op->right = std::move(right);
      expr = std::move(binary_op);
    }

    return expr;
  }

  std::unique_ptr<Expression> Parser::parse_unary_expression()
  {
    if (current_token().value == "NOT" || current_token().value == "-")
    {
      auto op_token = current_token();
      advance();
      auto operand = parse_unary_expression();
      if (!operand)
        return nullptr;

      auto unary_op = std::make_unique<UnaryOpExpression>();
      unary_op->op = token_to_unary_operator(op_token);
      unary_op->operand = std::move(operand);
      return unary_op;
    }

    return parse_primary_expression();
  }

  std::unique_ptr<Expression> Parser::parse_primary_expression()
  {
    const Token &token = current_token();

    if (token.type == TokenType::IDENTIFIER)
    {
      auto identifier = std::make_unique<IdentifierExpression>();
      identifier->name = token.value;
      advance();
      return identifier;
    }
    else if (token.type == TokenType::NUMBER_LITERAL)
    {
      auto literal = std::make_unique<LiteralExpression>();
      literal->literal_type = LiteralExpression::LiteralType::NUMBER;
      literal->value = token.value;
      advance();
      return literal;
    }
    else if (token.type == TokenType::STRING_LITERAL)
    {
      auto literal = std::make_unique<LiteralExpression>();
      literal->literal_type = LiteralExpression::LiteralType::STRING;
      literal->value = token.value;
      advance();
      return literal;
    }
    else if (token.type == TokenType::KEYWORD)
    {
      std::string keyword = token.value;
      std::transform(keyword.begin(), keyword.end(), keyword.begin(), ::toupper);

      if (keyword == "NULL")
      {
        auto literal = std::make_unique<LiteralExpression>();
        literal->literal_type = LiteralExpression::LiteralType::NULL_VALUE;
        literal->value = "NULL";
        advance();
        return literal;
      }
      else if (keyword == "TRUE" || keyword == "FALSE")
      {
        auto literal = std::make_unique<LiteralExpression>();
        literal->literal_type = LiteralExpression::LiteralType::BOOLEAN;
        literal->value = keyword;
        advance();
        return literal;
      }
      else if (keyword == "*")
      {
        // Handle SELECT * case
        auto identifier = std::make_unique<IdentifierExpression>();
        identifier->name = "*";
        advance();
        return identifier;
      }
    }
    else if (token.value == "*")
    {
      // Handle * as identifier for SELECT *
      auto identifier = std::make_unique<IdentifierExpression>();
      identifier->name = "*";
      advance();
      return identifier;
    }
    else if (match(TokenType::PUNCTUATION, "("))
    {
      auto expr = parse_expression();
      if (!expr)
        return nullptr;

      if (!consume(TokenType::PUNCTUATION, ")", "Expected ')' after expression"))
        return nullptr;

      return expr;
    }

    set_error("Expected expression");
    return nullptr;
  }

  ColumnDefinition Parser::parse_column_definition()
  {
    ColumnDefinition column;

    // Column name
    if (current_token().type != TokenType::IDENTIFIER)
    {
      set_error("Expected column name");
      return column;
    }
    column.name = current_token().value;
    advance();

    // Column type (can be IDENTIFIER or KEYWORD for built-in types)
    if (current_token().type != TokenType::IDENTIFIER && current_token().type != TokenType::KEYWORD)
    {
      set_error("Expected column type");
      return column;
    }
    column.type = current_token().value;
    advance();

    // Optional constraints
    while (current_token().type == TokenType::KEYWORD)
    {
      std::string keyword = current_token().value;
      std::transform(keyword.begin(), keyword.end(), keyword.begin(), ::toupper);

      if (keyword == "NOT")
      {
        advance();
        if (match(TokenType::KEYWORD, "NULL"))
        {
          column.is_nullable = false;
        }
        else
        {
          set_error("Expected 'NULL' after 'NOT'");
          return column;
        }
      }
      else if (keyword == "PRIMARY")
      {
        advance();
        if (match(TokenType::KEYWORD, "KEY"))
        {
          column.is_primary_key = true;
          column.is_nullable = false; // Primary key implies NOT NULL
        }
        else
        {
          set_error("Expected 'KEY' after 'PRIMARY'");
          return column;
        }
      }
      else
      {
        break; // Unknown keyword, stop parsing constraints
      }
    }

    return column;
  }

  std::vector<std::unique_ptr<Expression>> Parser::parse_expression_list()
  {
    std::vector<std::unique_ptr<Expression>> expressions;

    do
    {
      auto expr = parse_expression();
      if (!expr)
      {
        return expressions; // Error will be set by parse_expression
      }
      expressions.push_back(std::move(expr));

      if (match(TokenType::PUNCTUATION, ","))
      {
        continue; // More expressions
      }
      else
      {
        break; // End of expression list
      }
    } while (!is_at_end());

    return expressions;
  }

  std::vector<std::string> Parser::parse_identifier_list()
  {
    std::vector<std::string> identifiers;

    do
    {
      if (current_token().type != TokenType::IDENTIFIER)
      {
        set_error("Expected identifier");
        return identifiers;
      }
      identifiers.push_back(current_token().value);
      advance();

      if (match(TokenType::PUNCTUATION, ","))
      {
        continue; // More identifiers
      }
      else
      {
        break; // End of identifier list
      }
    } while (!is_at_end());

    return identifiers;
  }

  BinaryOpExpression::Operator Parser::token_to_binary_operator(const Token &token)
  {
    if (token.value == "=")
      return BinaryOpExpression::Operator::EQUAL;
    else if (token.value == "!=" || token.value == "<>")
      return BinaryOpExpression::Operator::NOT_EQUAL;
    else if (token.value == "<")
      return BinaryOpExpression::Operator::LESS_THAN;
    else if (token.value == "<=")
      return BinaryOpExpression::Operator::LESS_EQUAL;
    else if (token.value == ">")
      return BinaryOpExpression::Operator::GREATER_THAN;
    else if (token.value == ">=")
      return BinaryOpExpression::Operator::GREATER_EQUAL;
    else if (token.value == "AND")
      return BinaryOpExpression::Operator::AND;
    else if (token.value == "OR")
      return BinaryOpExpression::Operator::OR;
    else if (token.value == "+")
      return BinaryOpExpression::Operator::PLUS;
    else if (token.value == "-")
      return BinaryOpExpression::Operator::MINUS;
    else if (token.value == "*")
      return BinaryOpExpression::Operator::MULTIPLY;
    else if (token.value == "/")
      return BinaryOpExpression::Operator::DIVIDE;

    throw std::runtime_error("Unknown binary operator: " + token.value);
  }

  UnaryOpExpression::Operator Parser::token_to_unary_operator(const Token &token)
  {
    if (token.value == "NOT")
      return UnaryOpExpression::Operator::NOT;
    else if (token.value == "-")
      return UnaryOpExpression::Operator::MINUS;

    throw std::runtime_error("Unknown unary operator: " + token.value);
  }

  bool Parser::is_comparison_operator(const Token &token)
  {
    return token.value == "<" || token.value == "<=" ||
           token.value == ">" || token.value == ">=";
  }

  bool Parser::is_equality_operator(const Token &token)
  {
    return token.value == "=" || token.value == "!=" || token.value == "<>";
  }

} // namespace tinydb
