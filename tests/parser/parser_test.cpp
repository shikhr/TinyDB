#include <catch2/catch_all.hpp>
#include "parser/parser.h"
#include "parser/lexer.h"

namespace tinydb
{

  TEST_CASE("Parser - CREATE TABLE statement", "[parser]")
  {
    SECTION("Basic CREATE TABLE")
    {
      std::string sql = "CREATE TABLE users (id INTEGER, name TEXT)";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      REQUIRE(result.statement != nullptr);
      REQUIRE(result.statement->get_type() == Statement::Type::CREATE_TABLE);

      auto create_stmt = static_cast<CreateTableStatement *>(result.statement.get());
      REQUIRE(create_stmt->table_name == "users");
      REQUIRE(create_stmt->columns.size() == 2);
      REQUIRE(create_stmt->columns[0].name == "id");
      REQUIRE(create_stmt->columns[0].type == "INTEGER");
      REQUIRE(create_stmt->columns[1].name == "name");
      REQUIRE(create_stmt->columns[1].type == "TEXT");
    }

    SECTION("CREATE TABLE with constraints")
    {
      std::string sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto create_stmt = static_cast<CreateTableStatement *>(result.statement.get());
      REQUIRE(create_stmt->columns[0].is_primary_key);
      REQUIRE_FALSE(create_stmt->columns[0].is_nullable);
      REQUIRE_FALSE(create_stmt->columns[1].is_nullable);
    }
  }

  TEST_CASE("Parser - INSERT statement", "[parser]")
  {
    SECTION("Basic INSERT")
    {
      std::string sql = "INSERT INTO users (id, name) VALUES (1, \"John\")";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      REQUIRE(result.statement->get_type() == Statement::Type::INSERT);

      auto insert_stmt = static_cast<InsertStatement *>(result.statement.get());
      REQUIRE(insert_stmt->table_name == "users");
      REQUIRE(insert_stmt->columns.size() == 2);
      REQUIRE(insert_stmt->columns[0] == "id");
      REQUIRE(insert_stmt->columns[1] == "name");
      REQUIRE(insert_stmt->values.size() == 1);
      REQUIRE(insert_stmt->values[0].size() == 2);
    }

    SECTION("INSERT with column list")
    {
      std::string sql = "INSERT INTO users (id, name) VALUES (1, \"John\")";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto insert_stmt = static_cast<InsertStatement *>(result.statement.get());
      REQUIRE(insert_stmt->columns.size() == 2);
      REQUIRE(insert_stmt->columns[0] == "id");
      REQUIRE(insert_stmt->columns[1] == "name");
    }

    SECTION("INSERT multiple rows")
    {
      std::string sql = "INSERT INTO users (id, name) VALUES (1, \"John\"), (2, \"Jane\")";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto insert_stmt = static_cast<InsertStatement *>(result.statement.get());
      REQUIRE(insert_stmt->columns.size() == 2);
      REQUIRE(insert_stmt->values.size() == 2);
    }

    SECTION("INSERT without column list should fail")
    {
      std::string sql = "INSERT INTO users VALUES (1, \"John\")";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Expected '(' before column list") != std::string::npos);
    }
  }

  TEST_CASE("Parser - SELECT statement", "[parser]")
  {
    SECTION("SELECT *")
    {
      std::string sql = "SELECT * FROM users";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      REQUIRE(result.statement->get_type() == Statement::Type::SELECT);

      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->from_table == "users");
      REQUIRE(select_stmt->select_list.size() == 1);

      auto identifier = static_cast<IdentifierExpression *>(select_stmt->select_list[0].get());
      REQUIRE(identifier->name == "*");
    }

    SECTION("SELECT with column list")
    {
      std::string sql = "SELECT id, name FROM users";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->select_list.size() == 2);
    }

    SECTION("SELECT with WHERE clause")
    {
      std::string sql = "SELECT * FROM users WHERE id = 1";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->where_clause != nullptr);
      REQUIRE(select_stmt->where_clause->get_type() == Expression::Type::BINARY_OP);

      auto binary_op = static_cast<BinaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(binary_op->op == BinaryOpExpression::Operator::EQUAL);
    }

    SECTION("SELECT with complex WHERE clause")
    {
      std::string sql = "SELECT * FROM users WHERE id > 1 AND name = \"John\"";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->where_clause != nullptr);

      auto and_op = static_cast<BinaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(and_op->op == BinaryOpExpression::Operator::AND);
    }
  }

  TEST_CASE("Parser - DELETE statement", "[parser]")
  {
    SECTION("DELETE without WHERE")
    {
      std::string sql = "DELETE FROM users";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      REQUIRE(result.statement->get_type() == Statement::Type::DELETE);

      auto delete_stmt = static_cast<DeleteStatement *>(result.statement.get());
      REQUIRE(delete_stmt->table_name == "users");
      REQUIRE(delete_stmt->where_clause == nullptr);
    }

    SECTION("DELETE with WHERE")
    {
      std::string sql = "DELETE FROM users WHERE id = 1";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto delete_stmt = static_cast<DeleteStatement *>(result.statement.get());
      REQUIRE(delete_stmt->where_clause != nullptr);
    }
  }

  TEST_CASE("Parser - UPDATE statement", "[parser]")
  {
    SECTION("Basic UPDATE with WHERE")
    {
      std::string sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      REQUIRE(result.statement->get_type() == Statement::Type::UPDATE);

      auto update_stmt = static_cast<UpdateStatement *>(result.statement.get());
      REQUIRE(update_stmt->table_name == "users");
      REQUIRE(update_stmt->set_clauses.size() == 1);
      REQUIRE(update_stmt->set_clauses[0].first == "name");
      auto lit = dynamic_cast<LiteralExpression *>(update_stmt->set_clauses[0].second.get());
      REQUIRE(lit != nullptr);
      REQUIRE(update_stmt->where_clause != nullptr);
      auto where_bin = dynamic_cast<BinaryOpExpression *>(update_stmt->where_clause.get());
      REQUIRE(where_bin != nullptr);
      REQUIRE(where_bin->op == BinaryOpExpression::Operator::EQUAL);
    }

    SECTION("UPDATE multiple SET clauses")
    {
      std::string sql = "UPDATE users SET name = 'Jane', id = 3 WHERE name = 'Alice'";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto update_stmt = static_cast<UpdateStatement *>(result.statement.get());
      REQUIRE(update_stmt->set_clauses.size() == 2);
    }

    SECTION("UPDATE without WHERE")
    {
      std::string sql = "UPDATE users SET name = 'Zed'";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto update_stmt = static_cast<UpdateStatement *>(result.statement.get());
      REQUIRE(update_stmt->where_clause == nullptr);
    }

    SECTION("UPDATE missing SET should fail")
    {
      std::string sql = "UPDATE users name = 'Bob'"; // Missing SET keyword
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Expected 'SET'") != std::string::npos);
    }

    SECTION("UPDATE invalid SET clause should fail")
    {
      std::string sql = "UPDATE users SET = 1"; // Missing column name
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Expected column name in SET clause") != std::string::npos);
    }
  }

  TEST_CASE("Parser - Expression parsing", "[parser]")
  {
    SECTION("Literal expressions")
    {
      std::string sql = "SELECT 42, \"hello\", TRUE, NULL FROM users";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->select_list.size() == 4);

      // Number literal
      auto num_literal = static_cast<LiteralExpression *>(select_stmt->select_list[0].get());
      REQUIRE(num_literal->literal_type == LiteralExpression::LiteralType::NUMBER);
      REQUIRE(num_literal->value == "42");

      // String literal
      auto str_literal = static_cast<LiteralExpression *>(select_stmt->select_list[1].get());
      REQUIRE(str_literal->literal_type == LiteralExpression::LiteralType::STRING);

      // Boolean literal
      auto bool_literal = static_cast<LiteralExpression *>(select_stmt->select_list[2].get());
      REQUIRE(bool_literal->literal_type == LiteralExpression::LiteralType::BOOLEAN);

      // NULL literal
      auto null_literal = static_cast<LiteralExpression *>(select_stmt->select_list[3].get());
      REQUIRE(null_literal->literal_type == LiteralExpression::LiteralType::NULL_VALUE);
    }

    SECTION("Binary operations")
    {
      std::string sql = "SELECT * FROM users WHERE age >= 18 AND name != \"admin\"";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());

      auto and_op = static_cast<BinaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(and_op->op == BinaryOpExpression::Operator::AND);

      auto ge_op = static_cast<BinaryOpExpression *>(and_op->left.get());
      REQUIRE(ge_op->op == BinaryOpExpression::Operator::GREATER_EQUAL);

      auto ne_op = static_cast<BinaryOpExpression *>(and_op->right.get());
      REQUIRE(ne_op->op == BinaryOpExpression::Operator::NOT_EQUAL);
    }

    SECTION("Unary operations")
    {
      std::string sql = "SELECT * FROM users WHERE NOT active";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());

      auto not_op = static_cast<UnaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(not_op->op == UnaryOpExpression::Operator::NOT);
    }

    SECTION("Parenthesized expressions")
    {
      std::string sql = "SELECT * FROM users WHERE (age > 18) AND (status = \"active\")";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());
      REQUIRE(select_stmt->where_clause->get_type() == Expression::Type::BINARY_OP);
    }
  }

  TEST_CASE("Parser - Error handling", "[parser]")
  {
    SECTION("Syntax error")
    {
      std::string sql = "SELECT FROM users"; // Missing column list
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE_FALSE(result.error_message.empty());
    }

    SECTION("Invalid statement")
    {
      std::string sql = "INVALID STATEMENT";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE_FALSE(result.error_message.empty());
    }

    SECTION("Incomplete CREATE TABLE")
    {
      std::string sql = "CREATE TABLE users ("; // Missing closing parenthesis
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE_FALSE(result.success);
      REQUIRE_FALSE(result.error_message.empty());
    }
  }

  TEST_CASE("Parser - Operator precedence", "[parser]")
  {
    SECTION("Arithmetic precedence")
    {
      std::string sql = "SELECT * FROM users WHERE age + 5 * 2 > 30";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());

      // Should parse as: age + (5 * 2) > 30
      auto gt_op = static_cast<BinaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(gt_op->op == BinaryOpExpression::Operator::GREATER_THAN);

      auto plus_op = static_cast<BinaryOpExpression *>(gt_op->left.get());
      REQUIRE(plus_op->op == BinaryOpExpression::Operator::PLUS);

      auto mult_op = static_cast<BinaryOpExpression *>(plus_op->right.get());
      REQUIRE(mult_op->op == BinaryOpExpression::Operator::MULTIPLY);
    }

    SECTION("Logical precedence")
    {
      std::string sql = "SELECT * FROM users WHERE age > 18 AND active OR status = \"premium\"";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      REQUIRE(result.success);
      auto select_stmt = static_cast<SelectStatement *>(result.statement.get());

      // Should parse as: (age > 18 AND active) OR (status = "premium")
      auto or_op = static_cast<BinaryOpExpression *>(select_stmt->where_clause.get());
      REQUIRE(or_op->op == BinaryOpExpression::Operator::OR);

      auto and_op = static_cast<BinaryOpExpression *>(or_op->left.get());
      REQUIRE(and_op->op == BinaryOpExpression::Operator::AND);
    }
  }

  TEST_CASE("Parser - expect function", "[parser]")
  {
    SECTION("expect without advancing token")
    {
      std::string sql = "CREATE TABLE users";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));

      auto result = parser.parse();

      // This should fail because there's no opening parenthesis after table name
      REQUIRE_FALSE(result.success);
      REQUIRE_FALSE(result.error_message.empty());
    }

    SECTION("expect with value parameter")
    {
      std::string sql = "CREATE TABLE";
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      REQUIRE_FALSE(lexer.had_error());

      Parser parser(std::move(tokens));
      auto result = parser.parse();

      // Should fail because no table name is provided
      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Expected table name") != std::string::npos);
    }
  }

} // namespace tinydb
