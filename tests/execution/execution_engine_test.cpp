#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <memory>

#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "parser/lexer.h"
#include "catalog/catalog.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"

namespace tinydb
{

  class ExecutionEngineTestFixture
  {
  public:
    ExecutionEngineTestFixture()
    {
      // Create a temporary database file
      temp_db_file_ = std::filesystem::temp_directory_path() / "test_execution_engine.db";
      std::filesystem::remove(temp_db_file_); // Remove if exists

      // Create the storage stack
      disk_manager_ = std::make_unique<DiskManager>(temp_db_file_);
      buffer_pool_ = std::make_unique<BufferPoolManager>(10, disk_manager_.get());
      catalog_ = std::make_unique<Catalog>(buffer_pool_.get());
      execution_engine_ = std::make_unique<ExecutionEngine>(catalog_.get());
    }

    ~ExecutionEngineTestFixture()
    {
      execution_engine_.reset();
      catalog_.reset();
      buffer_pool_.reset();
      disk_manager_.reset();

      // Clean up temporary file
      std::filesystem::remove(temp_db_file_);
    }

    ParseResult parse_sql(const std::string &sql)
    {
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      if (lexer.had_error())
      {
        ParseResult result;
        result.success = false;
        result.error_message = "Lexer error: " + lexer.error_message();
        return result;
      }

      Parser parser(std::move(tokens));
      return parser.parse();
    }

    ExecutionResult execute_sql(const std::string &sql)
    {
      auto parse_result = parse_sql(sql);
      if (!parse_result.success)
      {
        ExecutionResult result;
        result.success = false;
        result.error_message = "Parse error: " + parse_result.error_message;
        return result;
      }

      return execution_engine_->execute(*parse_result.statement);
    }

  protected:
    std::filesystem::path temp_db_file_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<ExecutionEngine> execution_engine_;
  };

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Create Table", "[execution_engine]")
  {
    SECTION("Create simple table")
    {
      std::string sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 0);

      // Verify table exists in catalog
      auto table = catalog_->get_table("users");
      REQUIRE(table != nullptr);

      auto schema = catalog_->get_schema("users");
      REQUIRE(schema != nullptr);
      REQUIRE(schema->get_column_count() == 2);
      REQUIRE(schema->get_column(0).get_name() == "id");
      REQUIRE(schema->get_column(0).get_type() == ColumnType::INTEGER);
      REQUIRE(schema->get_column(1).get_name() == "name");
      REQUIRE(schema->get_column(1).get_type() == ColumnType::VARCHAR);
    }

    SECTION("Create table with invalid type")
    {
      std::string sql = "CREATE TABLE test (id INVALID_TYPE)";
      auto result = execute_sql(sql);

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Invalid column type") != std::string::npos);
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Insert Records", "[execution_engine]")
  {
    // First create a table
    auto create_result = execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR)");
    REQUIRE(create_result.success);

    SECTION("Insert single record")
    {
      std::string sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
    }

    SECTION("Insert multiple records")
    {
      std::string sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 2);
    }

    SECTION("Insert with column specification")
    {
      std::string sql = "INSERT INTO users (name, id) VALUES ('Charlie', 3)";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
    }

    SECTION("Insert into non-existent table")
    {
      std::string sql = "INSERT INTO nonexistent (id, name) VALUES (1, 'test')";
      auto result = execute_sql(sql);

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Table does not exist") != std::string::npos);
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Select Records", "[execution_engine]")
  {
    // Setup: Create table and insert data
    REQUIRE(execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").success);
    REQUIRE(execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").success);

    SECTION("Select all records")
    {
      std::string sql = "SELECT * FROM users";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 3);
      REQUIRE(result.column_names.size() == 2);
      REQUIRE(result.column_names[0] == "id");
      REQUIRE(result.column_names[1] == "name");
      REQUIRE(result.result_rows.size() == 3);

      // Check first row
      REQUIRE(result.result_rows[0].size() == 2);
      REQUIRE(result.result_rows[0][0].get_type() == ColumnType::INTEGER);
      REQUIRE(result.result_rows[0][0].get_integer() == 1);
      REQUIRE(result.result_rows[0][1].get_type() == ColumnType::VARCHAR);
      REQUIRE(result.result_rows[0][1].get_string() == "Alice");
    }

    SECTION("Select with WHERE clause")
    {
      std::string sql = "SELECT * FROM users WHERE id = 2";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
      REQUIRE(result.result_rows.size() == 1);
      REQUIRE(result.result_rows[0][0].get_integer() == 2);
      REQUIRE(result.result_rows[0][1].get_string() == "Bob");
    }

    SECTION("Select with string comparison")
    {
      std::string sql = "SELECT * FROM users WHERE name = 'Charlie'";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
      REQUIRE(result.result_rows.size() == 1);
      REQUIRE(result.result_rows[0][0].get_integer() == 3);
      REQUIRE(result.result_rows[0][1].get_string() == "Charlie");
    }

    SECTION("Select from non-existent table")
    {
      std::string sql = "SELECT * FROM nonexistent";
      auto result = execute_sql(sql);

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Table does not exist") != std::string::npos);
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Delete Records", "[execution_engine]")
  {
    // Setup: Create table and insert data
    REQUIRE(execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").success);
    REQUIRE(execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").success);

    SECTION("Delete specific record")
    {
      std::string sql = "DELETE FROM users WHERE id = 2";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);

      // Verify record is deleted
      auto select_result = execute_sql("SELECT * FROM users");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 2);
    }

    SECTION("Delete multiple records")
    {
      std::string sql = "DELETE FROM users WHERE id > 1";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 2);

      // Verify only one record remains
      auto select_result = execute_sql("SELECT * FROM users");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 1);
      REQUIRE(select_result.result_rows[0][0].get_integer() == 1);
    }

    SECTION("Delete all records")
    {
      std::string sql = "DELETE FROM users";
      auto result = execute_sql(sql);

      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 3);

      // Verify table is empty
      auto select_result = execute_sql("SELECT * FROM users");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 0);
    }

    SECTION("Delete from non-existent table")
    {
      std::string sql = "DELETE FROM nonexistent";
      auto result = execute_sql(sql);

      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Table does not exist") != std::string::npos);
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Update Records", "[execution_engine]")
  {
    // Setup: Create table and insert data
    REQUIRE(execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").success);
    REQUIRE(execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").success);

    SECTION("Update single record with WHERE")
    {
      auto update_result = execute_sql("UPDATE users SET name = 'Bobby' WHERE id = 2");
      REQUIRE(update_result.success);
      REQUIRE(update_result.rows_affected == 1);

      auto select_result = execute_sql("SELECT * FROM users WHERE id = 2");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 1);
      REQUIRE(select_result.result_rows[0][1].get_string() == "Bobby");
    }

    SECTION("Update multiple records")
    {
      auto update_result = execute_sql("UPDATE users SET name = 'Anon' WHERE id > 1");
      REQUIRE(update_result.success);
      REQUIRE(update_result.rows_affected == 2);

      auto select_result = execute_sql("SELECT * FROM users WHERE id > 1");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 2);
      REQUIRE(select_result.result_rows[0][1].get_string() == "Anon");
      REQUIRE(select_result.result_rows[1][1].get_string() == "Anon");
    }

    SECTION("Update without WHERE updates all")
    {
      auto update_result = execute_sql("UPDATE users SET name = 'X'");
      REQUIRE(update_result.success);
      REQUIRE(update_result.rows_affected == 3);

      auto select_result = execute_sql("SELECT * FROM users");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 3);
      for (const auto &row : select_result.result_rows)
      {
        REQUIRE(row[1].get_string() == "X");
      }
    }

    SECTION("Update multiple SET clauses")
    {
      auto update_result = execute_sql("UPDATE users SET name = 'Z', id = 10 WHERE id = 1");
      REQUIRE(update_result.success);
      REQUIRE(update_result.rows_affected == 1);

      auto select_result = execute_sql("SELECT * FROM users WHERE name = 'Z'");
      REQUIRE(select_result.success);
      REQUIRE(select_result.rows_affected == 1);
      REQUIRE(select_result.result_rows[0][0].get_integer() == 10);
    }

    SECTION("Update on non-existent table")
    {
      auto result = execute_sql("UPDATE nope SET x = 1");
      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Table does not exist") != std::string::npos);
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Complex WHERE Clauses", "[execution_engine]")
  {
    // Setup: Create table and insert data
    REQUIRE(execute_sql("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)").success);
    REQUIRE(execute_sql("INSERT INTO products (id, name, price) VALUES (1, 'Apple', 100), (2, 'Banana', 50), (3, 'Orange', 75)").success);

    SECTION("Comparison operators")
    {
      // Test less than
      auto result = execute_sql("SELECT * FROM products WHERE price < 75");
      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
      REQUIRE(result.result_rows[0][1].get_string() == "Banana");

      // Test greater than or equal
      result = execute_sql("SELECT * FROM products WHERE price >= 75");
      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 2);
    }

    SECTION("String comparison")
    {
      auto result = execute_sql("SELECT * FROM products WHERE name > 'B'");
      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 2);
      // Should match "Banana" and "Orange" since both are > 'B'

      // Test a more specific comparison
      result = execute_sql("SELECT * FROM products WHERE name > 'O'");
      REQUIRE(result.success);
      REQUIRE(result.rows_affected == 1);
      REQUIRE(result.result_rows[0][1].get_string() == "Orange");
    }
  }

  TEST_CASE_METHOD(ExecutionEngineTestFixture, "ExecutionEngine - Error Handling", "[execution_engine]")
  {
    SECTION("Invalid SQL syntax")
    {
      auto result = execute_sql("INVALID SQL SYNTAX");
      REQUIRE_FALSE(result.success);
      REQUIRE(result.error_message.find("Parse error") != std::string::npos);
    }

    SECTION("Type conversion errors")
    {
      REQUIRE(execute_sql("CREATE TABLE test (id INTEGER)").success);

      // This should fail if we try to insert a string into an integer column
      // Note: This depends on the parser handling non-numeric literals
    }
  }

} // namespace tinydb
