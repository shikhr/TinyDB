#include <catch2/catch_all.hpp>
#include "parser/lexer.h"

namespace tinydb
{

  TEST_CASE("Lexer - Basic tokenization", "[lexer]")
  {
    SECTION("Empty input")
    {
      Lexer lexer("");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 1);
      REQUIRE(tokens[0].type == TokenType::END_OF_FILE);
    }

    SECTION("Whitespace only")
    {
      Lexer lexer("   \t\n  ");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 1);
      REQUIRE(tokens[0].type == TokenType::END_OF_FILE);
    }
  }

  TEST_CASE("Lexer - Keywords", "[lexer]")
  {
    SECTION("Basic SQL keywords")
    {
      Lexer lexer("SELECT FROM WHERE");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4); // 3 keywords + EOF

      REQUIRE(tokens[0].type == TokenType::KEYWORD);
      REQUIRE(tokens[0].value == "SELECT");
      REQUIRE(tokens[1].type == TokenType::KEYWORD);
      REQUIRE(tokens[1].value == "FROM");
      REQUIRE(tokens[2].type == TokenType::KEYWORD);
      REQUIRE(tokens[2].value == "WHERE");
      REQUIRE(tokens[3].type == TokenType::END_OF_FILE);
    }

    SECTION("Case insensitive keywords")
    {
      Lexer lexer("select Select SeLeCt");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      // All should be normalized to uppercase
      for (int i = 0; i < 3; ++i)
      {
        REQUIRE(tokens[i].type == TokenType::KEYWORD);
        REQUIRE(tokens[i].value == "SELECT");
      }
    }

    SECTION("Data type keywords")
    {
      Lexer lexer("INTEGER TEXT VARCHAR BOOLEAN");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 5);

      REQUIRE(tokens[0].value == "INTEGER");
      REQUIRE(tokens[1].value == "TEXT");
      REQUIRE(tokens[2].value == "VARCHAR");
      REQUIRE(tokens[3].value == "BOOLEAN");
    }
  }

  TEST_CASE("Lexer - Identifiers", "[lexer]")
  {
    SECTION("Simple identifiers")
    {
      Lexer lexer("table_name user_id column1");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      REQUIRE(tokens[0].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[0].value == "table_name");
      REQUIRE(tokens[1].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[1].value == "user_id");
      REQUIRE(tokens[2].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[2].value == "column1");
    }

    SECTION("Identifiers with underscores")
    {
      Lexer lexer("_private __double_underscore table_");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      REQUIRE(tokens[0].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[0].value == "_private");
      REQUIRE(tokens[1].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[1].value == "__double_underscore");
      REQUIRE(tokens[2].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[2].value == "table_");
    }

    SECTION("Mixed case identifiers")
    {
      Lexer lexer("MyTable userId CamelCase");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      // Identifiers should preserve case (unlike keywords)
      REQUIRE(tokens[0].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[0].value == "MyTable");
      REQUIRE(tokens[1].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[1].value == "userId");
      REQUIRE(tokens[2].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[2].value == "CamelCase");
    }
  }

  TEST_CASE("Lexer - String literals", "[lexer]")
  {
    SECTION("Simple string")
    {
      Lexer lexer("\"hello world\"");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 2);

      REQUIRE(tokens[0].type == TokenType::STRING_LITERAL);
      REQUIRE(tokens[0].value == "hello world");
    }

    SECTION("Empty string")
    {
      Lexer lexer("\"\"");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 2);

      REQUIRE(tokens[0].type == TokenType::STRING_LITERAL);
      REQUIRE(tokens[0].value == "");
    }

    SECTION("String with special characters")
    {
      Lexer lexer("\"Hello, 123! @#$%\"");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 2);

      REQUIRE(tokens[0].type == TokenType::STRING_LITERAL);
      REQUIRE(tokens[0].value == "Hello, 123! @#$%");
    }

    SECTION("Multiple strings")
    {
      Lexer lexer("\"first\" \"second\" \"third\"");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      REQUIRE(tokens[0].value == "first");
      REQUIRE(tokens[1].value == "second");
      REQUIRE(tokens[2].value == "third");
    }
  }

  TEST_CASE("Lexer - Number literals", "[lexer]")
  {
    SECTION("Single digit")
    {
      Lexer lexer("5");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 2);

      REQUIRE(tokens[0].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[0].value == "5");
    }

    SECTION("Multi-digit numbers")
    {
      Lexer lexer("123 456789 0");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 4);

      REQUIRE(tokens[0].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[0].value == "123");
      REQUIRE(tokens[1].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[1].value == "456789");
      REQUIRE(tokens[2].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[2].value == "0");
    }

    SECTION("Large numbers")
    {
      Lexer lexer("999999999999999");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 2);

      REQUIRE(tokens[0].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[0].value == "999999999999999");
    }
  }

  TEST_CASE("Lexer - Operators", "[lexer]")
  {
    SECTION("Single character operators")
    {
      Lexer lexer("+ - * / = < > !");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 9); // 8 operators + EOF

      std::vector<std::string> expected = {"+", "-", "*", "/", "=", "<", ">", "!"};
      for (size_t i = 0; i < expected.size(); ++i)
      {
        REQUIRE(tokens[i].type == TokenType::OPERATOR);
        REQUIRE(tokens[i].value == expected[i]);
      }
    }

    SECTION("Multi-character operators")
    {
      Lexer lexer("== != <= >= <>");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 6); // 5 operators + EOF

      std::vector<std::string> expected = {"==", "!=", "<=", ">=", "<>"};
      for (size_t i = 0; i < expected.size(); ++i)
      {
        REQUIRE(tokens[i].type == TokenType::OPERATOR);
        REQUIRE(tokens[i].value == expected[i]);
      }
    }

    SECTION("Mixed operators")
    {
      Lexer lexer("= == < <= > >= ! !=");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 9);

      std::vector<std::string> expected = {"=", "==", "<", "<=", ">", ">=", "!", "!="};
      for (size_t i = 0; i < expected.size(); ++i)
      {
        REQUIRE(tokens[i].type == TokenType::OPERATOR);
        REQUIRE(tokens[i].value == expected[i]);
      }
    }
  }

  TEST_CASE("Lexer - Punctuation", "[lexer]")
  {
    SECTION("Common punctuation")
    {
      Lexer lexer("( ) [ ] { } , . ; :");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 11); // 10 punctuation + EOF

      std::vector<std::string> expected = {"(", ")", "[", "]", "{", "}", ",", ".", ";", ":"};
      for (size_t i = 0; i < expected.size(); ++i)
      {
        REQUIRE(tokens[i].type == TokenType::PUNCTUATION);
        REQUIRE(tokens[i].value == expected[i]);
      }
    }
  }

  TEST_CASE("Lexer - Complex SQL statements", "[lexer]")
  {
    SECTION("CREATE TABLE statement")
    {
      Lexer lexer("CREATE TABLE users (id INTEGER, name TEXT)");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 11); // tokens + EOF

      // Verify key tokens
      REQUIRE(tokens[0].type == TokenType::KEYWORD);
      REQUIRE(tokens[0].value == "CREATE");
      REQUIRE(tokens[1].type == TokenType::KEYWORD);
      REQUIRE(tokens[1].value == "TABLE");
      REQUIRE(tokens[2].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[2].value == "users");
      REQUIRE(tokens[3].type == TokenType::PUNCTUATION);
      REQUIRE(tokens[3].value == "(");
      REQUIRE(tokens[4].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[4].value == "id");
    }

    SECTION("SELECT statement")
    {
      Lexer lexer("SELECT * FROM users WHERE id = 123");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 9);

      REQUIRE(tokens[0].value == "SELECT");
      REQUIRE(tokens[1].value == "*");
      REQUIRE(tokens[2].value == "FROM");
      REQUIRE(tokens[3].value == "users");
      REQUIRE(tokens[4].value == "WHERE");
      REQUIRE(tokens[5].value == "id");
      REQUIRE(tokens[6].value == "=");
      REQUIRE(tokens[7].value == "123");
    }

    SECTION("INSERT statement")
    {
      Lexer lexer("INSERT INTO users VALUES (1, \"John Doe\")");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 10);

      REQUIRE(tokens[0].value == "INSERT");
      REQUIRE(tokens[1].value == "INTO");
      REQUIRE(tokens[2].value == "users");
      REQUIRE(tokens[3].value == "VALUES");
      REQUIRE(tokens[7].type == TokenType::STRING_LITERAL);
      REQUIRE(tokens[7].value == "John Doe");
    }
  }

  TEST_CASE("Lexer - Line and column tracking", "[lexer]")
  {
    SECTION("Single line")
    {
      Lexer lexer("SELECT name FROM users");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());

      // All tokens should be on line 1
      for (const auto &token : tokens)
      {
        REQUIRE(token.line == 1);
      }
    }

    SECTION("Multi-line input")
    {
      Lexer lexer("SELECT name\nFROM users\nWHERE id = 1");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 9);

      // Check line numbers
      REQUIRE(tokens[0].line == 1); // SELECT
      REQUIRE(tokens[1].line == 1); // name
      REQUIRE(tokens[2].line == 2); // FROM
      REQUIRE(tokens[3].line == 2); // users
      REQUIRE(tokens[4].line == 3); // WHERE
      REQUIRE(tokens[5].line == 3); // id
      REQUIRE(tokens[6].line == 3); // =
      REQUIRE(tokens[7].line == 3); // 1
    }
  }

  TEST_CASE("Lexer - Error handling", "[lexer]")
  {
    SECTION("Unknown character")
    {
      Lexer lexer("SELECT @ FROM users");
      auto tokens = lexer.tokenize();

      REQUIRE(lexer.had_error());
      REQUIRE(lexer.error_message().find("Unknown character: @") != std::string::npos);
    }

    SECTION("Unterminated string - graceful handling")
    {
      // Note: Current implementation throws an exception for unterminated strings
      // This test documents the current behavior
      Lexer lexer("SELECT \"unterminated");

      REQUIRE_THROWS_WITH(lexer.tokenize(), "Attempted to consume from an empty input.");
    }
  }

  TEST_CASE("Lexer - Edge cases", "[lexer]")
  {
    SECTION("Adjacent tokens without spaces")
    {
      Lexer lexer("SELECT(id)FROM users");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 7);

      REQUIRE(tokens[0].value == "SELECT");
      REQUIRE(tokens[1].value == "(");
      REQUIRE(tokens[2].value == "id");
      REQUIRE(tokens[3].value == ")");
      REQUIRE(tokens[4].value == "FROM");
      REQUIRE(tokens[5].value == "users");
    }

    SECTION("Numbers and identifiers")
    {
      Lexer lexer("table1 123abc column_1");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 5);

      REQUIRE(tokens[0].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[0].value == "table1");
      // Note: "123abc" would be tokenized as "123" + "abc"
      REQUIRE(tokens[1].type == TokenType::NUMBER_LITERAL);
      REQUIRE(tokens[1].value == "123");
      REQUIRE(tokens[2].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[2].value == "abc");
      REQUIRE(tokens[3].type == TokenType::IDENTIFIER);
      REQUIRE(tokens[3].value == "column_1");
    }

    SECTION("Operators and punctuation together")
    {
      Lexer lexer("WHERE id >= 10 AND name != \"test\"");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      REQUIRE(tokens.size() == 9);

      REQUIRE(tokens[2].value == ">=");
      REQUIRE(tokens[6].value == "!=");
    }
  }

  TEST_CASE("Lexer - SQL keyword completeness", "[lexer]")
  {
    SECTION("DDL keywords")
    {
      Lexer lexer("CREATE DROP ALTER TABLE INDEX");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      for (int i = 0; i < 5; ++i)
      {
        REQUIRE(tokens[i].type == TokenType::KEYWORD);
      }
    }

    SECTION("DML keywords")
    {
      Lexer lexer("SELECT INSERT UPDATE DELETE");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      for (int i = 0; i < 4; ++i)
      {
        REQUIRE(tokens[i].type == TokenType::KEYWORD);
      }
    }

    SECTION("Constraint keywords")
    {
      Lexer lexer("PRIMARY KEY FOREIGN REFERENCES UNIQUE NOT NULL");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      for (int i = 0; i < 7; ++i)
      {
        REQUIRE(tokens[i].type == TokenType::KEYWORD);
      }
    }

    SECTION("Boolean and null literals")
    {
      Lexer lexer("TRUE FALSE NULL");
      auto tokens = lexer.tokenize();

      REQUIRE_FALSE(lexer.had_error());
      for (int i = 0; i < 3; ++i)
      {
        REQUIRE(tokens[i].type == TokenType::KEYWORD);
      }
    }
  }

} // namespace tinydb
