#include <catch2/catch_all.hpp>
#include "catalog/schema.h"
using namespace tinydb;

TEST_CASE("Schema Serialization Tests", "[schema]")
{
  SECTION("Value Creation and Type Checking")
  {
    Value int_val(42);
    Value str_val(std::string("hello"));
    Value null_val;

    REQUIRE(int_val.get_type() == ColumnType::INTEGER);
    REQUIRE(str_val.get_type() == ColumnType::VARCHAR);
    REQUIRE(null_val.is_null());

    REQUIRE(int_val.get_integer() == 42);
    REQUIRE(str_val.get_string() == "hello");
  }

  SECTION("Column Properties")
  {
    Column int_col("id", ColumnType::INTEGER);
    Column str_col("name", ColumnType::VARCHAR, 50);

    REQUIRE(int_col.get_name() == "id");
    REQUIRE(int_col.get_type() == ColumnType::INTEGER);
    REQUIRE(int_col.get_fixed_size() == sizeof(int32_t));
    REQUIRE_FALSE(int_col.is_variable_length());

    REQUIRE(str_col.get_name() == "name");
    REQUIRE(str_col.get_type() == ColumnType::VARCHAR);
    REQUIRE(str_col.get_max_length() == 50);
    REQUIRE(str_col.is_variable_length());
  }

  SECTION("Schema Column Lookup")
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 100),
        Column("age", ColumnType::INTEGER)};
    Schema schema(std::move(columns));

    REQUIRE(schema.get_column_count() == 3);

    auto id_index = schema.get_column_index("id");
    auto name_index = schema.get_column_index("name");
    auto missing_index = schema.get_column_index("missing");

    REQUIRE(id_index.has_value());
    REQUIRE(id_index.value() == 0);
    REQUIRE(name_index.has_value());
    REQUIRE(name_index.value() == 1);
    REQUIRE_FALSE(missing_index.has_value());
  }

  SECTION("Record Serialization and Deserialization")
  {
    // Create schema: (id INTEGER, name VARCHAR(50), age INTEGER)
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 50),
        Column("age", ColumnType::INTEGER)};
    Schema schema(std::move(columns));

    // Create test values
    std::vector<Value> values = {
        Value(123),                  // id
        Value(std::string("Alice")), // name
        Value(25)                    // age
    };

    // Serialize
    auto serialized = schema.serialize_record(values);
    REQUIRE(serialized.size() > 0);

    // Deserialize
    auto deserialized = schema.deserialize_record(serialized.data(), serialized.size());

    REQUIRE(deserialized.size() == 3);
    REQUIRE(deserialized[0].get_type() == ColumnType::INTEGER);
    REQUIRE(deserialized[0].get_integer() == 123);
    REQUIRE(deserialized[1].get_type() == ColumnType::VARCHAR);
    REQUIRE(deserialized[1].get_string() == "Alice");
    REQUIRE(deserialized[2].get_type() == ColumnType::INTEGER);
    REQUIRE(deserialized[2].get_integer() == 25);
  }

  SECTION("Null Value Serialization")
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 50)};
    Schema schema(std::move(columns));

    // Create values with a null
    std::vector<Value> values = {
        Value(456), // id
        Value()     // name (null)
    };

    // Serialize and deserialize
    auto serialized = schema.serialize_record(values);
    auto deserialized = schema.deserialize_record(serialized.data(), serialized.size());

    REQUIRE(deserialized.size() == 2);
    REQUIRE(deserialized[0].get_integer() == 456);
    REQUIRE(deserialized[1].is_null());
  }
}
