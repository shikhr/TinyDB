#include <catch2/catch_all.hpp>
#include "catalog/catalog.h"
#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include <filesystem>

using namespace tinydb;

TEST_CASE("Catalog Integration Tests", "[catalog]")
{
  // Setup test database
  std::string db_file = std::filesystem::temp_directory_path() / "catalog_test.db";
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }

  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto buffer_pool_manager = std::make_unique<BufferPoolManager>(10, disk_manager.get());
  Catalog catalog(buffer_pool_manager.get());

  SECTION("Create Table and Schema Integration")
  {
    // Create schema
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 100),
        Column("age", ColumnType::INTEGER)};
    Schema schema(std::move(columns));

    // Create table
    TableHeap *table = catalog.create_table("users", schema);
    REQUIRE(table != nullptr);

    // Retrieve table
    TableHeap *retrieved_table = catalog.get_table("users");
    REQUIRE(retrieved_table == table);

    // Get schema
    const Schema *retrieved_schema = catalog.get_schema("users");
    REQUIRE(retrieved_schema != nullptr);
    REQUIRE(retrieved_schema->get_column_count() == 3);
    REQUIRE(retrieved_schema->get_column(0).get_name() == "id");
    REQUIRE(retrieved_schema->get_column(1).get_name() == "name");
    REQUIRE(retrieved_schema->get_column(2).get_name() == "age");
  }

  SECTION("Insert and Retrieve Records with Schema")
  {
    // Create schema
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 50)};
    Schema schema(std::move(columns));

    // Create table
    TableHeap *table = catalog.create_table("employees", schema);
    REQUIRE(table != nullptr);

    // Create typed values
    std::vector<Value> values = {
        Value(1),
        Value(std::string("Alice"))};

    // Serialize the record using the schema
    auto serialized_data = schema.serialize_record(values);

    // Create a record with the serialized data
    Record record(RecordID(), serialized_data.size(), serialized_data.data());

    // Insert the record
    RecordID rid;
    bool inserted = table->insert_record(record, &rid);
    REQUIRE(inserted);

    // Retrieve the record
    Record retrieved_record(RecordID(), 0, nullptr);
    bool found = table->get_record(rid, &retrieved_record);
    REQUIRE(found);

    // Deserialize the retrieved data
    auto deserialized_values = schema.deserialize_record(
        retrieved_record.get_data(),
        retrieved_record.get_size());

    REQUIRE(deserialized_values.size() == 2);
    REQUIRE(deserialized_values[0].get_integer() == 1);
    REQUIRE(deserialized_values[1].get_string() == "Alice");
  }

  SECTION("Duplicate Table Names")
  {
    std::vector<Column> columns = {Column("id", ColumnType::INTEGER)};
    Schema schema(std::move(columns));

    // Create first table
    TableHeap *table1 = catalog.create_table("test_table", schema);
    REQUIRE(table1 != nullptr);

    // Try to create table with same name
    std::vector<Column> columns2 = {Column("name", ColumnType::VARCHAR, 50)};
    Schema schema2(std::move(columns2));
    TableHeap *table2 = catalog.create_table("test_table", schema2);
    REQUIRE(table2 == nullptr); // Should fail
  }

  SECTION("Non-existent Table")
  {
    TableHeap *table = catalog.get_table("non_existent");
    REQUIRE(table == nullptr);

    const Schema *schema = catalog.get_schema("non_existent");
    REQUIRE(schema == nullptr);
  }

  // Cleanup
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
