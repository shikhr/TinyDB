#include <catch2/catch_all.hpp>
#include <filesystem>
#include "catalog/catalog.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "storage/db_header_page.h"
#include "catalog/schema.h"
#include "common/config.h"

using namespace tinydb;

class CatalogPersistenceTestFixture
{
public:
  CatalogPersistenceTestFixture() : db_filename_("test_catalog_persistence.db")
  {
    // Remove any existing test database file
    if (std::filesystem::exists(db_filename_))
    {
      std::filesystem::remove(db_filename_);
    }
  }

  ~CatalogPersistenceTestFixture()
  {
    // Clean up test database file
    if (std::filesystem::exists(db_filename_))
    {
      std::filesystem::remove(db_filename_);
    }
  }

  void create_fresh_database()
  {
    disk_manager_ = std::make_unique<DiskManager>(db_filename_);
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(10, disk_manager_.get());
    catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
  }

  void shutdown_database()
  {
    catalog_.reset();
    buffer_pool_manager_.reset();
    disk_manager_.reset();
  }

  void reopen_existing_database()
  {
    disk_manager_ = std::make_unique<DiskManager>(db_filename_);
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(10, disk_manager_.get());
    catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
  }

  Schema create_test_schema()
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER, 0, false),
        Column("name", ColumnType::VARCHAR, 50, false),
        Column("age", ColumnType::INTEGER, 0, true)};
    return Schema(std::move(columns));
  }

protected:
  std::string db_filename_;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  std::unique_ptr<Catalog> catalog_;
};

TEST_CASE_METHOD(CatalogPersistenceTestFixture, "Database Creation and Catalog Bootstrap", "[catalog][persistence]")
{
  SECTION("Fresh database creation initializes meta-tables")
  {
    // Create a fresh database
    create_fresh_database();

    // Database file should be created
    REQUIRE(std::filesystem::exists(db_filename_));

    // Should be able to create a table (verifies meta-tables are working)
    Schema schema = create_test_schema();
    TableHeap *table = catalog_->create_table("users", schema);
    REQUIRE(table != nullptr);

    // Should be able to retrieve the table
    TableHeap *retrieved_table = catalog_->get_table("users");
    REQUIRE(retrieved_table == table);

    // Should be able to retrieve the schema
    const Schema *retrieved_schema = catalog_->get_schema("users");
    REQUIRE(retrieved_schema != nullptr);
    REQUIRE(retrieved_schema->get_columns().size() == 3);
    REQUIRE(retrieved_schema->get_columns()[0].get_name() == "id");
    REQUIRE(retrieved_schema->get_columns()[1].get_name() == "name");
    REQUIRE(retrieved_schema->get_columns()[2].get_name() == "age");

    shutdown_database();
  }
}

TEST_CASE_METHOD(CatalogPersistenceTestFixture, "Table Persistence Across Database Restarts", "[catalog][persistence]")
{
  // Create database and add some tables
  create_fresh_database();

  Schema users_schema = create_test_schema();
  TableHeap *users_table = catalog_->create_table("users", users_schema);
  REQUIRE(users_table != nullptr);

  std::vector<Column> products_columns = {
      Column("product_id", ColumnType::INTEGER, 0, false),
      Column("product_name", ColumnType::VARCHAR, 100, false),
      Column("price", ColumnType::INTEGER, 0, false)};
  Schema products_schema(std::move(products_columns));
  TableHeap *products_table = catalog_->create_table("products", products_schema);
  REQUIRE(products_table != nullptr);

  // Verify tables exist before shutdown
  REQUIRE(catalog_->get_table("users") != nullptr);
  REQUIRE(catalog_->get_table("products") != nullptr);

  shutdown_database();

  // Reopen database
  reopen_existing_database();

  // Verify tables are restored after restart
  TableHeap *restored_users = catalog_->get_table("users");
  REQUIRE(restored_users != nullptr);

  TableHeap *restored_products = catalog_->get_table("products");
  REQUIRE(restored_products != nullptr);

  // Verify schemas are restored correctly
  const Schema *restored_users_schema = catalog_->get_schema("users");
  REQUIRE(restored_users_schema != nullptr);
  REQUIRE(restored_users_schema->get_columns().size() == 3);
  REQUIRE(restored_users_schema->get_columns()[0].get_name() == "id");
  REQUIRE(restored_users_schema->get_columns()[0].get_type() == ColumnType::INTEGER);
  REQUIRE(restored_users_schema->get_columns()[1].get_name() == "name");
  REQUIRE(restored_users_schema->get_columns()[1].get_type() == ColumnType::VARCHAR);
  REQUIRE(restored_users_schema->get_columns()[1].get_max_length() == 50);

  const Schema *restored_products_schema = catalog_->get_schema("products");
  REQUIRE(restored_products_schema != nullptr);
  REQUIRE(restored_products_schema->get_columns().size() == 3);
  REQUIRE(restored_products_schema->get_columns()[0].get_name() == "product_id");
  REQUIRE(restored_products_schema->get_columns()[1].get_name() == "product_name");
  REQUIRE(restored_products_schema->get_columns()[1].get_max_length() == 100);
  REQUIRE(restored_products_schema->get_columns()[2].get_name() == "price");

  shutdown_database();
}

TEST_CASE_METHOD(CatalogPersistenceTestFixture, "Multiple Restart Cycles", "[catalog][persistence]")
{
  // First cycle: Create database and add a table
  create_fresh_database();
  Schema schema1 = create_test_schema();
  catalog_->create_table("table1", schema1);
  shutdown_database();

  // Second cycle: Reopen and add another table
  reopen_existing_database();
  REQUIRE(catalog_->get_table("table1") != nullptr);

  std::vector<Column> table2_columns = {
      Column("col1", ColumnType::VARCHAR, 20, false),
      Column("col2", ColumnType::INTEGER, 0, false)};
  Schema schema2(std::move(table2_columns));
  catalog_->create_table("table2", schema2);
  shutdown_database();

  // Third cycle: Reopen and verify both tables exist
  reopen_existing_database();
  REQUIRE(catalog_->get_table("table1") != nullptr);
  REQUIRE(catalog_->get_table("table2") != nullptr);

  const Schema *schema1_restored = catalog_->get_schema("table1");
  const Schema *schema2_restored = catalog_->get_schema("table2");

  REQUIRE(schema1_restored != nullptr);
  REQUIRE(schema1_restored->get_columns().size() == 3);

  REQUIRE(schema2_restored != nullptr);
  REQUIRE(schema2_restored->get_columns().size() == 2);
  REQUIRE(schema2_restored->get_columns()[0].get_name() == "col1");
  REQUIRE(schema2_restored->get_columns()[1].get_name() == "col2");

  shutdown_database();
}

TEST_CASE_METHOD(CatalogPersistenceTestFixture, "Table Creation After Database Restart", "[catalog][persistence]")
{
  // Create empty database
  create_fresh_database();
  shutdown_database();

  // Reopen and add tables
  reopen_existing_database();

  Schema schema = create_test_schema();
  TableHeap *table = catalog_->create_table("new_table", schema);
  REQUIRE(table != nullptr);

  // Verify table can be retrieved
  REQUIRE(catalog_->get_table("new_table") == table);

  shutdown_database();

  // Reopen again and verify persistence
  reopen_existing_database();

  TableHeap *restored_table = catalog_->get_table("new_table");
  REQUIRE(restored_table != nullptr);

  const Schema *restored_schema = catalog_->get_schema("new_table");
  REQUIRE(restored_schema != nullptr);
  REQUIRE(restored_schema->get_columns().size() == 3);

  shutdown_database();
}

TEST_CASE_METHOD(CatalogPersistenceTestFixture, "Database Header Page Validation", "[catalog][persistence]")
{
  // Create database
  create_fresh_database();

  // Create a table to ensure catalog root page is set
  Schema schema = create_test_schema();
  catalog_->create_table("test_table", schema);

  shutdown_database();

  // Manually verify database header page
  {
    DiskManager disk_manager(db_filename_);
    char page_data[kPageSize];
    disk_manager.read_page(0, page_data);

    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(page_data);
    REQUIRE(header->is_valid());
    REQUIRE(header->get_catalog_tables_page_id() != INVALID_PAGE_ID);
    REQUIRE(header->get_fs_map_root_page_id() == 1); // Should be Page 1
  }

  // Reopen database - should work without issues
  reopen_existing_database();
  REQUIRE(catalog_->get_table("test_table") != nullptr);

  shutdown_database();
}
