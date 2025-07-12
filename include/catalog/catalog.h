#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/table_heap.h"
#include "storage/free_space_manager.h"

namespace tinydb
{

  /**
   * The Catalog is a metadata manager for the database.
   * It stores information about tables, schemas, and other database objects.
   * It uses meta-tables to persist catalog information across database restarts.
   */
  class Catalog
  {
  public:
    Catalog(BufferPoolManager *buffer_pool_manager);

    // Create a new table
    TableHeap *create_table(const std::string &table_name, const Schema &schema);

    // Get a table by name
    TableHeap *get_table(const std::string &table_name);

    // Get schema for a table
    const Schema *get_schema(const std::string &table_name) const;

  private:
    // System constants
    static constexpr table_id_t kCatalogTablesTableId = 0;
    static constexpr table_id_t kCatalogColumnsTableId = 1;
    static constexpr table_id_t kFirstUserTableId = 2;

    BufferPoolManager *m_buffer_pool_manager_;
    std::unique_ptr<FreeSpaceManager> m_free_space_manager_;

    // System meta-tables
    std::unique_ptr<TableHeap> m_catalog_tables_heap_;  // Table metadata
    std::unique_ptr<TableHeap> m_catalog_columns_heap_; // Column metadata
    std::unique_ptr<Schema> m_catalog_tables_schema_;   // Tables schema
    std::unique_ptr<Schema> m_catalog_columns_schema_;  // Columns schema

    // User tables cache
    std::unordered_map<std::string, table_id_t> m_table_names_;
    std::unordered_map<table_id_t, std::unique_ptr<TableHeap>> m_tables_;
    std::unordered_map<table_id_t, Schema> m_schemas_;
    table_id_t m_next_table_id_{kFirstUserTableId};

    // Bootstrap logic
    bool bootstrap_database();
    bool load_existing_catalog();
    bool create_meta_tables();
    bool load_user_tables_from_catalog();
    Schema load_schema_for_table(table_id_t table_id);

    // Meta-table schema creation
    Schema create_catalog_tables_schema() const;
    Schema create_catalog_columns_schema() const;

    // Persistence helpers
    bool persist_table_metadata(table_id_t table_id, const std::string &name, page_id_t first_page_id);
    bool persist_column_metadata(table_id_t table_id, const Schema &schema);
  };

} // namespace tinydb
