#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/table_heap.h"

namespace tinydb
{

  /**
   * The Catalog is a metadata manager for the database.
   * It stores information about tables, schemas, and other database objects.
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
    BufferPoolManager *m_buffer_pool_manager_;
    std::unordered_map<std::string, table_id_t> m_table_names_;
    std::unordered_map<table_id_t, std::unique_ptr<TableHeap>> m_tables_;
    std::unordered_map<table_id_t, Schema> m_schemas_;
    table_id_t m_next_table_id_{0};
  };

} // namespace tinydb
