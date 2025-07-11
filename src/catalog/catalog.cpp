#include "catalog/catalog.h"
#include "storage/table_page.h"

namespace tinydb
{

  Catalog::Catalog(BufferPoolManager *buffer_pool_manager)
      : m_buffer_pool_manager_(buffer_pool_manager) {}

  TableHeap *Catalog::create_table(const std::string &table_name, const Schema &schema)
  {
    // Check if table already exists
    if (m_table_names_.find(table_name) != m_table_names_.end())
    {
      return nullptr; // Table already exists
    }

    // Allocate a new table ID
    table_id_t table_id = m_next_table_id_++;

    // Create the first page for the table
    page_id_t first_page_id;
    Page *first_page = m_buffer_pool_manager_->new_page(&first_page_id);
    if (first_page == nullptr)
    {
      return nullptr; // Failed to allocate page
    }

    // Initialize the first page as a table page
    TablePage *table_page = reinterpret_cast<TablePage *>(first_page);
    table_page->init(first_page_id, INVALID_PAGE_ID);

    // Unpin the page (we don't need it pinned anymore)
    m_buffer_pool_manager_->unpin_page(first_page_id, true);

    // Create the table heap
    auto table_heap = std::make_unique<TableHeap>(m_buffer_pool_manager_, first_page_id);

    // Store the table information
    m_table_names_[table_name] = table_id;
    m_schemas_.emplace(table_id, schema);
    TableHeap *table_ptr = table_heap.get();
    m_tables_[table_id] = std::move(table_heap);

    return table_ptr;
  }

  TableHeap *Catalog::get_table(const std::string &table_name)
  {
    auto it = m_table_names_.find(table_name);
    if (it == m_table_names_.end())
    {
      return nullptr; // Table not found
    }

    table_id_t table_id = it->second;
    auto table_it = m_tables_.find(table_id);
    if (table_it == m_tables_.end())
    {
      return nullptr; // This should not happen
    }

    return table_it->second.get();
  }

  const Schema *Catalog::get_schema(const std::string &table_name) const
  {
    auto it = m_table_names_.find(table_name);
    if (it == m_table_names_.end())
    {
      return nullptr; // Table not found
    }

    table_id_t table_id = it->second;
    auto schema_it = m_schemas_.find(table_id);
    if (schema_it == m_schemas_.end())
    {
      return nullptr; // This should not happen
    }

    return &schema_it->second;
  }

} // namespace tinydb
