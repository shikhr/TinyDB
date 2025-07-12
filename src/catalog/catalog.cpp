#include "catalog/catalog.h"
#include "storage/table_page.h"
#include "storage/db_header_page.h"
#include <stdexcept>
#include <algorithm>

namespace tinydb
{

  Catalog::Catalog(BufferPoolManager *buffer_pool_manager)
      : m_buffer_pool_manager_(buffer_pool_manager),
        m_free_space_manager_(std::make_unique<FreeSpaceManager>(buffer_pool_manager))
  {
    // Initialize free space manager
    if (!m_free_space_manager_->initialize())
    {
      throw std::runtime_error("Failed to initialize free space manager");
    }

    // Bootstrap the catalog
    if (!bootstrap_database())
    {
      throw std::runtime_error("Failed to bootstrap catalog");
    }
  }

  bool Catalog::bootstrap_database()
  {
    // Try to fetch Page 0 (DBHeaderPage)
    Page *header_page_raw = m_buffer_pool_manager_->fetch_page(0);

    if (header_page_raw == nullptr)
    {
      // Page 0 doesn't exist - this is a new database
      // Write Page 0 directly to disk first
      char header_data[kPageSize];
      std::memset(header_data, 0, kPageSize);

      DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_data);
      header->init();

      // Write to disk directly
      DiskManager *disk_manager = m_buffer_pool_manager_->get_disk_manager();
      if (disk_manager == nullptr)
      {
        return false;
      }
      disk_manager->write_page(0, header_data);

      // Now fetch it through buffer pool
      header_page_raw = m_buffer_pool_manager_->fetch_page(0);
      if (header_page_raw == nullptr)
      {
        return false;
      }

      m_buffer_pool_manager_->unpin_page(0, false);

      // Create meta-tables for new database
      return create_meta_tables();
    }

    // Read existing header
    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page_raw->get_data());

    if (!header->is_valid())
    {
      m_buffer_pool_manager_->unpin_page(0, false);
      return false;
    }

    page_id_t catalog_tables_page_id = header->get_catalog_tables_page_id();
    m_buffer_pool_manager_->unpin_page(0, false);

    if (catalog_tables_page_id == INVALID_PAGE_ID)
    {
      // New database - create meta-tables
      return create_meta_tables();
    }
    else
    {
      // Existing database - load catalog
      return load_existing_catalog();
    }
  }

  bool Catalog::create_meta_tables()
  {
    // Create schemas for meta-tables
    m_catalog_tables_schema_ = std::make_unique<Schema>(create_catalog_tables_schema());
    m_catalog_columns_schema_ = std::make_unique<Schema>(create_catalog_columns_schema());

    // Step 1: Create __catalog_tables
    page_id_t tables_page_id = m_free_space_manager_->allocate_page();
    if (tables_page_id == INVALID_PAGE_ID)
    {
      return false;
    }

    Page *tables_page = m_buffer_pool_manager_->new_page(tables_page_id);
    if (tables_page == nullptr)
    {
      m_free_space_manager_->deallocate_page(tables_page_id);
      return false;
    }

    TablePage *tables_table_page = reinterpret_cast<TablePage *>(tables_page);
    tables_table_page->init(tables_page_id, INVALID_PAGE_ID);
    m_buffer_pool_manager_->unpin_page(tables_page_id, true);

    m_catalog_tables_heap_ = std::make_unique<TableHeap>(m_buffer_pool_manager_, tables_page_id, m_free_space_manager_.get());

    // Step 2: Create __catalog_columns
    page_id_t columns_page_id = m_free_space_manager_->allocate_page();
    if (columns_page_id == INVALID_PAGE_ID)
    {
      return false;
    }

    Page *columns_page = m_buffer_pool_manager_->new_page(columns_page_id);
    if (columns_page == nullptr)
    {
      m_free_space_manager_->deallocate_page(columns_page_id);
      return false;
    }

    TablePage *columns_table_page = reinterpret_cast<TablePage *>(columns_page);
    columns_table_page->init(columns_page_id, INVALID_PAGE_ID);
    m_buffer_pool_manager_->unpin_page(columns_page_id, true);

    m_catalog_columns_heap_ = std::make_unique<TableHeap>(m_buffer_pool_manager_, columns_page_id, m_free_space_manager_.get());

    // Step 3: Update DBHeaderPage with catalog tables root page ID
    Page *header_page = m_buffer_pool_manager_->fetch_page(0);
    if (header_page == nullptr)
    {
      return false;
    }

    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page->get_data());
    header->set_catalog_tables_page_id(tables_page_id);
    m_buffer_pool_manager_->unpin_page(0, true);

    // Step 4: Insert meta-table records into __catalog_tables so they describe themselves
    if (!persist_table_metadata(kCatalogTablesTableId, "__catalog_tables", tables_page_id) ||
        !persist_table_metadata(kCatalogColumnsTableId, "__catalog_columns", columns_page_id))
    {
      return false;
    }

    // Step 5: Insert column metadata for all meta-tables
    if (!persist_column_metadata(kCatalogTablesTableId, *m_catalog_tables_schema_) ||
        !persist_column_metadata(kCatalogColumnsTableId, *m_catalog_columns_schema_))
    {
      return false;
    }

    return true;
  }

  bool Catalog::load_existing_catalog()
  {
    // Get catalog tables page ID from header
    Page *header_page = m_buffer_pool_manager_->fetch_page(0);
    if (header_page == nullptr)
    {
      return false;
    }

    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page->get_data());
    page_id_t tables_page_id = header->get_catalog_tables_page_id();
    m_buffer_pool_manager_->unpin_page(0, false);

    // Create schemas for meta-tables
    m_catalog_tables_schema_ = std::make_unique<Schema>(create_catalog_tables_schema());
    m_catalog_columns_schema_ = std::make_unique<Schema>(create_catalog_columns_schema());

    // Load __catalog_tables
    m_catalog_tables_heap_ = std::make_unique<TableHeap>(m_buffer_pool_manager_, tables_page_id, m_free_space_manager_.get());

    // Find __catalog_columns page ID from __catalog_tables
    page_id_t columns_page_id = INVALID_PAGE_ID;
    auto table_iter = m_catalog_tables_heap_->begin();
    auto table_end = m_catalog_tables_heap_->end();

    for (; table_iter != table_end; ++table_iter)
    {
      const Record &record = *table_iter;
      auto values = m_catalog_tables_schema_->deserialize_record(record.get_data(), record.get_size());

      if (values.size() >= 3)
      {
        table_id_t table_id = static_cast<table_id_t>(values[0].get_integer());
        if (table_id == kCatalogColumnsTableId)
        {
          columns_page_id = static_cast<page_id_t>(values[2].get_integer());
          break;
        }
      }
    }

    if (columns_page_id == INVALID_PAGE_ID)
    {
      return false; // Could not find __catalog_columns table
    }

    // Load __catalog_columns
    m_catalog_columns_heap_ = std::make_unique<TableHeap>(m_buffer_pool_manager_, columns_page_id, m_free_space_manager_.get());

    // Load user tables from catalog
    return load_user_tables_from_catalog();
  }

  bool Catalog::load_user_tables_from_catalog()
  {
    // Load all user tables (table_id >= kFirstUserTableId) from __catalog_tables
    auto table_iter = m_catalog_tables_heap_->begin();
    auto table_end = m_catalog_tables_heap_->end();

    for (; table_iter != table_end; ++table_iter)
    {
      const Record &record = *table_iter;
      auto values = m_catalog_tables_schema_->deserialize_record(record.get_data(), record.get_size());

      if (values.size() >= 3)
      {
        table_id_t table_id = static_cast<table_id_t>(values[0].get_integer());

        // Skip system tables
        if (table_id < kFirstUserTableId)
        {
          continue;
        }

        std::string table_name = values[1].get_string();
        page_id_t first_page_id = static_cast<page_id_t>(values[2].get_integer());

        // Load schema for this table from __catalog_columns
        Schema schema = load_schema_for_table(table_id);

        // Create TableHeap for this table
        auto table_heap = std::make_unique<TableHeap>(m_buffer_pool_manager_, first_page_id, m_free_space_manager_.get());

        // Store in memory maps
        m_table_names_[table_name] = table_id;
        m_schemas_.emplace(table_id, std::move(schema));
        m_tables_[table_id] = std::move(table_heap);

        // Update next table ID
        if (table_id >= m_next_table_id_)
        {
          m_next_table_id_ = table_id + 1;
        }
      }
    }

    return true;
  }

  Schema Catalog::load_schema_for_table(table_id_t table_id)
  {
    std::vector<Column> columns;

    // Scan __catalog_columns to find all columns for this table
    auto column_iter = m_catalog_columns_heap_->begin();
    auto column_end = m_catalog_columns_heap_->end();

    // Collect all columns for this table with their indices
    std::vector<std::pair<int32_t, Column>> indexed_columns;

    for (; column_iter != column_end; ++column_iter)
    {
      const Record &record = *column_iter;
      auto values = m_catalog_columns_schema_->deserialize_record(record.get_data(), record.get_size());

      if (values.size() >= 5)
      {
        table_id_t col_table_id = static_cast<table_id_t>(values[0].get_integer());

        if (col_table_id == table_id)
        {
          std::string column_name = values[1].get_string();
          ColumnType column_type = static_cast<ColumnType>(values[2].get_integer());
          int32_t column_length = values[3].get_integer();
          int32_t column_index = values[4].get_integer();

          // Create column with nullable=false (default for now)
          Column column(column_name, column_type, column_length, false);
          indexed_columns.emplace_back(column_index, std::move(column));
        }
      }
    }

    // Sort columns by their index to maintain original order
    std::sort(indexed_columns.begin(), indexed_columns.end(),
              [](const auto &a, const auto &b)
              { return a.first < b.first; });

    // Extract sorted columns
    for (const auto &indexed_col : indexed_columns)
    {
      columns.push_back(std::move(indexed_col.second));
    }

    return Schema(std::move(columns));
  }

  Schema Catalog::create_catalog_tables_schema() const
  {
    std::vector<Column> columns = {
        Column("table_id", ColumnType::INTEGER, 0, false),
        Column("table_name", ColumnType::VARCHAR, 64, false),
        Column("first_page_id", ColumnType::INTEGER, 0, false)};
    return Schema(std::move(columns));
  }

  Schema Catalog::create_catalog_columns_schema() const
  {
    std::vector<Column> columns = {
        Column("table_id", ColumnType::INTEGER, 0, false),
        Column("column_name", ColumnType::VARCHAR, 64, false),
        Column("column_type", ColumnType::INTEGER, 0, false),
        Column("column_length", ColumnType::INTEGER, 0, false),
        Column("column_index", ColumnType::INTEGER, 0, false)};
    return Schema(std::move(columns));
  }

  TableHeap *Catalog::create_table(const std::string &table_name, const Schema &schema)
  {
    // Check if table already exists
    if (m_table_names_.find(table_name) != m_table_names_.end())
    {
      return nullptr; // Table already exists
    }

    // Allocate a new table ID
    table_id_t table_id = m_next_table_id_++;

    // Step 1: Get page ID for the new table
    page_id_t allocated_page_id = m_free_space_manager_->allocate_page();
    if (allocated_page_id == INVALID_PAGE_ID)
    {
      return nullptr;
    }

    // Step 2: Get the page frame and initialize it
    Page *first_page = m_buffer_pool_manager_->new_page(allocated_page_id);
    if (first_page == nullptr)
    {
      m_free_space_manager_->deallocate_page(allocated_page_id);
      return nullptr;
    }

    // Initialize the first page as a table page
    TablePage *table_page = reinterpret_cast<TablePage *>(first_page);
    table_page->init(allocated_page_id, INVALID_PAGE_ID);
    m_buffer_pool_manager_->unpin_page(allocated_page_id, true);

    // Create the table heap
    auto table_heap = std::make_unique<TableHeap>(m_buffer_pool_manager_, allocated_page_id, m_free_space_manager_.get());

    // Store in memory maps
    m_table_names_[table_name] = table_id;
    m_schemas_.emplace(table_id, schema);
    TableHeap *table_ptr = table_heap.get();
    m_tables_[table_id] = std::move(table_heap);

    // Persist to catalog meta-tables
    if (!persist_table_metadata(table_id, table_name, allocated_page_id))
    {
      // Rollback on failure
      m_table_names_.erase(table_name);
      m_schemas_.erase(table_id);
      m_tables_.erase(table_id);
      m_free_space_manager_->deallocate_page(allocated_page_id);
      return nullptr;
    }

    if (!persist_column_metadata(table_id, schema))
    {
      // Rollback on failure
      m_table_names_.erase(table_name);
      m_schemas_.erase(table_id);
      m_tables_.erase(table_id);
      m_free_space_manager_->deallocate_page(allocated_page_id);
      return nullptr;
    }
    return table_ptr;
  }

  bool Catalog::persist_table_metadata(table_id_t table_id, const std::string &name, page_id_t first_page_id)
  {
    std::vector<Value> values = {
        Value(static_cast<int32_t>(table_id)),
        Value(name),
        Value(static_cast<int32_t>(first_page_id))};

    auto record_data = m_catalog_tables_schema_->serialize_record(values);
    Record record(RecordID(), record_data.size(), record_data.data());

    RecordID rid;
    return m_catalog_tables_heap_->insert_record(record, &rid);
  }

  bool Catalog::persist_column_metadata(table_id_t table_id, const Schema &schema)
  {
    const auto &columns = schema.get_columns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
      const auto &column = columns[i];

      std::vector<Value> values = {
          Value(static_cast<int32_t>(table_id)),
          Value(column.get_name()),
          Value(static_cast<int32_t>(column.get_type())),
          Value(static_cast<int32_t>(column.get_max_length())),
          Value(static_cast<int32_t>(i))};

      auto record_data = m_catalog_columns_schema_->serialize_record(values);
      Record record(RecordID(), record_data.size(), record_data.data());

      RecordID rid;
      if (!m_catalog_columns_heap_->insert_record(record, &rid))
      {
        return false;
      }
    }

    return true;
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
