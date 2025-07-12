#pragma once

#include "common/config.h"
#include <cstring>

namespace tinydb
{

  /**
   * DBHeaderPage represents the layout of Page 0 in the database file.
   * This is the superblock that serves as the master record for the entire database.
   * It must be exactly one page (4096 bytes) in size.
   */
  class DBHeaderPage
  {
  public:
    // Initialize a new database header
    void init()
    {
      std::memcpy(m_magic_string_, kMagicString, 8);
      m_page_count_ = 2; // Start with header page and free space map page
      m_catalog_tables_page_id_ = INVALID_PAGE_ID;
      m_fs_map_root_page_id_ = 1; // Fixed at page 1
      std::memset(m_reserved_, 0, sizeof(m_reserved_));
    }

    // Magic string access
    bool is_valid() const
    {
      return std::memcmp(m_magic_string_, kMagicString, 8) == 0;
    }

    // Page count management
    uint32_t get_page_count() const { return m_page_count_; }
    void set_page_count(uint32_t count) { m_page_count_ = count; }

    // Catalog tables page management
    page_id_t get_catalog_tables_page_id() const { return m_catalog_tables_page_id_; }
    void set_catalog_tables_page_id(page_id_t page_id) { m_catalog_tables_page_id_ = page_id; }

    // Free space map root page management
    page_id_t get_fs_map_root_page_id() const { return m_fs_map_root_page_id_; }
    void set_fs_map_root_page_id(page_id_t page_id) { m_fs_map_root_page_id_ = page_id; }

    // Check if database is initialized
    bool is_initialized() const
    {
      return is_valid() && m_catalog_tables_page_id_ != INVALID_PAGE_ID;
    }

  private:
    static constexpr const char kMagicString[9] = "TINYDB01";

    // Database identification
    char m_magic_string_[8]; // "TINYDB01"

    // Database metadata
    uint32_t m_page_count_;              // Total number of pages allocated
    page_id_t m_catalog_tables_page_id_; // First page of __catalog_tables
    page_id_t m_fs_map_root_page_id_;    // Free space map page (always 1)

    // Reserved space for future extensions
    char m_reserved_[4076]; // Fill remaining page space (4096 - 8 - 4 - 4 - 4 = 4076)
  };

  // Ensure the header page is exactly one page size
  static_assert(sizeof(DBHeaderPage) == kPageSize, "DBHeaderPage must be exactly one page size");

} // namespace tinydb
