#pragma once

#include "common/config.h"
#include "storage/page.h"
#include <string>
#include <fstream>
#include <mutex>
#include <stdexcept>

namespace tinydb
{

  /**
   * DiskManager handles low-level disk I/O operations only.
   * It is responsible for reading and writing pages to/from the database file.
   * Page allocation/deallocation is handled by FreeSpaceManager.
   */
  class DiskManager
  {
  public:
    explicit DiskManager(const std::string &db_file);
    ~DiskManager();

    // Core I/O operations
    void write_page(page_id_t page_id, const char *page_data);
    bool read_page(page_id_t page_id, char *page_data);

    // Get the size of the database file in pages
    page_id_t get_file_size_in_pages();

  private:
    std::fstream m_db_io_;
    std::string m_file_name_;
    std::mutex m_latch_;
  };

} // namespace tinydb
