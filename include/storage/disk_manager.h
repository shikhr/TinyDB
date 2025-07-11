#pragma once

#include "common/config.h"
#include "storage/page.h"
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <stdexcept>
#include <unordered_set>

namespace tinydb
{

  class DiskManager
  {
  public:
    explicit DiskManager(const std::string &db_file);
    ~DiskManager();

    bool validate_page_id(page_id_t page_id) const;
    void write_page(page_id_t page_id, const char *page_data);
    bool read_page(page_id_t page_id, char *page_data);
    page_id_t allocate_page();
    void deallocate_page(page_id_t page_id);

  private:
    void write_header();
    void read_header();

    void write_physical_page(page_id_t physical_page_id, const char *page_data);
    void read_physical_page(page_id_t physical_page_id, char *page_data);

    std::fstream m_db_io_;
    std::string m_file_name_;
    std::mutex m_latch_;
    page_id_t m_next_page_id_;
    std::unordered_set<page_id_t> m_free_pages_;
  };

} // namespace tinydb
