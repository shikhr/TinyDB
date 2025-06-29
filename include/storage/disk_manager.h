#pragma once

#include "common/config.h"
#include "storage/page.h"
#include <string>
#include <fstream>
#include <mutex>

class DiskManager
{
public:
  explicit DiskManager(const std::string &db_file);
  ~DiskManager();

  bool validate_page_id(page_id_t page_id) const;
  void write_page(page_id_t page_id, const char *page_data);
  void read_page(page_id_t page_id, char *page_data);
  page_id_t allocate_page();
  void deallocate_page(page_id_t page_id);

private:
  std::fstream m_db_file_;
  std::string m_file_name_;
  page_id_t m_next_page_id_;
  std::mutex m_latch_;
};
