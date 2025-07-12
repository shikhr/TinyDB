#include "storage/disk_manager.h"
#include <cassert>
#include <stdexcept>

namespace tinydb
{

  DiskManager::DiskManager(const std::string &db_file) : m_file_name_(db_file)
  {
    // Try to open existing file
    m_db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);

    if (!m_db_io_.is_open())
    {
      // Create new file
      m_db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
      if (!m_db_io_.is_open())
      {
        throw std::runtime_error("Failed to open or create database file: " + db_file);
      }
    }
  }

  DiskManager::~DiskManager()
  {
    if (m_db_io_.is_open())
    {
      m_db_io_.close();
    }
  }

  void DiskManager::write_page(page_id_t page_id, const char *page_data)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    size_t offset = static_cast<size_t>(page_id) * kPageSize;

    // Clear any error flags before seeking
    m_db_io_.clear();

    m_db_io_.seekp(offset);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error seeking to page " + std::to_string(page_id) +
                               " at offset " + std::to_string(offset));
    }

    m_db_io_.write(page_data, kPageSize);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error writing to file at page " + std::to_string(page_id));
    }
    m_db_io_.flush();
  }

  bool DiskManager::read_page(page_id_t page_id, char *page_data)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    size_t offset = static_cast<size_t>(page_id) * kPageSize;
    m_db_io_.seekg(offset);
    if (m_db_io_.fail())
    {
      return false;
    }
    m_db_io_.read(page_data, kPageSize);
    if (m_db_io_.fail())
    {
      return false;
    }
    // Check if we actually read the full page
    if (m_db_io_.gcount() != kPageSize)
    {
      return false; // Didn't read the expected amount (e.g., read past EOF)
    }
    return true;
  }

  page_id_t DiskManager::get_file_size_in_pages()
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    m_db_io_.seekg(0, std::ios::end);
    size_t file_size = m_db_io_.tellg();
    return static_cast<page_id_t>(file_size / kPageSize);
  }

} // namespace tinydb
