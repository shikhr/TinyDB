#include "storage/disk_manager.h"
#include <cassert>
#include <stdexcept>
#include <sys/stat.h>
#include <vector>

namespace tinydb
{

  DiskManager::DiskManager(const std::string &db_file) : m_file_name_(db_file), m_next_page_id_(1)
  { // Page 0 is header
    m_db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);

    if (!m_db_io_.is_open())
    {
      m_db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
      if (!m_db_io_.is_open())
      {
        throw std::runtime_error("Failed to open or create database file: " + db_file);
      }
      // New file, initialize header
      write_header();
    }
    else
    {
      // Existing file, read header
      read_header();
    }
  }

  DiskManager::~DiskManager()
  {
    if (m_db_io_.is_open())
    {
      write_header(); // Persist metadata on close
      m_db_io_.close();
    }
  }

  void DiskManager::write_page(page_id_t page_id, const char *page_data)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    assert(validate_page_id(page_id));
    size_t offset = static_cast<size_t>(page_id) * kPageSize;
    m_db_io_.seekp(offset);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error seeking to page " + std::to_string(page_id));
    }
    m_db_io_.write(page_data, kPageSize);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error writing to file");
    }
    m_db_io_.flush();
  }

  bool DiskManager::read_page(page_id_t page_id, char *page_data)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    if (!validate_page_id(page_id) || m_free_pages_.find(page_id) != m_free_pages_.end())
    {
      return false;
    }
    size_t offset = static_cast<size_t>(page_id) * kPageSize;
    m_db_io_.seekp(offset);
    m_db_io_.read(page_data, kPageSize);
    if (m_db_io_.fail() && !m_db_io_.eof())
    {
      throw std::runtime_error("Error reading from file");
    }
    return true;
  }

  page_id_t DiskManager::allocate_page()
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    page_id_t new_page_id;
    if (!m_free_pages_.empty())
    {
      new_page_id = *m_free_pages_.begin();
      m_free_pages_.erase(m_free_pages_.begin());
    }
    else
    {
      new_page_id = m_next_page_id_++;
    }
    return new_page_id;
  }

  void DiskManager::deallocate_page(page_id_t page_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    if (page_id > 0 && page_id < m_next_page_id_ && m_free_pages_.find(page_id) == m_free_pages_.end())
    {
      m_free_pages_.insert(page_id);
    }
  }

  bool DiskManager::validate_page_id(page_id_t page_id) const
  {
    if (page_id < 0 || static_cast<uint32_t>(page_id) >= m_next_page_id_)
    {
      return false;
    }
    return true;
  }

  void DiskManager::write_header()
  {
    m_db_io_.seekp(0);
    m_db_io_.write(reinterpret_cast<const char *>(&m_next_page_id_), sizeof(m_next_page_id_));

    // Serialize the free list
    uint32_t free_list_size = m_free_pages_.size();
    m_db_io_.write(reinterpret_cast<const char *>(&free_list_size), sizeof(free_list_size));
    for (page_id_t free_page : m_free_pages_)
    {
      m_db_io_.write(reinterpret_cast<const char *>(&free_page), sizeof(free_page));
    }

    // Pad the rest of the header page with zeros
    size_t header_size = sizeof(m_next_page_id_) + sizeof(free_list_size) + free_list_size * sizeof(page_id_t);
    if (header_size < kPageSize)
    {
      std::vector<char> padding(kPageSize - header_size, 0);
      m_db_io_.write(padding.data(), padding.size());
    }
    m_db_io_.flush();
  }

  void DiskManager::read_header()
  {
    m_db_io_.seekg(0);
    m_db_io_.read(reinterpret_cast<char *>(&m_next_page_id_), sizeof(m_next_page_id_));

    // Deserialize the free list
    uint32_t free_list_size;
    m_db_io_.read(reinterpret_cast<char *>(&free_list_size), sizeof(free_list_size));
    m_free_pages_.clear();
    for (uint32_t i = 0; i < free_list_size; ++i)
    {
      page_id_t free_page;
      m_db_io_.read(reinterpret_cast<char *>(&free_page), sizeof(free_page));
      m_free_pages_.insert(free_page);
    }
  }

  void DiskManager::write_physical_page(page_id_t physical_page_id, const char *page_data)
  {
    size_t offset = static_cast<size_t>(physical_page_id) * kPageSize;
    m_db_io_.seekp(offset);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error seeking to page " + std::to_string(physical_page_id));
    }
    m_db_io_.write(page_data, kPageSize);
    if (m_db_io_.fail())
    {
      throw std::runtime_error("Error writing to file");
    }
    m_db_io_.flush();
  }

  void DiskManager::read_physical_page(page_id_t physical_page_id, char *page_data)
  {
    size_t offset = static_cast<size_t>(physical_page_id) * kPageSize;
    m_db_io_.seekp(offset);
    m_db_io_.read(page_data, kPageSize);
    if (m_db_io_.fail() && !m_db_io_.eof())
    {
      throw std::runtime_error("Error reading from file");
    }
  }

} // namespace tinydb
