#include "storage/disk_manager.h"
#include <cassert>
#include <stdexcept>
#include <sys/stat.h>
#include <vector>

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
  if (!validate_page_id(page_id))
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
  // A page is not valid if it has been deallocated.
  return m_free_pages_.find(page_id) == m_free_pages_.end();
}

void DiskManager::write_header()
{
  char header_data[kPageSize] = {0};

  // Serialize m_next_page_id_
  memcpy(header_data, &m_next_page_id_, sizeof(m_next_page_id_));
  char *ptr = header_data + sizeof(m_next_page_id_);

  // Serialize number of free pages
  size_t free_pages_count = m_free_pages_.size();
  memcpy(ptr, &free_pages_count, sizeof(free_pages_count));
  ptr += sizeof(free_pages_count);

  // Serialize the free pages themselves by converting set to vector
  if (free_pages_count > 0)
  {
    std::vector<page_id_t> free_pages_vec(m_free_pages_.begin(), m_free_pages_.end());
    memcpy(ptr, free_pages_vec.data(), free_pages_count * sizeof(page_id_t));
  }

  // Write to page 0
  write_physical_page(0, header_data);
}

void DiskManager::read_header()
{
  char header_data[kPageSize] = {0};

  // Read from page 0
  read_physical_page(0, header_data);

  // Deserialize m_next_page_id_
  memcpy(&m_next_page_id_, header_data, sizeof(m_next_page_id_));
  char *ptr = header_data + sizeof(m_next_page_id_);

  // Deserialize number of free pages
  size_t free_pages_count;
  memcpy(&free_pages_count, ptr, sizeof(free_pages_count));
  ptr += sizeof(free_pages_count);

  // Deserialize the free pages
  if (free_pages_count > 0)
  {
    std::vector<page_id_t> free_pages_vec(free_pages_count);
    memcpy(free_pages_vec.data(), ptr, free_pages_count * sizeof(page_id_t));
    m_free_pages_ = std::unordered_set<page_id_t>(free_pages_vec.begin(), free_pages_vec.end());
  }
}

void DiskManager::write_physical_page(page_id_t physical_page_id, const char *page_data)
{
  m_db_io_.seekp(physical_page_id * kPageSize);
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
  m_db_io_.seekg(physical_page_id * kPageSize);
  if (m_db_io_.fail())
  {
    throw std::runtime_error("Error seeking to page " + std::to_string(physical_page_id));
  }
  m_db_io_.read(page_data, kPageSize);
  if (m_db_io_.fail() && !m_db_io_.eof())
  {
    throw std::runtime_error("Error reading from file");
  }
}
