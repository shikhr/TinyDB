#include "storage/disk_manager.h"
#include <stdexcept>
#include <filesystem>

DiskManager::DiskManager(const std::string &db_file) : m_file_name_(db_file)
{
  bool file_exists = std::filesystem::exists(m_file_name_);

  // Open the file with appropriate flags.
  m_db_file_.open(m_file_name_, std::ios::in | std::ios::out | std::ios::binary);

  if (!m_db_file_.is_open())
  {
    // We can try creating it explicitly with std::ios::out, which creates the file.
    m_db_file_.open(m_file_name_, std::ios::out | std::ios::binary);
    m_db_file_.close();
    // And now open it in the desired read/write mode.
    m_db_file_.open(m_file_name_, std::ios::in | std::ios::out | std::ios::binary);
  }

  if (!m_db_file_.is_open())
  {
    // If it still fails, maybe the directory doesn't exist. For simplicity, we'll throw.
    throw std::runtime_error("Could not create or open database file: " + db_file);
  }

  m_db_file_.seekg(0, std::ios::end);
  size_t file_size = m_db_file_.tellg();
  m_next_page_id_ = file_size / kPageSize;
}

DiskManager::~DiskManager()
{
  if (m_db_file_.is_open())
  {
    m_db_file_.close();
  }
}

bool DiskManager::validate_page_id(page_id_t page_id) const
{
  // Check if the page_id is within valid range
  return page_id >= 0 && page_id < m_next_page_id_;
}

void DiskManager::write_page(page_id_t page_id, const char *page_data)
{
  // if page_id is out of bounds, throw an exception
  if (!validate_page_id(page_id))
    throw std::out_of_range("Page ID is out of bounds");
  // Ensure the page_data is not null
  if (page_data == nullptr)
    throw std::invalid_argument("Page data cannot be null");

  // Lock the mutex to ensure exclusive access to the file
  std::lock_guard<std::mutex> lock(m_latch_);

  // Move the file pointer and write the page data
  m_db_file_.seekp(page_id * kPageSize);
  m_db_file_.write(page_data, kPageSize);
  if (m_db_file_.fail())
  {
    throw std::runtime_error("Error writing to file");
  }
  m_db_file_.flush();
}

void DiskManager::read_page(page_id_t page_id, char *page_data)
{
  if (!validate_page_id(page_id))
    throw std::out_of_range("Page ID is out of bounds");
  // Ensure the page_data is not null
  if (page_data == nullptr)
    throw std::invalid_argument("Page data cannot be null");

  // Lock the mutex to ensure exclusive access to the file
  std::lock_guard<std::mutex> lock(m_latch_);
  // Move the file pointer and read the page data
  m_db_file_.seekg(page_id * kPageSize);
  m_db_file_.read(page_data, kPageSize);
  if (m_db_file_.fail() && !m_db_file_.eof())
  {
    throw std::runtime_error("Error reading from file");
  }
}

page_id_t DiskManager::allocate_page()
{
  std::lock_guard<std::mutex> lock(m_latch_);
  return m_next_page_id_++;
}

void DiskManager::deallocate_page(page_id_t page_id)
{
  // For now, we don't reuse page ids.
}
