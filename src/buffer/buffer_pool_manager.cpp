#include "buffer/buffer_pool_manager.h"
#include <stdexcept>
#include <iostream>

namespace tinydb
{

  BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
      : m_pool_size_(pool_size), m_disk_manager_(disk_manager)
  {
    m_pages_ = new Page[m_pool_size_];
    m_replacer_ = std::make_unique<LRUReplacer>(m_pool_size_);

    for (size_t i = 0; i < m_pool_size_; ++i)
    {
      m_free_list_.push_back(static_cast<frame_id_t>(i));
    }
  }

  BufferPoolManager::~BufferPoolManager()
  {
    flush_all_pages();
    delete[] m_pages_;
  }

  bool BufferPoolManager::find_free_frame(frame_id_t *frame_id)
  {
    if (!m_free_list_.empty())
    {
      *frame_id = m_free_list_.front();
      m_free_list_.erase(m_free_list_.begin());
      return true;
    }
    return m_replacer_->victim(frame_id);
  }

  Page *BufferPoolManager::fetch_page(page_id_t page_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);

    if (m_page_table_.count(page_id))
    {
      frame_id_t frame_id = m_page_table_[page_id];
      m_pages_[frame_id].set_pin_count(m_pages_[frame_id].get_pin_count() + 1);
      m_replacer_->pin(frame_id);
      return &m_pages_[frame_id];
    }

    frame_id_t frame_id;
    if (!find_free_frame(&frame_id))
    {
      return nullptr; // No free frame available
    }

    Page *frame_page = &m_pages_[frame_id];
    if (frame_page->get_page_id() != -1)
    { // If the frame was occupied
      if (frame_page->is_dirty())
      {
        m_disk_manager_->write_page(frame_page->get_page_id(), frame_page->get_data());
      }
      m_page_table_.erase(frame_page->get_page_id());
    }

    // Read the page from disk.
    if (!m_disk_manager_->read_page(page_id, frame_page->get_data()))
    {
      // Page does not exist on disk, return the frame to the free list and fail.
      m_free_list_.push_back(frame_id);
      return nullptr;
    }

    // Update page metadata and page table.
    frame_page->set_page_id(page_id);
    frame_page->set_pin_count(1);
    frame_page->set_dirty(false);
    m_page_table_[page_id] = frame_id;
    m_replacer_->pin(frame_id);

    return frame_page;
  }

  bool BufferPoolManager::unpin_page(page_id_t page_id, bool is_dirty)
  {
    std::lock_guard<std::mutex> lock(m_latch_);

    if (!m_page_table_.count(page_id))
    {
      return false;
    }

    frame_id_t frame_id = m_page_table_[page_id];
    if (m_pages_[frame_id].get_pin_count() <= 0)
    {
      return false;
    }

    m_pages_[frame_id].set_pin_count(m_pages_[frame_id].get_pin_count() - 1);
    if (is_dirty)
    {
      m_pages_[frame_id].set_dirty(true);
    }

    if (m_pages_[frame_id].get_pin_count() == 0)
    {
      m_replacer_->unpin(frame_id);
    }

    return true;
  }

  bool BufferPoolManager::flush_page_unlocked(page_id_t page_id)
  {
    if (!m_page_table_.count(page_id))
    {
      return false;
    }

    frame_id_t frame_id = m_page_table_[page_id];
    m_disk_manager_->write_page(page_id, m_pages_[frame_id].get_data());
    m_pages_[frame_id].set_dirty(false);

    return true;
  }

  bool BufferPoolManager::flush_page(page_id_t page_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    return flush_page_unlocked(page_id);
  }

  void BufferPoolManager::flush_all_pages()
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    for (auto const &[page_id, frame_id] : m_page_table_)
    {
      flush_page_unlocked(page_id);
    }
  }

  Page *BufferPoolManager::new_page(page_id_t *page_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);

    frame_id_t frame_id;
    if (!find_free_frame(&frame_id))
    {
      return nullptr; // No free frame available
    }

    Page *frame_page = &m_pages_[frame_id];
    if (frame_page->get_page_id() != -1)
    { // If the frame was occupied
      if (frame_page->is_dirty())
      {
        m_disk_manager_->write_page(frame_page->get_page_id(), frame_page->get_data());
      }
      m_page_table_.erase(frame_page->get_page_id());
    }

    // Allocate a new page on disk.
    page_id_t new_page_id = m_disk_manager_->allocate_page();
    if (new_page_id == -1)
    {
      // Disk is full, return the frame to the free list and fail.
      m_free_list_.push_back(frame_id);
      return nullptr;
    }

    // Update page metadata and page table.
    frame_page->set_page_id(new_page_id);
    frame_page->set_pin_count(1);
    frame_page->set_dirty(false);
    m_page_table_[new_page_id] = frame_id;
    m_replacer_->pin(frame_id);

    *page_id = new_page_id;
    return frame_page;
  }

  bool BufferPoolManager::delete_page(page_id_t page_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);

    if (!m_page_table_.count(page_id))
    {
      // Page is not in buffer pool, just deallocate on disk
      m_disk_manager_->deallocate_page(page_id);
      return true;
    }

    frame_id_t frame_id = m_page_table_[page_id];
    Page *frame_page = &m_pages_[frame_id];

    if (frame_page->get_pin_count() > 0)
    {
      return false; // Page is pinned
    }

    // Remove from page table and add frame to free list
    m_page_table_.erase(page_id);
    m_replacer_->pin(frame_id); // To remove it from replacer's tracking
    m_free_list_.push_back(frame_id);

    // Reset page metadata
    frame_page->set_page_id(-1);
    frame_page->set_pin_count(0);
    frame_page->set_dirty(false);

    // Deallocate the page on disk
    m_disk_manager_->deallocate_page(page_id);

    return true;
  }

} // namespace tinydb
