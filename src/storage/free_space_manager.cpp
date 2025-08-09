#include "storage/free_space_manager.h"
#include "storage/db_header_page.h"
#include <cstring>

namespace tinydb
{

  FreeSpaceManager::FreeSpaceManager(BufferPoolManager *buffer_pool_manager)
      : m_buffer_pool_manager_(buffer_pool_manager) {}

  bool FreeSpaceManager::initialize()
  {
    // First, ensure Header page exists and is initialized
    Page *header_page = m_buffer_pool_manager_->fetch_page(kHeaderPageId);
    if (header_page == nullptr)
    {
      // Header page doesn't exist - create and initialize it
      header_page = m_buffer_pool_manager_->new_page(kHeaderPageId);
      if (header_page == nullptr)
      {
        return false;
      }

      DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page->get_data());
      header->init();
      m_buffer_pool_manager_->unpin_page(kHeaderPageId, true); // Mark as dirty
    }
    else
    {
      m_buffer_pool_manager_->unpin_page(kHeaderPageId, false);
    }

    // Then, handle the free space map page (FSM)
    Page *fs_page = m_buffer_pool_manager_->fetch_page(kFreeSpaceMapPageId);

    if (fs_page == nullptr)
    {
      // FSM page doesn't exist yet - this is a new database
      fs_page = m_buffer_pool_manager_->new_page(kFreeSpaceMapPageId);
      if (fs_page == nullptr)
      {
        return false;
      }

      // Initialize the bitmap for a new database
      uint8_t *bitmap = reinterpret_cast<uint8_t *>(fs_page->get_data());
      // Mark Header and FSM pages as allocated
      bitmap[0] |= 0x03; // Set bits 0 and 1

      m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, true); // Mark as dirty
    }
    else
    {
      // FSM exists - this is an existing database, just unpin it
      m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, false);
    }

    return true;
  }

  page_id_t FreeSpaceManager::allocate_page()
  {
    // First, check if we have any previously deallocated pages that can be reused
    page_id_t reused_page_id = find_first_free_page_in_range();
    if (reused_page_id != INVALID_PAGE_ID)
    {
      // Mark the page as allocated in our bitmap
      if (set_bit(reused_page_id, true))
      {
        return reused_page_id;
      }
    }

    // No free pages available for reuse, need to allocate a new page
    // Get current page count (high watermark) from the header page
    Page *header_page = m_buffer_pool_manager_->fetch_page(kHeaderPageId);
    if (header_page == nullptr)
    {
      return INVALID_PAGE_ID;
    }

    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page->get_data());
    page_id_t new_page_id = header->get_page_count();
    header->set_page_count(new_page_id + 1);                 // Update high watermark
    m_buffer_pool_manager_->unpin_page(kHeaderPageId, true); // Mark as dirty

    // Mark the page as allocated in our bitmap
    if (!set_bit(new_page_id, true))
    {
      return INVALID_PAGE_ID;
    }

    return new_page_id;
  }

  bool FreeSpaceManager::deallocate_page(page_id_t page_id)
  {
    // Cannot deallocate system pages
    if (page_id == kHeaderPageId || page_id == kFreeSpaceMapPageId)
    {
      return false;
    }

    return set_bit(page_id, false);
  }

  bool FreeSpaceManager::is_page_allocated(page_id_t page_id)
  {
    return get_bit(page_id);
  }

  bool FreeSpaceManager::get_bit(page_id_t page_id)
  {
    if (page_id >= kMaxPages)
    {
      return false;
    }

    Page *fs_page = m_buffer_pool_manager_->fetch_page(kFreeSpaceMapPageId);
    if (fs_page == nullptr)
    {
      return false;
    }

    uint8_t *bitmap = reinterpret_cast<uint8_t *>(fs_page->get_data());
    size_t byte_index = page_id / kBitsPerByte;
    size_t bit_index = page_id % kBitsPerByte;

    bool is_set = (bitmap[byte_index] & (1 << bit_index)) != 0;
    m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, false);

    return is_set;
  }

  bool FreeSpaceManager::set_bit(page_id_t page_id, bool value)
  {
    if (page_id >= kMaxPages)
    {
      return false;
    }

    Page *fs_page = m_buffer_pool_manager_->fetch_page(kFreeSpaceMapPageId);
    if (fs_page == nullptr)
    {
      return false;
    }

    uint8_t *bitmap = reinterpret_cast<uint8_t *>(fs_page->get_data());
    size_t byte_index = page_id / kBitsPerByte;
    size_t bit_index = page_id % kBitsPerByte;

    if (value)
    {
      bitmap[byte_index] |= (1 << bit_index); // Set bit
    }
    else
    {
      bitmap[byte_index] &= ~(1 << bit_index); // Clear bit
    }

    m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, true); // Mark as dirty
    return true;
  }

  page_id_t FreeSpaceManager::find_first_free_page_in_range()
  {
    // Only look for pages that were previously allocated and then deallocated
    // Use the high watermark from the header page to know how many pages have been allocated

    // Get the current page count (high watermark) from the header page
    Page *header_page = m_buffer_pool_manager_->fetch_page(kHeaderPageId);
    if (header_page == nullptr)
    {
      return INVALID_PAGE_ID;
    }

    DBHeaderPage *header = reinterpret_cast<DBHeaderPage *>(header_page->get_data());
    uint32_t page_count = header->get_page_count();
    m_buffer_pool_manager_->unpin_page(kHeaderPageId, false);

    Page *fs_page = m_buffer_pool_manager_->fetch_page(kFreeSpaceMapPageId);
    if (fs_page == nullptr)
    {
      return INVALID_PAGE_ID;
    }

    uint8_t *bitmap = reinterpret_cast<uint8_t *>(fs_page->get_data());

    // Only scan up to the high watermark (pages that have been allocated before)
    for (page_id_t page_id = kFirstDataPageId; page_id < page_count; ++page_id) // Skip header and FSM
    {
      size_t byte_index = page_id / kBitsPerByte;
      size_t bit_index = page_id % kBitsPerByte;

      if (byte_index >= kPageSize)
        break; // Don't go beyond bitmap bounds

      if ((bitmap[byte_index] & (1 << bit_index)) == 0) // Bit is free (previously allocated, then deallocated)
      {
        m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, false);
        return page_id;
      }
    }

    m_buffer_pool_manager_->unpin_page(kFreeSpaceMapPageId, false);
    return INVALID_PAGE_ID; // No free pages found in allocated range
  }

} // namespace tinydb
