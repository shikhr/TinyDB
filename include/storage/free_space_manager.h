#pragma once

#include "common/config.h"
#include "buffer/buffer_pool_manager.h"

namespace tinydb
{

  /**
   * FreeSpaceManager manages page allocation and deallocation for the database.
   * It uses a bitmap stored on Page 1 to track which pages are allocated/free.
   * Each bit represents a page: 1 = allocated, 0 = free.
   *
   * This component owns the allocation policy - it decides whether to reuse
   * a previously deallocated page or allocate a new page ID.
   */
  class FreeSpaceManager
  {
  public:
    explicit FreeSpaceManager(BufferPoolManager *buffer_pool_manager);

    // Initialize the free space map (for new databases)
    bool initialize();

    // Allocate a new page and return its page_id
    // This method handles all allocation logic/policy
    page_id_t allocate_page();

    // Deallocate a page (mark it as free for future reuse)
    bool deallocate_page(page_id_t page_id);

    // Check if a page is allocated
    bool is_page_allocated(page_id_t page_id);

  private:
    static constexpr size_t kBitsPerByte = 8;
    static constexpr size_t kMaxPages = kPageSize * kBitsPerByte; // 32768 pages max

    BufferPoolManager *m_buffer_pool_manager_;

    // Helper methods
    bool get_bit(page_id_t page_id);
    bool set_bit(page_id_t page_id, bool value);
    page_id_t find_first_free_page_in_range();
  };

} // namespace tinydb
