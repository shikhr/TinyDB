#pragma once

#include "storage/disk_manager.h"
#include "storage/page.h"
#include "buffer/lru_replacer.h"
#include <vector>
#include <unordered_map>
#include <mutex>

namespace tinydb
{

  // Forward declaration to break circular dependency
  class FreeSpaceManager;

  /**
   * The BufferPoolManager is responsible for managing the buffer pool,
   * which is a collection of pages in memory that can be used to cache
   * frequently accessed data from disk. It handles page fetching, pinning,
   * unpinning, and flushing pages to disk.
   * It also interacts with the DiskManager for low-level I/O operations.
   *
   * The BufferPoolManager uses a Least Recently Used (LRU) replacement policy
   * to manage the eviction of pages from the buffer pool.
   */

  class BufferPoolManager
  {
  public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager);
    ~BufferPoolManager();

    Page *fetch_page(page_id_t page_id);
    Page *new_page(page_id_t page_id);
    bool unpin_page(page_id_t page_id, bool is_dirty);
    bool flush_page(page_id_t page_id);
    void flush_all_pages();
    bool delete_page(page_id_t page_id);

    // Access to disk manager for coordination (non-owning)
    DiskManager *get_disk_manager() { return m_disk_manager_; }

  private:
    size_t m_pool_size_;                                     // Total number of frames in the buffer pool
    DiskManager *m_disk_manager_;                            // Non-owning: DiskManager is owned by higher layer
    Page *m_pages_;                                          // Raw array of pages (TODO: migrate to std::vector<Page>)
    std::vector<frame_id_t> m_free_list_;                    // List of free frames in the buffer pool
    std::unordered_map<page_id_t, frame_id_t> m_page_table_; // Maps page IDs to frame IDs
    std::unique_ptr<LRUReplacer> m_replacer_;                // LRU replacer for managing page eviction
    std::mutex m_latch_;                                     // Mutex for synchronizing access to the buffer pool

    bool find_free_frame(frame_id_t *frame_id);
    bool flush_page_unlocked(page_id_t page_id);
  };

} // namespace tinydb
