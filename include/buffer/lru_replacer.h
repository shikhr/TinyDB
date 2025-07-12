#pragma once

#include "common/config.h"
#include <list>
#include <mutex>
#include <unordered_map>
#include <memory>

namespace tinydb
{

  /**
   * LRUReplacer implements a Least Recently Used (LRU) page replacement policy.
   * It maintains a list of pages in the order they were accessed, allowing
   * efficient eviction of the least recently used page when necessary.
   */
  class LRUReplacer
  {
  public:
    explicit LRUReplacer(size_t num_pages);

    ~LRUReplacer() = default;

    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t size();

  private:
    std::list<frame_id_t> m_lru_list_;
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> m_lru_map_;
    std::mutex m_latch_;
    size_t m_capacity_;
  };

} // namespace tinydb
