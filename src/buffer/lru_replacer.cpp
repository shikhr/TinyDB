#include "buffer/lru_replacer.h"

namespace tinydb
{

  LRUReplacer::LRUReplacer(size_t num_pages) : m_capacity_(num_pages) {}

  bool LRUReplacer::victim(frame_id_t *frame_id)
  {
    // only removes the lru page from the replaces, not from the buffer pool
    std::lock_guard<std::mutex> lock(m_latch_);
    if (m_lru_list_.empty())
    {
      return false;
    }

    *frame_id = m_lru_list_.back();
    m_lru_map_.erase(*frame_id);
    m_lru_list_.pop_back();

    return true;
  }

  void LRUReplacer::pin(frame_id_t frame_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    auto it = m_lru_map_.find(frame_id);
    if (it != m_lru_map_.end())
    {
      m_lru_list_.erase(it->second);
      m_lru_map_.erase(it);
    }
  }

  void LRUReplacer::unpin(frame_id_t frame_id)
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    auto it = m_lru_map_.find(frame_id);
    if (it != m_lru_map_.end())
    {
      m_lru_list_.erase(it->second);
      m_lru_list_.push_front(frame_id);
      m_lru_map_[frame_id] = m_lru_list_.begin();
      return;
    }

    if (m_lru_list_.size() >= m_capacity_)
    {
      return;
    }
    m_lru_list_.push_front(frame_id);
    m_lru_map_[frame_id] = m_lru_list_.begin();
  }

  size_t LRUReplacer::size()
  {
    std::lock_guard<std::mutex> lock(m_latch_);
    return m_lru_list_.size();
  }

} // namespace tinydb
