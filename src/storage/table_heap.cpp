#include "storage/table_heap.h"
#include "storage/table_page.h"
#include "storage/free_space_manager.h"

namespace tinydb
{

  TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, FreeSpaceManager *free_space_manager)
      : m_buffer_pool_manager_(buffer_pool_manager), m_free_space_manager_(free_space_manager), m_first_page_id_(first_page_id) {}

  bool TableHeap::insert_record(const Record &record, RecordID *rid)
  {
    page_id_t current_page_id = m_first_page_id_;
    page_id_t last_page_id = INVALID_PAGE_ID; // Track the last valid page

    while (current_page_id != INVALID_PAGE_ID)
    {
      Page *page = m_buffer_pool_manager_->fetch_page(current_page_id);
      if (page == nullptr)
      {
        return false;
      }
      TablePage *table_page = reinterpret_cast<TablePage *>(page);

      if (table_page->insert_record(record, rid))
      {
        m_buffer_pool_manager_->unpin_page(current_page_id, true); // Mark as dirty
        return true;
      }

      // If the current page is full, move to the next page
      last_page_id = current_page_id; // Remember this page as the last valid one
      page_id_t next_page_id = table_page->get_next_page_id();
      m_buffer_pool_manager_->unpin_page(current_page_id, false);
      current_page_id = next_page_id;
    }

    // If all existing pages are full, create a new page
    if (m_free_space_manager_ == nullptr)
    {
      return false; // Cannot allocate new pages without FreeSpaceManager
    }

    // Step 1: Get page ID
    page_id_t new_page_id = m_free_space_manager_->allocate_page();
    if (new_page_id == INVALID_PAGE_ID)
    {
      return false;
    }

    // Step 2: Get page frame and initialize
    Page *new_page_raw = m_buffer_pool_manager_->new_page(new_page_id);
    if (new_page_raw == nullptr)
    {
      m_free_space_manager_->deallocate_page(new_page_id);
      return false;
    }
    TablePage *new_table_page = reinterpret_cast<TablePage *>(new_page_raw);
    new_table_page->init(new_page_id, INVALID_PAGE_ID); // No previous page reference needed
    new_table_page->insert_record(record, rid);

    // Update the next_page_id of the last page in the chain
    if (m_first_page_id_ == INVALID_PAGE_ID)
    {
      // This is the first page in the table
      m_first_page_id_ = new_page_id;
    }
    else
    {
      // Link the new page to the end of the existing chain
      Page *last_page_raw = m_buffer_pool_manager_->fetch_page(last_page_id);
      TablePage *last_table_page = reinterpret_cast<TablePage *>(last_page_raw);
      last_table_page->set_next_page_id(new_page_id);
      m_buffer_pool_manager_->unpin_page(last_page_id, true);
    }

    m_buffer_pool_manager_->unpin_page(new_page_id, true);
    return true;
  }

  bool TableHeap::delete_record(const RecordID &rid)
  {
    Page *page = m_buffer_pool_manager_->fetch_page(rid.page_id_);
    if (page == nullptr)
    {
      return false;
    }
    TablePage *table_page = reinterpret_cast<TablePage *>(page);
    bool result = table_page->delete_record(rid);
    m_buffer_pool_manager_->unpin_page(rid.page_id_, result); // Mark dirty if delete was successful
    return result;
  }

  bool TableHeap::update_record(const Record &record, const RecordID &rid)
  {
    // First attempt in-place update on the same page
    Page *page = m_buffer_pool_manager_->fetch_page(rid.page_id_);
    if (page == nullptr)
    {
      return false;
    }
    TablePage *table_page = reinterpret_cast<TablePage *>(page);
    bool in_place = table_page->update_record(record, rid);
    m_buffer_pool_manager_->unpin_page(rid.page_id_, in_place);
    if (in_place)
    {
      return true;
    }

    // Fallback: delete the old record and insert the new one via heap logic
    if (!delete_record(rid))
    {
      return false;
    }

    RecordID new_rid;
    if (!insert_record(record, &new_rid))
    {
      // Insert failed after delete; report failure (record remains deleted)
      return false;
    }

    return true;
  }

  bool TableHeap::get_record(const RecordID &rid, Record *record)
  {
    Page *page = m_buffer_pool_manager_->fetch_page(rid.page_id_);
    if (page == nullptr)
    {
      return false;
    }
    TablePage *table_page = reinterpret_cast<TablePage *>(page);
    bool result = table_page->get_record(rid, record);
    m_buffer_pool_manager_->unpin_page(rid.page_id_, false); // No need to mark dirty for a read
    return result;
  }

  // Iterator implementation
  TableHeap::Iterator::Iterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, slot_num_t slot_id)
      : m_buffer_pool_manager_(buffer_pool_manager), m_current_page_id_(page_id),
        m_current_slot_id_(slot_id), m_record_loaded_(false)
  {
    if (m_current_page_id_ != INVALID_PAGE_ID)
    {
      advance_to_next_valid_record();
    }
  }

  const Record &TableHeap::Iterator::operator*() const
  {
    if (!m_record_loaded_)
    {
      // Load the current record
      RecordID rid(m_current_page_id_, m_current_slot_id_);
      Page *page = m_buffer_pool_manager_->fetch_page(m_current_page_id_);
      if (page != nullptr)
      {
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->get_record(rid, &m_current_record_);
        m_buffer_pool_manager_->unpin_page(m_current_page_id_, false);
        m_record_loaded_ = true;
      }
    }
    return m_current_record_;
  }

  const Record *TableHeap::Iterator::operator->() const
  {
    return &(operator*());
  }

  TableHeap::Iterator &TableHeap::Iterator::operator++()
  {
    m_record_loaded_ = false;
    ++m_current_slot_id_;
    advance_to_next_valid_record();
    return *this;
  }

  bool TableHeap::Iterator::operator==(const Iterator &other) const
  {
    return m_current_page_id_ == other.m_current_page_id_ &&
           m_current_slot_id_ == other.m_current_slot_id_;
  }

  bool TableHeap::Iterator::operator!=(const Iterator &other) const
  {
    return !(*this == other);
  }

  void TableHeap::Iterator::advance_to_next_valid_record()
  {
    while (m_current_page_id_ != INVALID_PAGE_ID)
    {
      Page *page = m_buffer_pool_manager_->fetch_page(m_current_page_id_);
      if (page == nullptr)
      {
        m_current_page_id_ = INVALID_PAGE_ID;
        break;
      }

      TablePage *table_page = reinterpret_cast<TablePage *>(page);

      // Check if current slot has a valid record
      RecordID rid(m_current_page_id_, m_current_slot_id_);
      Record temp_record;
      if (table_page->get_record(rid, &temp_record))
      {
        m_buffer_pool_manager_->unpin_page(m_current_page_id_, false);
        return; // Found valid record
      }

      // Try next slot
      ++m_current_slot_id_;

      // If we've exceeded the slots on this page, move to next page
      if (m_current_slot_id_ >= table_page->get_num_records())
      {
        page_id_t next_page_id = table_page->get_next_page_id();
        m_buffer_pool_manager_->unpin_page(m_current_page_id_, false);
        m_current_page_id_ = next_page_id;
        m_current_slot_id_ = 0;
      }
      else
      {
        m_buffer_pool_manager_->unpin_page(m_current_page_id_, false);
      }
    }
  }

  TableHeap::Iterator TableHeap::begin()
  {
    return Iterator(m_buffer_pool_manager_, m_first_page_id_, 0);
  }

  TableHeap::Iterator TableHeap::end()
  {
    return Iterator(m_buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }

} // namespace tinydb
