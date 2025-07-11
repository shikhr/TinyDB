#include "storage/table_heap.h"
#include "storage/table_page.h"

namespace tinydb
{

  TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id)
      : m_buffer_pool_manager_(buffer_pool_manager), m_first_page_id_(first_page_id) {}

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
    page_id_t new_page_id;
    Page *new_page_raw = m_buffer_pool_manager_->new_page(&new_page_id);
    if (new_page_raw == nullptr)
    {
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
    Page *page = m_buffer_pool_manager_->fetch_page(rid.page_id_);
    if (page == nullptr)
    {
      return false;
    }
    TablePage *table_page = reinterpret_cast<TablePage *>(page);
    bool result = table_page->update_record(record, rid);
    m_buffer_pool_manager_->unpin_page(rid.page_id_, result); // Mark dirty if update was successful
    return result;
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

} // namespace tinydb
