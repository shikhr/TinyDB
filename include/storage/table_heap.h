#pragma once

#include "buffer/buffer_pool_manager.h"
#include "storage/table_page.h"
#include "storage/record.h"

namespace tinydb
{

  class TableHeap
  {
  public:
    TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id);

    // Insert a record into the table
    bool insert_record(const Record &record, RecordID *rid);

    // Delete a record from the table (tombstone)
    bool delete_record(const RecordID &rid);

    // Update a record
    bool update_record(const Record &record, const RecordID &rid);

    // Get a record
    bool get_record(const RecordID &rid, Record *record);

    // An iterator for sequential scans
    // class Iterator { ... };
    // Iterator begin();
    // Iterator end();

  private:
    BufferPoolManager *m_buffer_pool_manager_;
    page_id_t m_first_page_id_;
  };

} // namespace tinydb
