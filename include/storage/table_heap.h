#pragma once

#include "buffer/buffer_pool_manager.h"
#include "storage/table_page.h"
#include "storage/record.h"

namespace tinydb
{

  // Forward declaration
  class FreeSpaceManager;

  /**
   * The TableHeap class represents a table stored in the database.
   * It manages the storage and retrieval of records in the table.
   * It provides methods for inserting, deleting, updating, and iterating over records.
   * It uses TablePage to manage individual pages of the table.
   * It also interacts with the FreeSpaceManager to manage free space within the table.
   */
  class TableHeap
  {
  public:
    TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, FreeSpaceManager *free_space_manager = nullptr);

    // Insert a record into the table
    bool insert_record(const Record &record, RecordID *rid);

    // Delete a record from the table (tombstone)
    bool delete_record(const RecordID &rid);

    // Update a record
    bool update_record(const Record &record, const RecordID &rid);

    // Get a record
    bool get_record(const RecordID &rid, Record *record);

    // Iterator for sequential scans
    class Iterator
    {
    public:
      Iterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, slot_num_t slot_id);
      Iterator(const Iterator &other) = default;
      Iterator &operator=(const Iterator &other) = default;

      const Record &operator*() const;
      const Record *operator->() const;
      Iterator &operator++();
      bool operator==(const Iterator &other) const;
      bool operator!=(const Iterator &other) const;

    private:
      void advance_to_next_valid_record();

      BufferPoolManager *m_buffer_pool_manager_;
      page_id_t m_current_page_id_;
      slot_num_t m_current_slot_id_;
      mutable Record m_current_record_; // Cache for current record
      mutable bool m_record_loaded_;
    };

    Iterator begin();
    Iterator end();

  private:
    BufferPoolManager *m_buffer_pool_manager_;
    FreeSpaceManager *m_free_space_manager_;
    page_id_t m_first_page_id_;
  };

} // namespace tinydb
