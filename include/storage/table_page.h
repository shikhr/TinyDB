#pragma once

#include "common/config.h"
#include "storage/page.h"
#include "storage/record.h"

namespace tinydb
{

  // Each page contains a header and a slot array. The header stores metadata
  // about the page, and the slot array stores metadata about the records.
  //
  // Page Header:
  // ----------------------------------------------------------------
  // | NextPageID (4) | NumRecords (4) | FreeSpacePointer (4) | ... |
  // ----------------------------------------------------------------
  //
  // Record Slot:
  // --------------------------
  // | Offset (4) | Size (4) |
  // --------------------------

  /**
   * RecordSlot stores the offset and size of a record within the page.
   */
  struct RecordSlot
  {
    offset_t offset_{0};
    uint32_t size_{0}; // If size is 0, the slot is considered empty/invalid.
  };

  /**
   * TablePage is a specialized page that implements the slotted page layout.
   * It manages the storage of records within a single page.
   *
   * Slotted Page Layout:
   * -------------------------------------------------------------------------------------
   * | Page Header | Slot Array (Record Directory) | Free Space | Record Data           |
   * -------------------------------------------------------------------------------------
   *                                               ^            ^
   *                                               |            |
   *                                               free_space_  record_data_
   *                                               pointer      pointer
   */
  class TablePage : public Page
  {
  public:
    void init(page_id_t page_id, page_id_t prev_page_id);

    // Record operations
    bool insert_record(const Record &record, RecordID *rid);
    bool delete_record(const RecordID &rid);
    bool update_record(const Record &record, const RecordID &rid);
    bool get_record(const RecordID &rid, Record *record);

    // Page header accessors
    page_id_t get_next_page_id() const;
    void set_next_page_id(page_id_t next_page_id);
    slot_num_t get_num_records() const;

  private:
    // The offset of the page header fields.
    static constexpr size_t kNextPageIdOffset = 0;
    static constexpr size_t kNumRecordsOffset = sizeof(page_id_t);
    static constexpr size_t kFreeSpacePointerOffset = kNumRecordsOffset + sizeof(slot_num_t);
    static constexpr size_t kPageHeaderSize = kFreeSpacePointerOffset + sizeof(offset_t);
    static constexpr size_t kSlotArrayOffset = kPageHeaderSize;

    // Page header accessors
    void set_num_records(slot_num_t num_records);

    offset_t get_free_space_pointer() const;
    void set_free_space_pointer(offset_t free_space_pointer);

    // Slot accessors
    RecordSlot *get_slot(slot_num_t slot_num);
    const RecordSlot *get_slot(slot_num_t slot_num) const;

    // Helper functions
    offset_t get_free_space_remaining() const;
  };

} // namespace tinydb
