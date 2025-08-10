#include "storage/table_page.h"

#include <cstring>

namespace tinydb
{

  void TablePage::init(page_id_t page_id, page_id_t prev_page_id)
  {
    set_page_id(page_id);
    set_next_page_id(INVALID_PAGE_ID);
    set_num_records(0);
    set_free_space_pointer(kPageSize);
  }

  bool TablePage::insert_record(const Record &record, RecordID *rid)
  {
    // 1. Check if there is enough space.
    if (get_free_space_remaining() < record.get_size() + sizeof(RecordSlot))
    {
      return false;
    }

    // 2. Find an available slot. For now, we append to the end.
    slot_num_t slot_num = get_num_records();

    // 3. Calculate the new free space pointer and copy the record data.
    offset_t new_free_space_pointer = get_free_space_pointer() - record.get_size();
    memcpy(get_data() + new_free_space_pointer, record.get_data(), record.get_size());

    // 4. Update the slot information.
    RecordSlot *slot = get_slot(slot_num);
    slot->offset_ = new_free_space_pointer;
    slot->size_ = record.get_size();

    // 5. Update the page header.
    set_num_records(get_num_records() + 1);
    set_free_space_pointer(new_free_space_pointer);

    // 6. Set the output RecordID.
    rid->page_id_ = get_page_id();
    rid->slot_num_ = slot_num;

    return true;
  }

  bool TablePage::delete_record(const RecordID &rid)
  {
    if (rid.slot_num_ >= get_num_records())
    {
      return false;
    }

    RecordSlot *slot = get_slot(rid.slot_num_);
    if (slot->size_ == 0)
    {
      return false; // Already deleted
    }

    // Mark the slot as empty (tombstone)
    slot->size_ = 0;
    // Note: We are not reclaiming the space occupied by the record data yet.
    // Compaction would be needed for that.
    return true;
  }

  bool TablePage::update_record(const Record &record, const RecordID &rid)
  {
    if (rid.slot_num_ >= get_num_records())
    {
      return false;
    }

    RecordSlot *slot = get_slot(rid.slot_num_);
    if (slot->size_ == 0)
    {
      return false; // Record is deleted
    }

    if (record.get_size() <= static_cast<int>(slot->size_))
    {
      // Fits in existing space: overwrite and shrink if needed
      std::memcpy(get_data() + slot->offset_, record.get_data(), record.get_size());
      slot->size_ = record.get_size();
      return true;
    }

    // Need to grow: check if we have enough free space to place a new copy
    offset_t needed_extra = record.get_size() - slot->size_;
    if (get_free_space_remaining() < needed_extra)
    {
      // Not enough space on this page to grow
      return false;
    }

    // Allocate new space from the free space pointer and copy the full record
    offset_t new_free_space_pointer = get_free_space_pointer() - record.get_size();
    std::memcpy(get_data() + new_free_space_pointer, record.get_data(), record.get_size());

    // Update slot to point to the new location and size
    slot->offset_ = new_free_space_pointer;
    slot->size_ = record.get_size();

    // Commit new free space pointer; old bytes become fragmented space
    set_free_space_pointer(new_free_space_pointer);

    return true;
  }

  bool TablePage::get_record(const RecordID &rid, Record *record)
  {
    if (rid.slot_num_ >= get_num_records())
    {
      return false;
    }

    const RecordSlot *slot = get_slot(rid.slot_num_);
    if (slot->size_ == 0)
    {
      return false; // Record is deleted
    }

    // Construct the record object using placement new.
    new (record) Record(rid, slot->size_, get_data() + slot->offset_);
    return true;
  }

  // --- Header Accessors ---

  page_id_t TablePage::get_next_page_id() const
  {
    return *reinterpret_cast<const page_id_t *>(get_data() + kNextPageIdOffset);
  }

  void TablePage::set_next_page_id(page_id_t next_page_id)
  {
    memcpy(get_data() + kNextPageIdOffset, &next_page_id, sizeof(next_page_id));
  }

  slot_num_t TablePage::get_num_records() const
  {
    return *reinterpret_cast<const slot_num_t *>(get_data() + kNumRecordsOffset);
  }

  void TablePage::set_num_records(slot_num_t num_records)
  {
    memcpy(get_data() + kNumRecordsOffset, &num_records, sizeof(num_records));
  }

  offset_t TablePage::get_free_space_pointer() const
  {
    return *reinterpret_cast<const offset_t *>(get_data() + kFreeSpacePointerOffset);
  }

  void TablePage::set_free_space_pointer(offset_t free_space_pointer)
  {
    memcpy(get_data() + kFreeSpacePointerOffset, &free_space_pointer, sizeof(free_space_pointer));
  }

  // --- Slot Accessors ---

  RecordSlot *TablePage::get_slot(slot_num_t slot_num)
  {
    return reinterpret_cast<RecordSlot *>(get_data() + kSlotArrayOffset + slot_num * sizeof(RecordSlot));
  }

  const RecordSlot *TablePage::get_slot(slot_num_t slot_num) const
  {
    return reinterpret_cast<const RecordSlot *>(get_data() + kSlotArrayOffset + slot_num * sizeof(RecordSlot));
  }

  // --- Helper Functions ---

  offset_t TablePage::get_free_space_remaining() const
  {
    return get_free_space_pointer() - (kSlotArrayOffset + get_num_records() * sizeof(RecordSlot));
  }

} // namespace tinydb
