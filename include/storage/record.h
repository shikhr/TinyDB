#pragma once

#include "common/config.h"

namespace tinydb
{

  /**
   * A RecordID is a unique identifier for a record within a table.
   * It consists of the page ID and the slot number within that page.
   */
  struct RecordID
  {
    page_id_t page_id_{INVALID_PAGE_ID};
    int slot_num_{-1}; // -1 indicates an invalid slot number

    RecordID() = default;
    RecordID(page_id_t page_id, int slot_num) : page_id_(page_id), slot_num_(slot_num) {}

    // Comparison operators
    bool operator==(const RecordID &other) const
    {
      return page_id_ == other.page_id_ && slot_num_ == other.slot_num_;
    }
  };

  /**
   * A Record represents a single tuple or row in a table.
   * For now, it holds a pointer to the data and its size.
   * The actual data is owned by the page it resides on.
   */
  class Record
  {
  public:
    Record(RecordID rid, int size, char *data) : m_rid_(rid), m_size_(size), m_data_(data) {}

    RecordID get_rid() const { return m_rid_; }
    char *get_data() const { return m_data_; }
    int get_size() const { return m_size_; }

  private:
    RecordID m_rid_;
    int m_size_;
    char *m_data_{nullptr};
    // We might add a std::unique_ptr<char[]> later if a record needs to own its data.
  };

} // namespace tinydb
