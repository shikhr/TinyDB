#pragma once

#include "common/config.h"
#include <cstring>
#include <memory>

namespace tinydb
{

  class Page
  {
  public:
    Page();
    ~Page() = default;

    char *get_data();
    const char *get_data() const;
    page_id_t get_page_id() const;
    int get_pin_count() const;
    bool is_dirty() const;

    void set_page_id(page_id_t page_id);
    void set_pin_count(int pin_count);
    void set_dirty(bool is_dirty);

  private:
    char m_data_[kPageSize]{};
    page_id_t m_page_id_ = -1;
    int m_pin_count_ = 0;
    bool m_is_dirty_ = false;
  };

} // namespace tinydb
