#include "storage/page.h"

Page::Page() = default;

char *Page::get_data()
{
  return m_data_;
}

page_id_t Page::get_page_id() const
{
  return m_page_id_;
}

int Page::get_pin_count() const
{
  return m_pin_count_;
}

bool Page::is_dirty() const
{
  return m_is_dirty_;
}

void Page::set_page_id(page_id_t page_id)
{
  m_page_id_ = page_id;
}

void Page::set_pin_count(int pin_count)
{
  m_pin_count_ = pin_count;
}

void Page::set_dirty(bool is_dirty)
{
  m_is_dirty_ = is_dirty;
}
