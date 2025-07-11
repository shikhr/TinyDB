#pragma once

#include <cstdint>

namespace tinydb
{

  static constexpr int kPageSize = 4096;
  static constexpr int kBufferPoolSize = 10;

  using page_id_t = int32_t;
  using frame_id_t = int32_t;
  using table_id_t = int32_t;

  // Slotted page related types
  using slot_num_t = uint32_t;
  using offset_t = uint32_t;

  static constexpr page_id_t INVALID_PAGE_ID = -1;

} // namespace tinydb
