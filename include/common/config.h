#pragma once

#include <cstdint>

static constexpr int kPageSize = 4096;
static constexpr int kBufferPoolSize = 10;

using page_id_t = int32_t;
using frame_id_t = int32_t;
