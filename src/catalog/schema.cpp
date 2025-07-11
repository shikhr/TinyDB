#include "catalog/schema.h"
#include <cstring>

namespace tinydb
{

  // Value class implementation
  Value::Value() : m_type_(ColumnType::INVALID) {}

  Value::Value(int32_t int_val) : m_type_(ColumnType::INTEGER), m_integer_val_(int_val) {}

  Value::Value(std::string str_val) : m_type_(ColumnType::VARCHAR), m_string_val_(std::move(str_val)) {}

  int32_t Value::get_integer() const
  {
    if (m_type_ != ColumnType::INTEGER)
    {
      throw std::runtime_error("Value is not an INTEGER");
    }
    return m_integer_val_;
  }

  const std::string &Value::get_string() const
  {
    if (m_type_ != ColumnType::VARCHAR)
    {
      throw std::runtime_error("Value is not a VARCHAR");
    }
    return m_string_val_;
  }

  size_t Value::get_serialized_size() const
  {
    switch (m_type_)
    {
    case ColumnType::INTEGER:
      return sizeof(int32_t);
    case ColumnType::VARCHAR:
      return sizeof(uint32_t) + m_string_val_.size(); // length + data
    case ColumnType::INVALID:
      return 0;
    default:
      throw std::runtime_error("Unknown column type");
    }
  }

  bool Value::operator==(const Value &other) const
  {
    if (m_type_ != other.m_type_)
      return false;

    switch (m_type_)
    {
    case ColumnType::INTEGER:
      return m_integer_val_ == other.m_integer_val_;
    case ColumnType::VARCHAR:
      return m_string_val_ == other.m_string_val_;
    case ColumnType::INVALID:
      return true; // Both are NULL
    default:
      return false;
    }
  }

  // Column class implementation
  size_t Column::get_fixed_size() const
  {
    switch (m_type_)
    {
    case ColumnType::INTEGER:
      return sizeof(int32_t);
    case ColumnType::VARCHAR:
      return 0; // Variable length
    case ColumnType::INVALID:
      return 0;
    default:
      throw std::runtime_error("Unknown column type");
    }
  }

  bool Column::is_variable_length() const
  {
    return m_type_ == ColumnType::VARCHAR;
  }

  // Schema class implementation
  std::optional<size_t> Schema::get_column_index(const std::string &name) const
  {
    for (size_t i = 0; i < m_columns_.size(); ++i)
    {
      if (m_columns_[i].get_name() == name)
      {
        return i;
      }
    }
    return std::nullopt;
  }

  size_t Schema::calculate_record_size(const std::vector<Value> &values) const
  {
    if (values.size() != m_columns_.size())
    {
      throw std::runtime_error("Number of values does not match schema");
    }

    size_t total_size = calculate_header_size();

    for (size_t i = 0; i < values.size(); ++i)
    {
      if (!values[i].is_null())
      {
        total_size += values[i].get_serialized_size();
      }
    }

    return total_size;
  }

  std::vector<char> Schema::serialize_record(const std::vector<Value> &values) const
  {
    if (values.size() != m_columns_.size())
    {
      throw std::runtime_error("Number of values does not match schema");
    }

    size_t record_size = calculate_record_size(values);
    std::vector<char> buffer(record_size);

    size_t offset = 0;

    // Write null bitmap
    size_t null_bitmap_size = get_null_bitmap_size();
    std::vector<uint8_t> null_bitmap((null_bitmap_size + 7) / 8, 0);

    for (size_t i = 0; i < values.size(); ++i)
    {
      if (values[i].is_null())
      {
        size_t byte_index = i / 8;
        size_t bit_index = i % 8;
        null_bitmap[byte_index] |= (1 << bit_index);
      }
    }

    std::memcpy(buffer.data() + offset, null_bitmap.data(), null_bitmap.size());
    offset += null_bitmap.size();

    // Write variable-length column offsets (if any)
    size_t var_col_count = 0;
    for (const auto &col : m_columns_)
    {
      if (col.is_variable_length())
      {
        var_col_count++;
      }
    }

    if (var_col_count > 0)
    {
      // Reserve space for variable column offsets
      std::vector<uint32_t> var_offsets(var_col_count);
      size_t var_index = 0;
      size_t data_offset = offset + var_col_count * sizeof(uint32_t);

      // Write fixed-length columns first, then variable-length
      for (size_t i = 0; i < values.size(); ++i)
      {
        if (!values[i].is_null() && !m_columns_[i].is_variable_length())
        {
          // Fixed-length column
          if (values[i].get_type() == ColumnType::INTEGER)
          {
            int32_t int_val = values[i].get_integer();
            std::memcpy(buffer.data() + data_offset, &int_val, sizeof(int32_t));
            data_offset += sizeof(int32_t);
          }
        }
      }

      // Write variable-length columns
      for (size_t i = 0; i < values.size(); ++i)
      {
        if (!values[i].is_null() && m_columns_[i].is_variable_length())
        {
          var_offsets[var_index] = data_offset;

          if (values[i].get_type() == ColumnType::VARCHAR)
          {
            const std::string &str_val = values[i].get_string();
            uint32_t str_len = str_val.size();
            std::memcpy(buffer.data() + data_offset, &str_len, sizeof(uint32_t));
            data_offset += sizeof(uint32_t);
            std::memcpy(buffer.data() + data_offset, str_val.data(), str_len);
            data_offset += str_len;
          }
          var_index++;
        }
      }

      // Write variable column offsets
      std::memcpy(buffer.data() + offset, var_offsets.data(), var_col_count * sizeof(uint32_t));
    }
    else
    {
      // No variable-length columns, just write fixed-length data
      for (size_t i = 0; i < values.size(); ++i)
      {
        if (!values[i].is_null())
        {
          if (values[i].get_type() == ColumnType::INTEGER)
          {
            int32_t int_val = values[i].get_integer();
            std::memcpy(buffer.data() + offset, &int_val, sizeof(int32_t));
            offset += sizeof(int32_t);
          }
        }
      }
    }

    return buffer;
  }

  std::vector<Value> Schema::deserialize_record(const char *data, size_t size) const
  {
    std::vector<Value> values;
    values.reserve(m_columns_.size());

    size_t offset = 0;

    // Read null bitmap
    size_t null_bitmap_bytes = (get_null_bitmap_size() + 7) / 8;
    const uint8_t *null_bitmap = reinterpret_cast<const uint8_t *>(data + offset);
    offset += null_bitmap_bytes;

    // Check which columns are null
    std::vector<bool> is_null(m_columns_.size());
    for (size_t i = 0; i < m_columns_.size(); ++i)
    {
      size_t byte_index = i / 8;
      size_t bit_index = i % 8;
      is_null[i] = (null_bitmap[byte_index] & (1 << bit_index)) != 0;
    }

    // Count variable-length columns
    size_t var_col_count = 0;
    for (const auto &col : m_columns_)
    {
      if (col.is_variable_length())
      {
        var_col_count++;
      }
    }

    // Read variable column offsets if any
    std::vector<uint32_t> var_offsets;
    if (var_col_count > 0)
    {
      var_offsets.resize(var_col_count);
      std::memcpy(var_offsets.data(), data + offset, var_col_count * sizeof(uint32_t));
      offset += var_col_count * sizeof(uint32_t);
    }

    // Read fixed-length columns
    size_t var_index = 0;
    for (size_t i = 0; i < m_columns_.size(); ++i)
    {
      if (is_null[i])
      {
        values.emplace_back(); // NULL value
      }
      else if (!m_columns_[i].is_variable_length())
      {
        // Fixed-length column
        if (m_columns_[i].get_type() == ColumnType::INTEGER)
        {
          int32_t int_val;
          std::memcpy(&int_val, data + offset, sizeof(int32_t));
          values.emplace_back(int_val);
          offset += sizeof(int32_t);
        }
      }
      else
      {
        // Variable-length column
        if (m_columns_[i].get_type() == ColumnType::VARCHAR)
        {
          size_t var_offset = var_offsets[var_index];
          uint32_t str_len;
          std::memcpy(&str_len, data + var_offset, sizeof(uint32_t));
          std::string str_val(data + var_offset + sizeof(uint32_t), str_len);
          values.emplace_back(std::move(str_val));
          var_index++;
        }
      }
    }

    return values;
  }

  size_t Schema::get_max_record_size() const
  {
    size_t max_size = calculate_header_size();

    for (const auto &col : m_columns_)
    {
      if (col.is_variable_length())
      {
        max_size += sizeof(uint32_t) + col.get_max_length(); // length + max data
      }
      else
      {
        max_size += col.get_fixed_size();
      }
    }

    return max_size;
  }

  size_t Schema::calculate_header_size() const
  {
    size_t header_size = get_null_bitmap_size();

    // Add space for variable column offsets
    size_t var_col_count = 0;
    for (const auto &col : m_columns_)
    {
      if (col.is_variable_length())
      {
        var_col_count++;
      }
    }

    header_size += var_col_count * sizeof(uint32_t);
    return header_size;
  }

  size_t Schema::get_null_bitmap_size() const
  {
    return (m_columns_.size() + 7) / 8; // Round up to nearest byte
  }

} // namespace tinydb
