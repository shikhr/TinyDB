#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <stdexcept>

#include "common/config.h"
#include "storage/record.h"

namespace tinydb
{

  // Forward declaration
  class TableHeap;

  // An enum for data types
  enum class ColumnType
  {
    INVALID,
    INTEGER,
    VARCHAR
  };

  /**
   * A Value represents a single typed value that can be stored in a column.
   * It provides type safety and handles null values.
   */
  class Value
  {
  public:
    // Constructors
    Value();                             // NULL value
    explicit Value(int32_t int_val);     // INTEGER value
    explicit Value(std::string str_val); // VARCHAR value

    // Type checking
    ColumnType get_type() const { return m_type_; }
    bool is_null() const { return m_type_ == ColumnType::INVALID; }

    // Value getters (throw if wrong type or null)
    int32_t get_integer() const;
    const std::string &get_string() const;

    // Serialization size
    size_t get_serialized_size() const;

    // Comparison operators
    bool operator==(const Value &other) const;
    bool operator!=(const Value &other) const { return !(*this == other); }

  private:
    ColumnType m_type_{ColumnType::INVALID};
    int32_t m_integer_val_{0};
    std::string m_string_val_;
  };

  class Column
  {
  public:
    Column(std::string column_name, ColumnType type, size_t max_length = 0, bool nullable = true)
        : m_column_name_(std::move(column_name)), m_type_(type), m_max_length_(max_length), m_nullable_(nullable) {}

    const std::string &get_name() const { return m_column_name_; }
    ColumnType get_type() const { return m_type_; }
    size_t get_max_length() const { return m_max_length_; }
    bool is_nullable() const { return m_nullable_; }

    // Get the fixed size for this column type (0 for variable-length)
    size_t get_fixed_size() const;

    // Check if this column type is variable length
    bool is_variable_length() const;

  private:
    std::string m_column_name_;
    ColumnType m_type_;
    size_t m_max_length_{0}; // For VARCHAR, maximum length; for others, ignored
    bool m_nullable_{true};  // Whether this column can be NULL
  };

  class Schema
  {
  public:
    Schema(std::vector<Column> columns) : m_columns_(std::move(columns)) {}

    const std::vector<Column> &get_columns() const { return m_columns_; }
    size_t get_column_count() const { return m_columns_.size(); }
    const Column &get_column(size_t index) const { return m_columns_.at(index); }

    // Find column index by name
    std::optional<size_t> get_column_index(const std::string &name) const;

    // Serialization support
    size_t calculate_record_size(const std::vector<Value> &values) const;
    std::vector<char> serialize_record(const std::vector<Value> &values) const;
    std::vector<Value> deserialize_record(const char *data, size_t size) const;

    // Get the maximum possible size for a record with this schema
    size_t get_max_record_size() const;

  private:
    std::vector<Column> m_columns_;

    // Helper methods for serialization
    size_t calculate_header_size() const;
    size_t get_null_bitmap_size() const;
  };

} // namespace tinydb
