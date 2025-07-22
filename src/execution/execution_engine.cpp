#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/table_heap.h"
#include <sstream>
#include <algorithm>

namespace tinydb
{

  ExecutionEngine::ExecutionEngine(Catalog *catalog)
      : m_catalog_(catalog)
  {
    if (!m_catalog_)
    {
      throw std::invalid_argument("Catalog cannot be null");
    }
  }

  ExecutionResult ExecutionEngine::execute(const Statement &statement)
  {
    try
    {
      switch (statement.get_type())
      {
      case Statement::Type::CREATE_TABLE:
        return execute_create_table(static_cast<const CreateTableStatement &>(statement));
      case Statement::Type::INSERT:
        return execute_insert(static_cast<const InsertStatement &>(statement));
      case Statement::Type::SELECT:
        return execute_select(static_cast<const SelectStatement &>(statement));
      case Statement::Type::DELETE:
        return execute_delete(static_cast<const DeleteStatement &>(statement));
      default:
        return make_error_result("Unsupported statement type");
      }
    }
    catch (const std::exception &e)
    {
      return make_error_result(std::string("Execution error: ") + e.what());
    }
  }

  ExecutionResult ExecutionEngine::execute_create_table(const CreateTableStatement &stmt)
  {
    // Convert parser column definitions to schema columns
    std::vector<Column> columns;

    for (const auto &col_def : stmt.columns)
    {
      ColumnType type = parse_type(col_def.type);
      if (type == ColumnType::INVALID)
      {
        return make_error_result("Invalid column type: " + col_def.type);
      }

      // For VARCHAR, we'll use a default max length if not specified
      size_t max_length = (type == ColumnType::VARCHAR) ? 255 : 0;

      columns.emplace_back(col_def.name, type, max_length, col_def.is_nullable);
    }

    Schema schema(std::move(columns));

    // Create the table in the catalog
    TableHeap *table = m_catalog_->create_table(stmt.table_name, schema);
    if (!table)
    {
      return make_error_result("Failed to create table: " + stmt.table_name);
    }

    return make_success_result();
  }

  ExecutionResult ExecutionEngine::execute_insert(const InsertStatement &stmt)
  {
    // Get the table and schema
    TableHeap *table = m_catalog_->get_table(stmt.table_name);
    if (!table)
    {
      return make_error_result("Table does not exist: " + stmt.table_name);
    }

    const Schema *schema = m_catalog_->get_schema(stmt.table_name);
    if (!schema)
    {
      return make_error_result("Schema not found for table: " + stmt.table_name);
    }

    size_t rows_inserted = 0;

    // Process each row of values
    for (const auto &value_row : stmt.values)
    {
      // If columns are specified, we need to map them to schema columns
      std::vector<Value> record_values(schema->get_column_count());

      if (!stmt.columns.empty())
      {
        // Columns are specified, map them
        if (stmt.columns.size() != value_row.size())
        {
          return make_error_result("Column count doesn't match value count");
        }

        for (size_t i = 0; i < stmt.columns.size(); ++i)
        {
          auto col_index = schema->get_column_index(stmt.columns[i]);
          if (!col_index)
          {
            return make_error_result("Column not found: " + stmt.columns[i]);
          }

          // Evaluate the expression and convert to appropriate type
          auto literal_expr = dynamic_cast<const LiteralExpression *>(value_row[i].get());
          if (!literal_expr)
          {
            return make_error_result("Only literal values are supported in INSERT");
          }

          const Column &col = schema->get_column(*col_index);
          auto value = convert_literal_to_type(literal_expr->value, col.get_type());
          if (!value)
          {
            return make_error_result("Cannot convert value to column type");
          }

          record_values[*col_index] = *value;
        }
      }
      else
      {
        // No columns specified, use schema order
        if (value_row.size() != schema->get_column_count())
        {
          return make_error_result("Value count doesn't match column count");
        }

        for (size_t i = 0; i < value_row.size(); ++i)
        {
          auto literal_expr = dynamic_cast<const LiteralExpression *>(value_row[i].get());
          if (!literal_expr)
          {
            return make_error_result("Only literal values are supported in INSERT");
          }

          const Column &col = schema->get_column(i);
          auto value = convert_literal_to_type(literal_expr->value, col.get_type());
          if (!value)
          {
            return make_error_result("Cannot convert value to column type");
          }

          record_values[i] = *value;
        }
      }

      // Serialize the record and insert
      auto record_data = schema->serialize_record(record_values);
      Record record(RecordID(), record_data.size(), record_data.data());

      RecordID rid;
      if (!table->insert_record(record, &rid))
      {
        return make_error_result("Failed to insert record");
      }

      rows_inserted++;
    }

    return make_success_result(rows_inserted);
  }

  ExecutionResult ExecutionEngine::execute_select(const SelectStatement &stmt)
  {
    // Get the table and schema
    TableHeap *table = m_catalog_->get_table(stmt.from_table);
    if (!table)
    {
      return make_error_result("Table does not exist: " + stmt.from_table);
    }

    const Schema *schema = m_catalog_->get_schema(stmt.from_table);
    if (!schema)
    {
      return make_error_result("Schema not found for table: " + stmt.from_table);
    }

    ExecutionResult result = make_success_result();

    // Determine which columns to select
    std::vector<size_t> selected_columns;

    for (const auto &col_name : stmt.select_list)
    {
      const IdentifierExpression *id_expr = dynamic_cast<const IdentifierExpression *>(col_name.get());
      if (id_expr->name == "*")
      {
        // If SELECT *, we already added all columns
        for (size_t i = 0; i < schema->get_column_count(); ++i)
        {
          selected_columns.push_back(i);
          result.column_names.push_back(schema->get_column(i).get_name());
        }
        continue;
      }
      if (id_expr)
      {
        auto col_index = schema->get_column_index(id_expr->name);
        if (col_index)
        {
          selected_columns.push_back(*col_index);
          result.column_names.push_back(schema->get_column(*col_index).get_name());
        }
      }
    }

    // Sequential scan through all records
    for (auto it = table->begin(); it != table->end(); ++it)
    {
      const Record &record = *it;

      // Deserialize record to values
      std::vector<Value> record_values = deserialize_record_to_values(record, *schema);

      // Evaluate WHERE clause if present
      bool include_record = true;
      if (stmt.where_clause)
      {
        auto where_result = evaluate_expression(*stmt.where_clause, *schema, record_values);
        if (!where_result || where_result->is_null())
        {
          include_record = false;
        }
        else if (where_result->get_type() != ColumnType::INTEGER || where_result->get_integer() == 0)
        {
          include_record = false;
        }
      }

      if (include_record)
      {
        // Add the selected columns to result
        std::vector<Value> result_row;
        for (size_t col_idx : selected_columns)
        {
          result_row.push_back(record_values[col_idx]);
        }
        result.result_rows.push_back(std::move(result_row));
        result.rows_affected++;
      }
    }

    return result;
  }

  ExecutionResult ExecutionEngine::execute_delete(const DeleteStatement &stmt)
  {
    // Get the table and schema
    TableHeap *table = m_catalog_->get_table(stmt.table_name);
    if (!table)
    {
      return make_error_result("Table does not exist: " + stmt.table_name);
    }

    const Schema *schema = m_catalog_->get_schema(stmt.table_name);
    if (!schema)
    {
      return make_error_result("Schema not found for table: " + stmt.table_name);
    }

    size_t rows_deleted = 0;
    std::vector<RecordID> records_to_delete;

    // Sequential scan to find records to delete
    for (auto it = table->begin(); it != table->end(); ++it)
    {
      const Record &record = *it;

      // Deserialize record to values
      std::vector<Value> record_values = deserialize_record_to_values(record, *schema);

      // Evaluate WHERE clause if present
      bool delete_record = true;
      if (stmt.where_clause)
      {
        auto where_result = evaluate_expression(*stmt.where_clause, *schema, record_values);
        if (!where_result || where_result->is_null())
        {
          delete_record = false;
        }
        else if (where_result->get_type() != ColumnType::INTEGER || where_result->get_integer() == 0)
        {
          delete_record = false;
        }
      }

      if (delete_record)
      {
        records_to_delete.push_back(record.get_rid());
      }
    }

    // Delete the records
    for (const auto &rid : records_to_delete)
    {
      if (table->delete_record(rid))
      {
        rows_deleted++;
      }
    }

    return make_success_result(rows_deleted);
  }

  std::optional<Value> ExecutionEngine::evaluate_expression(const Expression &expr,
                                                            const Schema &schema,
                                                            const std::vector<Value> &record_values)
  {
    switch (expr.get_type())
    {
    case Expression::Type::LITERAL:
      return evaluate_literal(static_cast<const LiteralExpression &>(expr));
    case Expression::Type::IDENTIFIER:
      return evaluate_identifier(static_cast<const IdentifierExpression &>(expr), schema, record_values);
    case Expression::Type::BINARY_OP:
      return evaluate_binary_op(static_cast<const BinaryOpExpression &>(expr), schema, record_values);
    case Expression::Type::UNARY_OP:
      return evaluate_unary_op(static_cast<const UnaryOpExpression &>(expr), schema, record_values);
    default:
      return std::nullopt;
    }
  }

  std::optional<Value> ExecutionEngine::evaluate_literal(const LiteralExpression &expr)
  {
    switch (expr.literal_type)
    {
    case LiteralExpression::LiteralType::NULL_VALUE:
      return Value(); // NULL value
    case LiteralExpression::LiteralType::STRING:
      return Value(expr.value);
    case LiteralExpression::LiteralType::NUMBER:
      try
      {
        int32_t int_val = std::stoi(expr.value);
        return Value(int_val);
      }
      catch (const std::exception &)
      {
        return std::nullopt;
      }
    case LiteralExpression::LiteralType::BOOLEAN:
      return Value(expr.value == "true" || expr.value == "TRUE" ? 1 : 0);
    default:
      return std::nullopt;
    }
  }

  std::optional<Value> ExecutionEngine::evaluate_identifier(const IdentifierExpression &expr,
                                                            const Schema &schema,
                                                            const std::vector<Value> &record_values)
  {
    auto col_index = schema.get_column_index(expr.name);
    if (!col_index || *col_index >= record_values.size())
    {
      return std::nullopt;
    }

    return record_values[*col_index];
  }

  std::optional<Value> ExecutionEngine::evaluate_binary_op(const BinaryOpExpression &expr,
                                                           const Schema &schema,
                                                           const std::vector<Value> &record_values)
  {
    auto left_val = evaluate_expression(*expr.left, schema, record_values);
    auto right_val = evaluate_expression(*expr.right, schema, record_values);

    if (!left_val || !right_val)
    {
      return std::nullopt;
    }

    // Handle comparison operations
    if (expr.op >= BinaryOpExpression::Operator::EQUAL &&
        expr.op <= BinaryOpExpression::Operator::GREATER_EQUAL)
    {
      bool result = compare_values(*left_val, *right_val, expr.op);
      return Value(result ? 1 : 0);
    }

    // Handle logical operations
    if (expr.op == BinaryOpExpression::Operator::AND)
    {
      bool left_bool = (!left_val->is_null() && left_val->get_type() == ColumnType::INTEGER && left_val->get_integer() != 0);
      bool right_bool = (!right_val->is_null() && right_val->get_type() == ColumnType::INTEGER && right_val->get_integer() != 0);
      return Value((left_bool && right_bool) ? 1 : 0);
    }

    if (expr.op == BinaryOpExpression::Operator::OR)
    {
      bool left_bool = (!left_val->is_null() && left_val->get_type() == ColumnType::INTEGER && left_val->get_integer() != 0);
      bool right_bool = (!right_val->is_null() && right_val->get_type() == ColumnType::INTEGER && right_val->get_integer() != 0);
      return Value((left_bool || right_bool) ? 1 : 0);
    }

    // TODO: Handle arithmetic operations (PLUS, MINUS, MULTIPLY, DIVIDE)

    return std::nullopt;
  }

  std::optional<Value> ExecutionEngine::evaluate_unary_op(const UnaryOpExpression &expr,
                                                          const Schema &schema,
                                                          const std::vector<Value> &record_values)
  {
    auto operand_val = evaluate_expression(*expr.operand, schema, record_values);
    if (!operand_val)
    {
      return std::nullopt;
    }

    if (expr.op == UnaryOpExpression::Operator::NOT)
    {
      bool operand_bool = (!operand_val->is_null() && operand_val->get_type() == ColumnType::INTEGER && operand_val->get_integer() != 0);
      return Value(operand_bool ? 0 : 1);
    }

    // TODO: Handle MINUS operator

    return std::nullopt;
  }

  bool ExecutionEngine::compare_values(const Value &left, const Value &right, BinaryOpExpression::Operator op)
  {
    // Handle NULL values
    if (left.is_null() || right.is_null())
    {
      return false; // SQL NULL comparison semantics
    }

    // Both values must have the same type for comparison
    if (left.get_type() != right.get_type())
    {
      return false;
    }

    bool result = false;

    switch (left.get_type())
    {
    case ColumnType::INTEGER:
    {
      int32_t left_int = left.get_integer();
      int32_t right_int = right.get_integer();

      switch (op)
      {
      case BinaryOpExpression::Operator::EQUAL:
        result = (left_int == right_int);
        break;
      case BinaryOpExpression::Operator::NOT_EQUAL:
        result = (left_int != right_int);
        break;
      case BinaryOpExpression::Operator::LESS_THAN:
        result = (left_int < right_int);
        break;
      case BinaryOpExpression::Operator::LESS_EQUAL:
        result = (left_int <= right_int);
        break;
      case BinaryOpExpression::Operator::GREATER_THAN:
        result = (left_int > right_int);
        break;
      case BinaryOpExpression::Operator::GREATER_EQUAL:
        result = (left_int >= right_int);
        break;
      default:
        break;
      }
    }
    break;

    case ColumnType::VARCHAR:
    {
      const std::string &left_str = left.get_string();
      const std::string &right_str = right.get_string();

      switch (op)
      {
      case BinaryOpExpression::Operator::EQUAL:
        result = (left_str == right_str);
        break;
      case BinaryOpExpression::Operator::NOT_EQUAL:
        result = (left_str != right_str);
        break;
      case BinaryOpExpression::Operator::LESS_THAN:
        result = (left_str < right_str);
        break;
      case BinaryOpExpression::Operator::LESS_EQUAL:
        result = (left_str <= right_str);
        break;
      case BinaryOpExpression::Operator::GREATER_THAN:
        result = (left_str > right_str);
        break;
      case BinaryOpExpression::Operator::GREATER_EQUAL:
        result = (left_str >= right_str);
        break;
      default:
        break;
      }
    }
    break;

    default:
      break;
    }

    return result;
  }

  ColumnType ExecutionEngine::parse_type(const std::string &type_name)
  {
    std::string lower_type = type_name;
    std::transform(lower_type.begin(), lower_type.end(), lower_type.begin(), ::tolower);

    if (lower_type == "integer" || lower_type == "int")
    {
      return ColumnType::INTEGER;
    }
    else if (lower_type == "varchar" || lower_type == "string" || lower_type == "text")
    {
      return ColumnType::VARCHAR;
    }

    return ColumnType::INVALID;
  }

  std::optional<Value> ExecutionEngine::convert_literal_to_type(const std::string &literal_value,
                                                                ColumnType target_type)
  {
    switch (target_type)
    {
    case ColumnType::INTEGER:
      try
      {
        int32_t int_val = std::stoi(literal_value);
        return Value(int_val);
      }
      catch (const std::exception &)
      {
        return std::nullopt;
      }

    case ColumnType::VARCHAR:
      return Value(literal_value);

    default:
      return std::nullopt;
    }
  }

  ExecutionResult ExecutionEngine::make_error_result(const std::string &error_message)
  {
    ExecutionResult result;
    result.success = false;
    result.error_message = error_message;
    return result;
  }

  ExecutionResult ExecutionEngine::make_success_result(size_t rows_affected)
  {
    ExecutionResult result;
    result.success = true;
    result.rows_affected = rows_affected;
    return result;
  }

  std::vector<Value> ExecutionEngine::deserialize_record_to_values(const Record &record, const Schema &schema)
  {
    return schema.deserialize_record(record.get_data(), record.get_size());
  }

} // namespace tinydb
