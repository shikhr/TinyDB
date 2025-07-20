#pragma once

#include <memory>
#include <optional>
#include <vector>
#include <string>

#include "parser/parser.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "storage/record.h"

namespace tinydb
{

  /**
   * Result of executing a query
   */
  struct ExecutionResult
  {
    bool success{false};
    std::string error_message;
    size_t rows_affected{0};

    // For SELECT queries, store the result set
    std::vector<std::string> column_names;
    std::vector<std::vector<Value>> result_rows;
  };

  /**
   * Abstract interface for the execution engine.
   * This allows for different execution strategies and testing.
   */
  class IExecutionEngine
  {
  public:
    virtual ~IExecutionEngine() = default;

    /**
     * Execute a parsed SQL statement
     */
    virtual ExecutionResult execute(const Statement &statement) = 0;
  };

  /**
   * The ExecutionEngine is responsible for taking an AST and executing it
   * against the storage layer. It orchestrates the entire operation using
   * the Catalog and TableHeap components.
   *
   * For the initial implementation, it will use sequential scans for all operations.
   */
  class ExecutionEngine : public IExecutionEngine
  {
  public:
    /**
     * Construct an execution engine with a catalog
     */
    explicit ExecutionEngine(Catalog *catalog);

    /**
     * Execute a parsed SQL statement
     */
    ExecutionResult execute(const Statement &statement) override;

  private:
    Catalog *m_catalog_;

    // Statement execution methods
    ExecutionResult execute_create_table(const CreateTableStatement &stmt);
    ExecutionResult execute_insert(const InsertStatement &stmt);
    ExecutionResult execute_select(const SelectStatement &stmt);
    ExecutionResult execute_delete(const DeleteStatement &stmt);

    // Expression evaluation methods
    std::optional<Value> evaluate_expression(const Expression &expr,
                                             const Schema &schema,
                                             const std::vector<Value> &record_values);

    // Helper methods for expression evaluation
    std::optional<Value> evaluate_literal(const struct LiteralExpression &expr);
    std::optional<Value> evaluate_identifier(const struct IdentifierExpression &expr,
                                             const Schema &schema,
                                             const std::vector<Value> &record_values);
    std::optional<Value> evaluate_binary_op(const struct BinaryOpExpression &expr,
                                            const Schema &schema,
                                            const std::vector<Value> &record_values);
    std::optional<Value> evaluate_unary_op(const struct UnaryOpExpression &expr,
                                           const Schema &schema,
                                           const std::vector<Value> &record_values);

    // Comparison operations for WHERE clause evaluation
    bool compare_values(const Value &left, const Value &right,
                        enum BinaryOpExpression::Operator op);

    // Type conversion and validation
    ColumnType parse_type(const std::string &type_name);
    std::optional<Value> convert_literal_to_type(const std::string &literal_value,
                                                 ColumnType target_type);

    // Utility methods
    ExecutionResult make_error_result(const std::string &error_message);
    ExecutionResult make_success_result(size_t rows_affected = 0);
    std::vector<Value> deserialize_record_to_values(const Record &record, const Schema &schema);
  };

} // namespace tinydb
