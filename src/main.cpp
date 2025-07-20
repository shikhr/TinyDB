#include <iostream>
#include <string>
#include <memory>
#include <filesystem>

#include <readline/readline.h>
#include <readline/history.h>

#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "parser/lexer.h"
#include "catalog/catalog.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"

using namespace tinydb;

class TinyDBCLI
{
public:
  TinyDBCLI(const std::string &db_file)
  {
    // Initialize the storage stack
    disk_manager_ = std::make_unique<DiskManager>(db_file);
    buffer_pool_ = std::make_unique<BufferPoolManager>(kBufferPoolSize, disk_manager_.get());
    catalog_ = std::make_unique<Catalog>(buffer_pool_.get());
    execution_engine_ = std::make_unique<ExecutionEngine>(catalog_.get());
    history_path = get_history_path();
  }

  std::string get_history_path()
  {
    const char *xdg = std::getenv("XDG_CONFIG_HOME");
    const char *home = std::getenv("HOME");

    if (xdg)
      return std::string(xdg) + "/.tinydb_history";
    else if (home)
      return std::string(home) + "/.tinydb_history";
    else
      return ".tinydb_history"; // fallback in current directory
  }

  void load_history()
  {
    read_history(history_path.c_str());
  }

  void save_history()
  {
    write_history(history_path.c_str());
  }

  void run()
  {
    load_history();

    std::cout << "TinyDB - A Simple Database Management System" << std::endl;
    std::cout << "Type 'quit' or 'exit' to leave, 'help' for help." << std::endl;
    std::cout << std::endl;

    std::string input;
    while (true)
    {
      char *command = readline("tinydb> ");

      if (command)
      {
        input = std::string(command);
        add_history(command);
        free(command);
      }

      if (input.empty())
        continue;

      // Handle special commands
      if (input == "quit" || input == "exit")
      {
        std::cout << "Goodbye!" << std::endl;
        break;
      }

      if (input == "help")
      {
        print_help();
        continue;
      }

      // Execute SQL
      execute_sql(input);
    }

    save_history();
  }

private:
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::string history_path;

  void execute_sql(const std::string &sql)
  {
    try
    {
      // Parse the SQL
      Lexer lexer(sql);
      auto tokens = lexer.tokenize();
      if (lexer.had_error())
      {
        std::cout << "Lexer error: " << lexer.error_message() << std::endl;
        return;
      }

      Parser parser(std::move(tokens));
      auto parse_result = parser.parse();
      if (!parse_result.success)
      {
        std::cout << "Parse error: " << parse_result.error_message << std::endl;
        return;
      }

      // Execute the statement
      auto result = execution_engine_->execute(*parse_result.statement);

      if (!result.success)
      {
        std::cout << "Execution error: " << result.error_message << std::endl;
        return;
      }

      // Display results
      if (result.result_rows.empty())
      {
        std::cout << std::endl;
        std::cout << "Query executed successfully. ";
        if (result.rows_affected > 0)
        {
          std::cout << result.rows_affected << " row(s) affected.";
        }
        std::cout << std::endl;
      }
      else
      {
        // Print column headers
        for (size_t i = 0; i < result.column_names.size(); ++i)
        {
          std::cout << result.column_names[i];
          if (i < result.column_names.size() - 1)
            std::cout << "\t";
        }
        std::cout << std::endl;

        // Print separator
        for (size_t i = 0; i < result.column_names.size(); ++i)
        {
          std::cout << std::string(result.column_names[i].length(), '-');
          if (i < result.column_names.size() - 1)
            std::cout << "\t";
        }
        std::cout << std::endl;

        // Print rows
        for (const auto &row : result.result_rows)
        {
          for (size_t i = 0; i < row.size(); ++i)
          {
            if (row[i].is_null())
            {
              std::cout << "NULL";
            }
            else if (row[i].get_type() == ColumnType::INTEGER)
            {
              std::cout << row[i].get_integer();
            }
            else if (row[i].get_type() == ColumnType::VARCHAR)
            {
              std::cout << row[i].get_string();
            }

            if (i < row.size() - 1)
              std::cout << "\t";
          }
          std::cout << std::endl;
        }

        std::cout << std::endl
                  << result.rows_affected << " row(s) returned." << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "Error: " << e.what() << std::endl;
    }
  }

  void print_help()
  {
    std::cout << "TinyDB supports the following SQL commands:" << std::endl;
    std::cout << "  CREATE TABLE table_name (column_name type, ...);" << std::endl;
    std::cout << "  INSERT INTO table_name (col1, col2) VALUES (value1, value2);" << std::endl;
    std::cout << "  SELECT * FROM table_name;" << std::endl;
    std::cout << "  SELECT * FROM table_name WHERE condition;" << std::endl;
    std::cout << "  DELETE FROM table_name WHERE condition;" << std::endl;
    std::cout << std::endl;
    std::cout << "Supported types: INTEGER, VARCHAR" << std::endl;
    std::cout << "Supported operators: =, !=, <, <=, >, >=, AND, OR" << std::endl;
    std::cout << std::endl;
  }
};

int main(int argc, char *argv[])
{
  std::string db_file = "tinydb.db";

  // Allow user to specify database file
  if (argc > 1)
  {
    db_file = argv[1];
  }

  try
  {
    TinyDBCLI cli(db_file);
    cli.run();
  }
  catch (const std::exception &e)
  {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
