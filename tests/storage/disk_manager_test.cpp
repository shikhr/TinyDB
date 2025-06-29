#include <catch2/catch_all.hpp>
#include "storage/disk_manager.h"
#include "common/config.h"
#include <filesystem>
#include <string>

TEST_CASE("DiskManagerTest", "[storage]")
{

  std::string db_file = std::filesystem::temp_directory_path() / "test.db";

  // Clean up any previous test files
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }

  SECTION("Create and Write/Read Page")
  {
    DiskManager disk_manager(db_file);
    char write_data[kPageSize];
    char read_data[kPageSize];

    // Write something to the page
    std::string message = "Hello, DiskManager!";
    strncpy(write_data, message.c_str(), kPageSize);

    page_id_t page_id = disk_manager.allocate_page();
    REQUIRE(page_id == 0);

    disk_manager.write_page(page_id, write_data);
    disk_manager.read_page(page_id, read_data);

    REQUIRE(std::string(read_data) == message);

    // Overwrite the page
    std::string new_message = "New data for the page.";
    strncpy(write_data, new_message.c_str(), kPageSize);
    disk_manager.write_page(page_id, write_data);
    disk_manager.read_page(page_id, read_data);

    REQUIRE(std::string(read_data) == new_message);
  }

  SECTION("Allocate Multiple Pages")
  {
    DiskManager disk_manager(db_file);
    REQUIRE(disk_manager.allocate_page() == 0);
    REQUIRE(disk_manager.allocate_page() == 1);
    REQUIRE(disk_manager.allocate_page() == 2);
  }

  SECTION("Persistence")
  {
    page_id_t page_id;
    {
      DiskManager disk_manager(db_file);
      page_id = disk_manager.allocate_page();
      char write_data[kPageSize];
      std::string message = "Persistent data";
      strncpy(write_data, message.c_str(), kPageSize);
      disk_manager.write_page(page_id, write_data);
    } // DiskManager goes out of scope, destructor is called, file is closed

    {
      DiskManager disk_manager(db_file); // Re-open the file
      char read_data[kPageSize];
      disk_manager.read_page(page_id, read_data);
      REQUIRE(std::string(read_data) == "Persistent data");
      // Check that new pages are allocated correctly
      REQUIRE(disk_manager.allocate_page() == 1);
    }
  }

  // Clean up the test file
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
