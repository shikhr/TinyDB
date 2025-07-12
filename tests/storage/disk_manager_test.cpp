#include <catch2/catch_all.hpp>
#include "storage/disk_manager.h"
#include "common/config.h"
#include <filesystem>
#include <string>
using namespace tinydb;

TEST_CASE("DiskManagerTest - New Architecture I/O Operations", "[storage]")
{
  std::string db_file = std::filesystem::temp_directory_path() / "test_new.db";

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

    // Write to page 0 (header page)
    page_id_t page_id = 0;
    disk_manager.write_page(page_id, write_data);

    // Read back the page and verify
    bool success = disk_manager.read_page(page_id, read_data);
    REQUIRE(success);
    REQUIRE(std::string(read_data).substr(0, message.size()) == message);
  }

  SECTION("Write/Read Multiple Pages")
  {
    DiskManager disk_manager(db_file);

    for (page_id_t page_id = 0; page_id < 10; ++page_id)
    {
      char write_data[kPageSize];
      char read_data[kPageSize];

      std::string message = "Page " + std::to_string(page_id);
      strncpy(write_data, message.c_str(), kPageSize);

      disk_manager.write_page(page_id, write_data);

      bool success = disk_manager.read_page(page_id, read_data);
      REQUIRE(success);
      REQUIRE(std::string(read_data).substr(0, message.size()) == message);
    }
  }

  SECTION("Read Non-existent Page")
  {
    DiskManager disk_manager(db_file);
    char read_data[kPageSize];

    // Try to read a page that doesn't exist
    bool success = disk_manager.read_page(1000, read_data);
    REQUIRE(!success); // Should fail
  }

  SECTION("File Size in Pages")
  {
    DiskManager disk_manager(db_file);

    // Initially should be 0 pages
    REQUIRE(disk_manager.get_file_size_in_pages() == 0);

    // Write a page
    char write_data[kPageSize];
    std::memset(write_data, 0, kPageSize);
    disk_manager.write_page(0, write_data);

    // Now should be 1 page
    REQUIRE(disk_manager.get_file_size_in_pages() == 1);

    // Write another page
    disk_manager.write_page(1, write_data);

    // Now should be 2 pages
    REQUIRE(disk_manager.get_file_size_in_pages() == 2);
  }

  // Clean up
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
