#include <catch2/catch_all.hpp>
#include "storage/disk_manager.h"
#include "common/config.h"
#include <filesystem>
#include <string>
using namespace tinydb;

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
    REQUIRE(page_id == 1); // First allocated page should be 1, since 0 is header

    disk_manager.write_page(page_id, write_data);
    REQUIRE(disk_manager.read_page(page_id, read_data));

    REQUIRE(std::string(read_data) == message);

    // Overwrite the page
    std::string new_message = "New data for the page.";
    strncpy(write_data, new_message.c_str(), kPageSize);
    disk_manager.write_page(page_id, write_data);
    REQUIRE(disk_manager.read_page(page_id, read_data));

    REQUIRE(std::string(read_data) == new_message);
  }

  SECTION("Allocate Multiple Pages")
  {
    DiskManager disk_manager(db_file);
    REQUIRE(disk_manager.allocate_page() == 1);
    REQUIRE(disk_manager.allocate_page() == 2);
    REQUIRE(disk_manager.allocate_page() == 3);
  }

  SECTION("Deallocate and Reuse Pages")
  {
    DiskManager disk_manager(db_file);
    page_id_t p1 = disk_manager.allocate_page(); // 1
    page_id_t p2 = disk_manager.allocate_page(); // 2
    REQUIRE(p1 == 1);
    REQUIRE(p2 == 2);

    disk_manager.deallocate_page(p1);
    page_id_t p3 = disk_manager.allocate_page(); // Should reuse 1
    REQUIRE(p3 == p1);

    page_id_t p4 = disk_manager.allocate_page(); // Should be 3
    REQUIRE(p4 == 3);
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
      REQUIRE(disk_manager.read_page(page_id, read_data));
      REQUIRE(std::string(read_data) == "Persistent data");
      // Check that new pages are allocated correctly
      REQUIRE(disk_manager.allocate_page() == 2);
    }
  }

  SECTION("Free List Persistence")
  {
    std::vector<page_id_t> free_list_before;
    {
      DiskManager disk_manager(db_file);

      // Allocate 1000 pages
      for (int i = 1; i <= 1000; ++i)
      {
        REQUIRE(disk_manager.allocate_page() == i);
      }

      // Deallocate pages with IDs that are multiples of 3
      for (page_id_t i = 1; i <= 1000; ++i)
      {
        if (i % 3 == 0)
        {
          disk_manager.deallocate_page(i);
        }
      }

      // Save the free list for later comparison
      // We expect the free pages to be in reverse order of deallocation
      for (int i = 1000; i >= 1; --i)
      {
        if (i % 3 == 0)
        {
          free_list_before.push_back(i);
        }
      }
    } // DiskManager is destroyed, data is persisted.

    {
      DiskManager disk_manager(db_file); // Re-open the database file.

      // Sort the original free list for comparison
      std::sort(free_list_before.begin(), free_list_before.end());

      // Allocate from the free list and store the new page IDs
      std::vector<page_id_t> free_list_after;
      for (size_t i = 0; i < free_list_before.size(); ++i)
      {
        free_list_after.push_back(disk_manager.allocate_page());
      }

      // Sort the newly allocated page IDs
      std::sort(free_list_after.begin(), free_list_after.end());

      // The two lists should now be identical
      REQUIRE(free_list_before == free_list_after);

      // After exhausting the free list, new pages should be allocated sequentially.
      REQUIRE(disk_manager.allocate_page() == 1001);
      REQUIRE(disk_manager.allocate_page() == 1002);
    }
  }

  // Clean up the test file
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
