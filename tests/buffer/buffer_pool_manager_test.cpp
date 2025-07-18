#include <catch2/catch_all.hpp>
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "storage/free_space_manager.h"
#include <filesystem>
#include <string>
#include <vector>
using namespace tinydb;

// Helper function to create a unique temporary file path
std::string temp_db_path()
{
  return (std::filesystem::temp_directory_path() / "temp_db.db").string();
}

TEST_CASE("BufferPoolManagerTests", "[BufferPoolManager]")
{
  const size_t buffer_pool_size = 10;
  std::string db_file = temp_db_path();

  // Setup: Ensure the database file does not exist before a test
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }

  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto buffer_pool_manager = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());
  auto free_space_manager = std::make_unique<FreeSpaceManager>(buffer_pool_manager.get());

  // Initialize free space manager to set up header page
  REQUIRE(free_space_manager->initialize());

  SECTION("NewPage: Basic Creation and Pinning")
  {
    // Allocate a page ID through FreeSpaceManager
    page_id_t page_id = free_space_manager->allocate_page();
    REQUIRE(page_id != INVALID_PAGE_ID);

    // Create the page in the buffer pool
    Page *page = buffer_pool_manager->new_page(page_id);
    REQUIRE(page != nullptr);
    REQUIRE(page->get_page_id() == page_id);
    REQUIRE(page->get_pin_count() == 1); // Should be pinned upon creation

    // Unpin the page to allow for cleanup
    buffer_pool_manager->unpin_page(page_id, false);
  }

  SECTION("NewPage: Buffer Pool Full with Pinned Pages")
  {
    std::vector<page_id_t> page_ids;

    // Fill the buffer pool
    for (size_t i = 0; i < buffer_pool_size; ++i)
    {
      page_id_t page_id = free_space_manager->allocate_page();
      REQUIRE(page_id != INVALID_PAGE_ID);

      Page *page = buffer_pool_manager->new_page(page_id);
      REQUIRE(page != nullptr); // Should be able to create up to buffer_pool_size
      page_ids.push_back(page_id);
    }

    // All pages are pinned, so the next allocate_page call should fail
    // because there's no room in the buffer pool to access metadata pages
    page_id_t new_page_id = free_space_manager->allocate_page();
    REQUIRE(new_page_id == INVALID_PAGE_ID); // Page ID allocation should fail when buffer pool is full

    // Unpin one page to make room
    buffer_pool_manager->unpin_page(page_ids[0], false);

    // Now allocation should work
    new_page_id = free_space_manager->allocate_page();
    REQUIRE(new_page_id != INVALID_PAGE_ID);

    Page *new_page = buffer_pool_manager->new_page(new_page_id);
    REQUIRE(new_page != nullptr); // Should succeed after unpinning a page

    // Cleanup: Unpin all remaining pages
    for (size_t i = 1; i < page_ids.size(); ++i)
    {
      buffer_pool_manager->unpin_page(page_ids[i], false);
    }
    buffer_pool_manager->unpin_page(new_page_id, false);
  }

  SECTION("FetchPage: Fetching from Disk and Cache")
  {
    // Create a page
    page_id_t page_id = free_space_manager->allocate_page();
    Page *page = buffer_pool_manager->new_page(page_id);
    REQUIRE(page != nullptr);
    strcpy(page->get_data(), "test data");

    // Unpin and mark as dirty to write to disk
    buffer_pool_manager->unpin_page(page_id, true);

    // Fetch again, should be from cache, pin count becomes 1
    Page *cached_page = buffer_pool_manager->fetch_page(page_id);
    REQUIRE(cached_page != nullptr);
    REQUIRE(cached_page->get_page_id() == page_id);
    REQUIRE(cached_page->get_pin_count() == 1);

    // Unpin again
    buffer_pool_manager->unpin_page(page_id, false);

    // Force eviction by filling the buffer pool with other pages
    std::vector<page_id_t> temp_pages;
    for (size_t i = 0; i < buffer_pool_size; ++i)
    {
      page_id_t temp_id = free_space_manager->allocate_page();
      Page *temp_page = buffer_pool_manager->new_page(temp_id);
      REQUIRE(temp_page != nullptr);
      buffer_pool_manager->unpin_page(temp_id, false);
      temp_pages.push_back(temp_id);
    }

    // The original page should now be on disk. Fetch it.
    Page *fetched_page = buffer_pool_manager->fetch_page(page_id);
    REQUIRE(fetched_page != nullptr);
    REQUIRE(strcmp(fetched_page->get_data(), "test data") == 0);
    REQUIRE(fetched_page->get_pin_count() == 1);

    buffer_pool_manager->unpin_page(page_id, false);
  }

  SECTION("UnpinPage: Dirty Flag Behavior")
  {
    page_id_t page_id = free_space_manager->allocate_page();
    Page *page = buffer_pool_manager->new_page(page_id);
    REQUIRE(page != nullptr);
    strcpy(page->get_data(), "dirty data");

    // Unpin and mark as dirty
    REQUIRE(buffer_pool_manager->unpin_page(page_id, true));
    REQUIRE(page->is_dirty());

    // Pin it again
    buffer_pool_manager->fetch_page(page_id);

    // Unpin without marking dirty. The dirty flag should remain true.
    REQUIRE(buffer_pool_manager->unpin_page(page_id, false));
    REQUIRE(page->is_dirty());

    // Flush to disk
    buffer_pool_manager->flush_page(page_id);
    REQUIRE_FALSE(page->is_dirty());
  }

  SECTION("DeletePage: Basic Deletion and Edge Cases")
  {
    page_id_t page_id = free_space_manager->allocate_page();
    Page *page = buffer_pool_manager->new_page(page_id);
    REQUIRE(page != nullptr);

    // Cannot delete a pinned page
    REQUIRE_FALSE(buffer_pool_manager->delete_page(page_id));

    // Unpin and then delete
    buffer_pool_manager->unpin_page(page_id, false);
    REQUIRE(buffer_pool_manager->delete_page(page_id));

    // The page should no longer be in the page table or accessible
    Page *fetched_page = buffer_pool_manager->fetch_page(page_id);
    REQUIRE(fetched_page == nullptr);
  }

  SECTION("EvictionPolicy: LRU Behavior")
  {
    std::vector<page_id_t> page_ids;

    // Fill the buffer pool
    for (size_t i = 0; i < buffer_pool_size; ++i)
    {
      page_id_t page_id = free_space_manager->allocate_page();
      Page *page = buffer_pool_manager->new_page(page_id);
      REQUIRE(page != nullptr);
      page_ids.push_back(page_id);
    }

    // Unpin all pages, making them candidates for eviction
    for (const auto &pid : page_ids)
    {
      buffer_pool_manager->unpin_page(pid, false);
    }

    // Create a new page, which should force the eviction of the first page (LRU)
    page_id_t new_page_id = free_space_manager->allocate_page();
    Page *new_page = buffer_pool_manager->new_page(new_page_id);
    REQUIRE(new_page != nullptr);

    // Try to fetch one of the potentially evicted pages. It should be read from disk.
    // This also implicitly tests that eviction occurred.
    Page *fetched_page = buffer_pool_manager->fetch_page(page_ids[0]);
    REQUIRE(fetched_page != nullptr);

    // Cleanup
    buffer_pool_manager->unpin_page(page_ids[0], false);
    buffer_pool_manager->unpin_page(new_page_id, false);
  }

  SECTION("PageAllocation: FreeSpaceManager Integration")
  {
    // Test that page allocation through FreeSpaceManager works correctly
    page_id_t p1 = free_space_manager->allocate_page();
    page_id_t p2 = free_space_manager->allocate_page();
    page_id_t p3 = free_space_manager->allocate_page();

    REQUIRE(p1 != INVALID_PAGE_ID);
    REQUIRE(p2 != INVALID_PAGE_ID);
    REQUIRE(p3 != INVALID_PAGE_ID);
    REQUIRE(p1 != p2);
    REQUIRE(p2 != p3);
    REQUIRE(p1 != p3);

    // Create pages in buffer pool
    Page *page1 = buffer_pool_manager->new_page(p1);
    Page *page2 = buffer_pool_manager->new_page(p2);
    Page *page3 = buffer_pool_manager->new_page(p3);

    REQUIRE(page1 != nullptr);
    REQUIRE(page2 != nullptr);
    REQUIRE(page3 != nullptr);

    // Test deallocation and reallocation
    buffer_pool_manager->unpin_page(p2, false);
    buffer_pool_manager->delete_page(p2);
    free_space_manager->deallocate_page(p2);

    // Next allocation should reuse p2
    page_id_t p4 = free_space_manager->allocate_page();
    REQUIRE(p4 == p2); // Should reuse the deallocated page

    // Cleanup
    buffer_pool_manager->unpin_page(p1, false);
    buffer_pool_manager->unpin_page(p3, false);
  }

  SECTION("FlushPage: Basic Functionality")
  {
    page_id_t page_id = free_space_manager->allocate_page();
    Page *page = buffer_pool_manager->new_page(page_id);
    REQUIRE(page != nullptr);

    // Write some data and mark as dirty
    strcpy(page->get_data(), "flush test data");
    buffer_pool_manager->unpin_page(page_id, true);
    REQUIRE(page->is_dirty());

    // Flush the page
    REQUIRE(buffer_pool_manager->flush_page(page_id));
    REQUIRE_FALSE(page->is_dirty()); // Should no longer be dirty

    // Verify data persists by fetching again
    Page *fetched_page = buffer_pool_manager->fetch_page(page_id);
    REQUIRE(fetched_page != nullptr);
    REQUIRE(strcmp(fetched_page->get_data(), "flush test data") == 0);

    buffer_pool_manager->unpin_page(page_id, false);
  }

  // Teardown: remove the database file after the test case
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
