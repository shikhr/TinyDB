#include <catch2/catch_all.hpp>
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include <filesystem>
#include <string>
#include <vector>

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

  SECTION("NewPage: Basic Creation and Pinning")
  {
    page_id_t page_id;
    Page *page = buffer_pool_manager->new_page(&page_id);

    REQUIRE(page != nullptr);
    REQUIRE(page_id == 1);
    REQUIRE(page->get_pin_count() == 1); // Should be pinned upon creation

    // Unpin the page to allow for cleanup
    buffer_pool_manager->unpin_page(page_id, false);
  }

  SECTION("NewPage: Buffer Pool Full with Pinned Pages")
  {
    std::vector<page_id_t> page_ids;
    for (size_t i = 0; i < buffer_pool_size; ++i)
    {
      page_id_t page_id;
      Page *page = buffer_pool_manager->new_page(&page_id);
      REQUIRE(page != nullptr); // Should be able to create up to buffer_pool_size
      page_ids.push_back(page_id);
    }

    // All pages are pinned, so the next new_page call should fail
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager->new_page(&new_page_id);
    REQUIRE(new_page == nullptr);

    // Cleanup: Unpin all pages
    for (const auto &pid : page_ids)
    {
      buffer_pool_manager->unpin_page(pid, false);
    }
  }

  SECTION("FetchPage: Fetching from Disk and Cache")
  {
    page_id_t page_id;
    Page *page = buffer_pool_manager->new_page(&page_id);
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
    for (size_t i = 0; i < buffer_pool_size; ++i)
    {
      page_id_t temp_id;
      Page *temp_page = buffer_pool_manager->new_page(&temp_id);
      REQUIRE(temp_page != nullptr);
      buffer_pool_manager->unpin_page(temp_id, false);
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
    page_id_t page_id;
    Page *page = buffer_pool_manager->new_page(&page_id);
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

    buffer_pool_manager->unpin_page(page_id, false);
  }

  SECTION("DeletePage: Basic Deletion and Edge Cases")
  {
    page_id_t page_id;
    Page *page = buffer_pool_manager->new_page(&page_id);
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
      page_id_t page_id;
      Page *page = buffer_pool_manager->new_page(&page_id);
      REQUIRE(page != nullptr);
      page_ids.push_back(page_id);
    }

    // Unpin all pages, making them candidates for eviction
    for (const auto &pid : page_ids)
    {
      buffer_pool_manager->unpin_page(pid, false);
    }

    // Create a new page, which should force the eviction of page 1 (the first one created)
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager->new_page(&new_page_id);
    REQUIRE(new_page != nullptr);

    // Try to fetch the evicted page (page_ids[1]). It should be read from disk.
    // This also implicitly tests that it was actually evicted.
    Page *fetched_page = buffer_pool_manager->fetch_page(page_ids[1]);
    REQUIRE(fetched_page != nullptr);

    // Cleanup
    buffer_pool_manager->unpin_page(page_ids[1], false);
    buffer_pool_manager->unpin_page(new_page_id, false);
  }

  SECTION("FreeList: Persistence and Reuse")
  {
    page_id_t p1, p2, p3;

    // Scope 1: Allocate some pages and deallocate one
    {
      auto disk_manager_local = std::make_unique<DiskManager>(db_file);
      auto buffer_pool_manager_local = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager_local.get());

      Page *page1 = buffer_pool_manager_local->new_page(&p1);
      Page *page2 = buffer_pool_manager_local->new_page(&p2);
      Page *page3 = buffer_pool_manager_local->new_page(&p3);

      REQUIRE(p1 == 1);
      REQUIRE(p2 == 2);
      REQUIRE(p3 == 3);

      // Unpin and delete page 2
      buffer_pool_manager_local->unpin_page(p1, false);
      buffer_pool_manager_local->unpin_page(p2, false);
      buffer_pool_manager_local->unpin_page(p3, false);
      REQUIRE(buffer_pool_manager_local->delete_page(p2));

      // Destructors will be called, saving state
    }

    // Scope 2: Reopen the database and check if the free list was restored
    {
      auto disk_manager_reopened = std::make_unique<DiskManager>(db_file);
      auto buffer_pool_manager_reopened = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager_reopened.get());

      page_id_t p4;
      Page *page4 = buffer_pool_manager_reopened->new_page(&p4);
      REQUIRE(page4 != nullptr);

      // The new page should reuse the deallocated page ID
      REQUIRE(p4 == p2); // Should reuse page 2

      page_id_t p5;
      Page *page5 = buffer_pool_manager_reopened->new_page(&p5);
      REQUIRE(page5 != nullptr);

      // The next page should be a fresh one
      REQUIRE(p5 == 4);
    }
  }

  // Teardown: remove the database file after the test case
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
