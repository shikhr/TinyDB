#include <catch2/catch_all.hpp>
#include "storage/table_heap.h"
#include "storage/table_page.h"
#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/free_space_manager.h"
#include "catalog/schema.h"
#include <filesystem>
#include <vector>
#include <string>
#include <random>
#include <set>
#include <algorithm>
#include <numeric>

using namespace tinydb;

TEST_CASE("TableHeap Stress Tests", "[table_heap]")
{
  // Setup test database
  std::string db_file = std::filesystem::temp_directory_path() / "table_heap_stress_test.db";
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }

  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto buffer_pool_manager = std::make_unique<BufferPoolManager>(10, disk_manager.get());

  // Create FreeSpaceManager and initialize it
  auto free_space_manager = std::make_unique<FreeSpaceManager>(buffer_pool_manager.get());
  REQUIRE(free_space_manager->initialize());

  // Use two-step allocation for the first table page
  // Step 1: Get page ID from FreeSpaceManager
  page_id_t first_page_id = free_space_manager->allocate_page();
  REQUIRE(first_page_id != INVALID_PAGE_ID);

  // Step 2: Get page frame from BufferPoolManager (use new_page for newly allocated pages)
  Page *first_page = buffer_pool_manager->new_page(first_page_id);
  REQUIRE(first_page != nullptr);

  // Initialize the first page as a table page
  TablePage *table_page = reinterpret_cast<TablePage *>(first_page);
  table_page->init(first_page_id, INVALID_PAGE_ID);
  buffer_pool_manager->unpin_page(first_page_id, true);

  // Create table heap with FreeSpaceManager
  TableHeap table_heap(buffer_pool_manager.get(), first_page_id, free_space_manager.get());

  SECTION("Multi-Page Insert Stress Test")
  {
    // Create a schema for testing
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("name", ColumnType::VARCHAR, 100),
        Column("description", ColumnType::VARCHAR, 200)};
    Schema schema(std::move(columns));

    std::vector<RecordID> inserted_rids;
    const int NUM_RECORDS = 1000; // This should definitely span multiple pages

    // Insert many records to force multiple pages
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
      // Create values with varying sizes to test space management
      std::string name = "User_" + std::to_string(i);
      std::string description = "This is a longer description for user " + std::to_string(i) +
                                ". Adding more text to make records larger and force page splits sooner.";

      std::vector<Value> values = {
          Value(i),
          Value(name),
          Value(description)};

      // Serialize the record
      auto serialized_data = schema.serialize_record(values);
      Record record(RecordID(), serialized_data.size(), serialized_data.data());

      // Insert the record
      RecordID rid;
      bool inserted = table_heap.insert_record(record, &rid);
      REQUIRE(inserted);

      // Verify the RID is valid
      REQUIRE(rid.page_id_ != INVALID_PAGE_ID);
      REQUIRE(rid.slot_num_ >= 0);

      inserted_rids.push_back(rid);
    }

    REQUIRE(inserted_rids.size() == NUM_RECORDS);

    // Verify we actually used multiple pages
    std::set<page_id_t> unique_pages;
    for (const auto &rid : inserted_rids)
    {
      unique_pages.insert(rid.page_id_);
    }

    INFO("Number of pages used: " << unique_pages.size());
    REQUIRE(unique_pages.size() > 1); // Should definitely use more than 1 page

    // Retrieve and verify all records
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
      const RecordID &rid = inserted_rids[i];

      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rid, &retrieved_record);
      REQUIRE(found);

      // Deserialize and verify the data
      auto deserialized_values = schema.deserialize_record(
          retrieved_record.get_data(),
          retrieved_record.get_size());

      REQUIRE(deserialized_values.size() == 3);
      REQUIRE(deserialized_values[0].get_integer() == i);
      REQUIRE(deserialized_values[1].get_string() == "User_" + std::to_string(i));

      std::string expected_description = "This is a longer description for user " + std::to_string(i) +
                                         ". Adding more text to make records larger and force page splits sooner.";
      REQUIRE(deserialized_values[2].get_string() == expected_description);
    }
  }

  SECTION("Random Access Pattern Stress Test")
  {
    std::vector<Column> columns = {
        Column("key", ColumnType::INTEGER),
        Column("value", ColumnType::VARCHAR, 50)};
    Schema schema(std::move(columns));

    const int NUM_RECORDS = 500;
    std::vector<RecordID> rids;

    // Insert records
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
      std::vector<Value> values = {
          Value(i * 2), // Use even numbers as keys
          Value("Value_" + std::to_string(i * 2))};

      auto serialized_data = schema.serialize_record(values);
      Record record(RecordID(), serialized_data.size(), serialized_data.data());

      RecordID rid;
      bool inserted = table_heap.insert_record(record, &rid);
      REQUIRE(inserted);
      rids.push_back(rid);
    }

    // Random access pattern - shuffle the access order
    std::vector<int> access_order(NUM_RECORDS);
    std::iota(access_order.begin(), access_order.end(), 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(access_order.begin(), access_order.end(), g);

    // Access records in random order
    for (int idx : access_order)
    {
      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rids[idx], &retrieved_record);
      REQUIRE(found);

      auto deserialized_values = schema.deserialize_record(
          retrieved_record.get_data(),
          retrieved_record.get_size());

      REQUIRE(deserialized_values[0].get_integer() == idx * 2);
      REQUIRE(deserialized_values[1].get_string() == "Value_" + std::to_string(idx * 2));
    }
  }

  SECTION("Update and Delete Stress Test")
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("status", ColumnType::VARCHAR, 20)};
    Schema schema(std::move(columns));

    const int NUM_RECORDS = 300;
    std::vector<RecordID> rids;

    // Insert records
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
      std::vector<Value> values = {
          Value(i),
          Value("ACTIVE")};

      auto serialized_data = schema.serialize_record(values);
      Record record(RecordID(), serialized_data.size(), serialized_data.data());

      RecordID rid;
      bool inserted = table_heap.insert_record(record, &rid);
      REQUIRE(inserted);
      rids.push_back(rid);
    }

    // Update every other record (same size update)
    for (int i = 0; i < NUM_RECORDS; i += 2)
    {
      std::vector<Value> updated_values = {
          Value(i),
          Value("UPATED") // Same length as "ACTIVE" (6 chars)
      };

      auto updated_data = schema.serialize_record(updated_values);
      Record updated_record(rids[i], updated_data.size(), updated_data.data());

      bool updated = table_heap.update_record(updated_record, rids[i]);
      REQUIRE(updated);
    }

    // Delete every fourth record
    for (int i = 0; i < NUM_RECORDS; i += 4)
    {
      bool deleted = table_heap.delete_record(rids[i]);
      REQUIRE(deleted);
    }

    // Verify the remaining records
    for (int i = 0; i < NUM_RECORDS; ++i)
    {
      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rids[i], &retrieved_record);

      if (i % 4 == 0)
      {
        // These should be deleted
        REQUIRE_FALSE(found);
      }
      else
      {
        // These should still exist
        REQUIRE(found);

        auto deserialized_values = schema.deserialize_record(
            retrieved_record.get_data(),
            retrieved_record.get_size());

        REQUIRE(deserialized_values[0].get_integer() == i);

        if (i % 2 == 0)
        {
          REQUIRE(deserialized_values[1].get_string() == "UPATED");
        }
        else
        {
          REQUIRE(deserialized_values[1].get_string() == "ACTIVE");
        }
      }
    }
  }

  SECTION("Large Record Stress Test")
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("large_data", ColumnType::VARCHAR, 2000) // Large VARCHAR
    };
    Schema schema(std::move(columns));

    const int NUM_LARGE_RECORDS = 100;
    std::vector<RecordID> rids;

    // Insert large records that should quickly fill pages
    for (int i = 0; i < NUM_LARGE_RECORDS; ++i)
    {
      // Create a large string (1500 characters)
      std::string large_data(1500, 'A' + (i % 26));
      large_data += "_record_" + std::to_string(i);

      std::vector<Value> values = {
          Value(i),
          Value(large_data)};

      auto serialized_data = schema.serialize_record(values);
      Record record(RecordID(), serialized_data.size(), serialized_data.data());

      RecordID rid;
      bool inserted = table_heap.insert_record(record, &rid);
      REQUIRE(inserted);
      rids.push_back(rid);
    }

    // Verify we used many pages due to large record sizes
    std::set<page_id_t> unique_pages;
    for (const auto &rid : rids)
    {
      unique_pages.insert(rid.page_id_);
    }

    INFO("Number of pages used for large records: " << unique_pages.size());
    REQUIRE(unique_pages.size() >= 10); // Should use many pages

    // Verify all large records
    for (int i = 0; i < NUM_LARGE_RECORDS; ++i)
    {
      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rids[i], &retrieved_record);
      REQUIRE(found);

      auto deserialized_values = schema.deserialize_record(
          retrieved_record.get_data(),
          retrieved_record.get_size());

      REQUIRE(deserialized_values[0].get_integer() == i);

      std::string expected_data(1500, 'A' + (i % 26));
      expected_data += "_record_" + std::to_string(i);
      REQUIRE(deserialized_values[1].get_string() == expected_data);
    }
  }

  SECTION("Extreme Multi-Page Stress Test")
  {
    std::vector<Column> columns = {
        Column("id", ColumnType::INTEGER),
        Column("data1", ColumnType::VARCHAR, 100),
        Column("data2", ColumnType::VARCHAR, 150),
        Column("data3", ColumnType::VARCHAR, 80)};
    Schema schema(std::move(columns));

    const int NUM_EXTREME_RECORDS = 5000; // Very large number to force many pages
    std::vector<RecordID> rids;

    // Phase 1: Insert many records
    INFO("Inserting " << NUM_EXTREME_RECORDS << " records...");
    for (int i = 0; i < NUM_EXTREME_RECORDS; ++i)
    {
      std::string data1 = "Data1_" + std::to_string(i) + "_" + std::string(50, 'X');
      std::string data2 = "Data2_" + std::to_string(i) + "_" + std::string(80, 'Y');
      std::string data3 = "Data3_" + std::to_string(i) + "_" + std::string(30, 'Z');

      std::vector<Value> values = {
          Value(i),
          Value(data1),
          Value(data2),
          Value(data3)};

      auto serialized_data = schema.serialize_record(values);
      Record record(RecordID(), serialized_data.size(), serialized_data.data());

      RecordID rid;
      bool inserted = table_heap.insert_record(record, &rid);
      REQUIRE(inserted);
      rids.push_back(rid);
    }

    // Verify we used many pages
    std::set<page_id_t> unique_pages;
    for (const auto &rid : rids)
    {
      unique_pages.insert(rid.page_id_);
    }

    INFO("Number of pages used for extreme test: " << unique_pages.size());
    REQUIRE(unique_pages.size() >= 50); // Should use many pages

    // Phase 2: Random verification (sample subset for performance)
    INFO("Randomly verifying records...");
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, NUM_EXTREME_RECORDS - 1);

    for (int i = 0; i < 1000; ++i) // Verify 1000 random records
    {
      int idx = dis(gen);

      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rids[idx], &retrieved_record);
      REQUIRE(found);

      auto deserialized_values = schema.deserialize_record(
          retrieved_record.get_data(),
          retrieved_record.get_size());

      REQUIRE(deserialized_values[0].get_integer() == idx);

      std::string expected_data1 = "Data1_" + std::to_string(idx) + "_" + std::string(50, 'X');
      std::string expected_data2 = "Data2_" + std::to_string(idx) + "_" + std::string(80, 'Y');
      std::string expected_data3 = "Data3_" + std::to_string(idx) + "_" + std::string(30, 'Z');

      REQUIRE(deserialized_values[1].get_string() == expected_data1);
      REQUIRE(deserialized_values[2].get_string() == expected_data2);
      REQUIRE(deserialized_values[3].get_string() == expected_data3);
    }

    // Phase 3: Delete random records
    INFO("Deleting random records...");
    std::set<int> delete_indices_set;
    while (delete_indices_set.size() < NUM_EXTREME_RECORDS / 10) // Delete 10% of records
    {
      delete_indices_set.insert(dis(gen));
    }

    for (int idx : delete_indices_set)
    {
      bool deleted = table_heap.delete_record(rids[idx]);
      REQUIRE(deleted);
    }

    // Phase 4: Verify deleted records are gone and others remain
    INFO("Verifying deletions...");

    for (int i = 0; i < 500; ++i) // Check 500 random records
    {
      int idx = dis(gen);

      Record retrieved_record(RecordID(), 0, nullptr);
      bool found = table_heap.get_record(rids[idx], &retrieved_record);

      if (delete_indices_set.count(idx))
      {
        REQUIRE_FALSE(found); // Should be deleted
      }
      else
      {
        REQUIRE(found); // Should still exist

        auto deserialized_values = schema.deserialize_record(
            retrieved_record.get_data(),
            retrieved_record.get_size());
        REQUIRE(deserialized_values[0].get_integer() == idx);
      }
    }

    INFO("Extreme stress test completed successfully!");
  }

  // Cleanup
  if (std::filesystem::exists(db_file))
  {
    std::filesystem::remove(db_file);
  }
}
