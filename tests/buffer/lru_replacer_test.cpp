#include <catch2/catch_all.hpp>
#include "buffer/lru_replacer.h"
using namespace tinydb;

TEST_CASE("LRUReplacerTest", "[buffer]")
{

  SECTION("Sample Test")
  {
    LRUReplacer lru_replacer(7);

    lru_replacer.unpin(1);
    lru_replacer.unpin(2);
    lru_replacer.unpin(3);
    lru_replacer.unpin(4);
    lru_replacer.unpin(5);
    lru_replacer.unpin(6);
    lru_replacer.unpin(1);

    REQUIRE(lru_replacer.size() == 6);

    frame_id_t frame_id;
    lru_replacer.victim(&frame_id);
    REQUIRE(frame_id == 2);
    lru_replacer.victim(&frame_id);
    REQUIRE(frame_id == 3);
    lru_replacer.victim(&frame_id);
    REQUIRE(frame_id == 4);

    lru_replacer.pin(5);
    lru_replacer.pin(6);
    REQUIRE(lru_replacer.size() == 1);

    lru_replacer.unpin(2);
    REQUIRE(lru_replacer.size() == 2);

    lru_replacer.victim(&frame_id);
    REQUIRE(frame_id == 1);
    lru_replacer.victim(&frame_id);
    REQUIRE(frame_id == 2);
  }
}
