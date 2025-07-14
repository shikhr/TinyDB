# Find Catch2
find_package(Catch2 3 REQUIRED)

# Source files
file(GLOB_RECURSE TEST_SRC "*.cpp")

# Add test executable
add_executable(tinydb_tests ${TEST_SRC})

# Add sources from the src directory
target_sources(tinydb_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/storage/disk_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/page.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/free_space_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/buffer/buffer_pool_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/buffer/lru_replacer.cpp
    ${CMAKE_SOURCE_DIR}/src/catalog/schema.cpp
    ${CMAKE_SOURCE_DIR}/src/catalog/catalog.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/table_heap.cpp
    ${CMAKE_SOURCE_DIR}/src/storage/table_page.cpp
    ${CMAKE_SOURCE_DIR}/src/parser/lexer.cpp
    ${CMAKE_SOURCE_DIR}/src/parser/parser.cpp
)

# Add include directories
target_include_directories(tinydb_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(tinydb_tests PRIVATE Catch2::Catch2WithMain)

# Include Catch test discovery
include(Catch)
catch_discover_tests(tinydb_tests)
