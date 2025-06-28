# Find Catch2
find_package(Catch2 3 REQUIRED)

# Source files
file(GLOB TEST_SRC "*.cpp")

# Add test executable
add_executable(tinydb_tests ${TEST_SRC})
target_link_libraries(tinydb_tests PRIVATE Catch2::Catch2WithMain)

# Include Catch test discovery
include(Catch)
catch_discover_tests(tinydb_tests)
