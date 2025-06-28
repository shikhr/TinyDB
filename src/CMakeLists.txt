# CMake for src directory

# Collect all source files
file(GLOB SRC_FILES "*.cpp")

# Define the main executable
add_executable(tinydb ${SRC_FILES})

# Include headers
target_include_directories(tinydb PRIVATE ${PROJECT_SOURCE_DIR}/include)
