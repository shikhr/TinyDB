# Minimum required version of CMake
cmake_minimum_required(VERSION 3.10)

# Define the project name and the programming language
project(TinyDB LANGUAGES CXX)

# Require C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set the output directories for the compiled binaries
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Include CTest module (for integrating unit testing support) and enable testing
include(CTest)
enable_testing()

# Add subdirectories for source code and tests
add_subdirectory(src)
add_subdirectory(tests) 