# CMake for src directory

# Collect all source files
file(GLOB SRC_FILES 
    "*.cpp"
    "storage/*.cpp"
    "buffer/*.cpp"
    "catalog/*.cpp"
    "parser/*.cpp"
    "execution/*.cpp"
)
    
# Define the main executable
add_executable(tinydb ${SRC_FILES})

# Include headers
target_include_directories(tinydb PRIVATE ${PROJECT_SOURCE_DIR}/include)

find_library(READLINE_LIB readline REQUIRED)
find_library(NCURSES_LIB ncurses REQUIRED)

target_link_libraries(tinydb PRIVATE ${READLINE_LIB} ${NCURSES_LIB})

