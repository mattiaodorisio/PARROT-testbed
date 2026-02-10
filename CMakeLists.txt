cmake_minimum_required(VERSION 3.16)
project(DeLI-testbed LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Add some compile options
add_compile_options(-g -fno-omit-frame-pointer -O3 -march=native)

# Add include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices)

add_executable(deLi_testbed src/main.cpp)
add_executable(generate_datasets src/generate_datasets.cpp)

add_subdirectory(indices/DILI)
target_link_libraries(deLi_testbed PRIVATE dili_func)
target_link_libraries(deLi_testbed PRIVATE butree_func)
target_link_libraries(deLi_testbed PRIVATE global_func)
target_link_libraries(deLi_testbed PRIVATE utils_func)
