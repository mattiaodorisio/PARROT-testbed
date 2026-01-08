cmake_minimum_required(VERSION 3.16)
project(DeLI-testbed LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# if((CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
#         AND CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
# endif()
add_compile_options(-march=native)

# Add include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices)

add_executable(deLi_testbed src/main.cpp)

add_subdirectory(indices/DILI)
target_link_libraries(deLi_testbed PRIVATE dili_func)
target_link_libraries(deLi_testbed PRIVATE butree_func)
target_link_libraries(deLi_testbed PRIVATE global_func)
target_link_libraries(deLi_testbed PRIVATE utils_func)
