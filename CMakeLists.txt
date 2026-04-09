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
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices/tlx)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices/unordered_dense)

add_executable(deLi_testbed src/main.cpp)
add_executable(generate_datasets src/generate_datasets.cpp)

# add_subdirectory(indices/DILI)
# target_link_libraries(deLi_testbed PRIVATE dili_func)
# target_link_libraries(deLi_testbed PRIVATE butree_func)
# target_link_libraries(deLi_testbed PRIVATE global_func)
# target_link_libraries(deLi_testbed PRIVATE utils_func)

add_subdirectory(indices/unordered_dense)
target_link_libraries(deLi_testbed PRIVATE unordered_dense::unordered_dense)

# sea21-predecessor: include paths + compile only the sources needed by DynIndex/bucket_bv/bucket_hybrid
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices/sea21-predecessor/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/indices/sea21-predecessor/extlib/robin-hood-hashing/src/include)
target_sources(deLi_testbed PRIVATE
  indices/sea21-predecessor/src/vec/fixed_width_int_vector.cpp
  indices/sea21-predecessor/src/vec/int_vector.cpp
  indices/sea21-predecessor/src/vec/bit_vector.cpp
  indices/sea21-predecessor/src/vec/bit_rank.cpp
  indices/sea21-predecessor/src/vec/allocate.cpp
  indices/sea21-predecessor/src/intrisics/intrisics.cpp
)