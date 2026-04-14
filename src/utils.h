// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include <unordered_set>
#include <random>
#include <functional>
#include <fstream>
#include <chrono>
#include <iostream>
#include <vector>
#include <algorithm>
#include <sstream>
#include <cassert>

#include "zipf.h"

static constexpr uint64_t seed = 123456789;
std::mt19937_64 rand_gen(seed);

struct bench_config {
  std::ofstream& out_file;
  const std::string data_filename;
  double time_limit;
  int batch_size;
  int min_batches;
  int max_batches;
  double rse_target;
  bool print_batch_stats;
  bool clear_cache;
  bool pareto;
  int min_size;
  int max_size;
  bool entire_dataset;
};

namespace utils {

#define FAST_COMPILE

// Loads values from binary file into vector.
template <typename T>
static std::vector<T> load_binary_data(const std::string& filename, size_t length = std::numeric_limits<size_t>::max()) {
  std::vector<T> data;
  std::ifstream in(filename, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "unable to open " << filename << std::endl;
    exit(EXIT_FAILURE);
  }
  // Read size.
  uint64_t size;
  in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
  size = std::min(size, length);
  data.resize(size);
  // Read values.
  in.read(reinterpret_cast<char*>(data.data()), size * sizeof(T));
  in.close();
  return data;
}

template <typename KeyType>
std::vector<KeyType> load_text_data(const std::string& file_path, size_t length = std::numeric_limits<size_t>::max()) {
  if constexpr (!std::is_same_v<KeyType, uint64_t>) {
    throw std::runtime_error("Text data loading supports only uint64_t key type.");
  }
  std::vector<KeyType> data;
  std::ifstream is(file_path.c_str());
  if (!is.is_open()) {
    return data;
  }
  size_t i = 0;
  std::string str;
  while (std::getline(is, str) && i++ < length) {
    std::istringstream ss(str);
    int64_t value;
    ss >> value;
    // shift values to positive range
    const uint64_t value_shifted = std::numeric_limits<uint64_t>::max() / 2 + value;
    data.push_back(value_shifted);
  }
  is.close();
  return data;
}

template <class RandomIt>
size_t count_distinct_sorted(RandomIt data_begin, RandomIt data_end) {
  if (data_begin == data_end) return 0;

  size_t count = 1;
  for (auto it = std::next(data_begin); it != data_end; ++it) {
    if (*it != *std::prev(it)) {
      count++;
    }
  }
  return count;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_existing_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches, bool allow_duplicates = true) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  const size_t data_size = std::distance(data_begin, data_end);
  std::uniform_int_distribution<size_t> dis(0, data_size - 1);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  if (allow_duplicates) {
    for (int i = 0; i < num_searches; i++) {
      size_t pos = dis(rand_gen);
      // Lower bound: Searches for the FIRST element which is not ordered before value
      while (pos > 0 && data_begin[pos].first == data_begin[pos - 1].first) --pos;
      data_sample.push_back(data_begin[pos]);
    }
  } else {
    if ((size_t)num_searches > data_size)
      return std::vector<T>(data_begin, data_end);
    std::unordered_set<size_t> used_indices;
    while ((int)data_sample.size() < num_searches) {
      size_t pos = dis(rand_gen);
      if (used_indices.insert(pos).second) {
        data_sample.push_back(data_begin[pos]);
      }
    }
  }
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_non_existing_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  assert(std::is_sorted(data_begin, data_end));
  using T = typename std::iterator_traits<RandomIt>::value_type;
  auto [min_it, max_it] = std::minmax_element(data_begin, data_end);
  T min_ = *min_it, max_ = *max_it;
  std::uniform_int_distribution<T> dis(min_, max_);
  std::unordered_set<T> added_keys;
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  int increases = 0;
  while (data_sample.size() < num_searches) {
    T key = dis(rand_gen);
    if (!std::binary_search(data_begin, data_end, key) && added_keys.insert(key).second) {
      data_sample.push_back(key);
    }
    if (++attempts > num_searches * 2) {
      if (++increases > 100) {
        throw std::runtime_error("Unable to find enough non-existing keys.");
      }
      T half_domain = (max_ - min_) / 2;
      min_ = std::numeric_limits<T>::min() + half_domain < min_ ? min_ - half_domain : std::numeric_limits<T>::min();
      max_ = std::numeric_limits<T>::max() - half_domain > max_ ? max_ + half_domain : std::numeric_limits<T>::max();
      dis = std::uniform_int_distribution<T>(min_, max_);
      attempts = 0;
    }
  }
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_non_existing_keys_in_distribution(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  assert(std::is_sorted(data_begin, data_end));
  using T = typename std::iterator_traits<RandomIt>::value_type;
  constexpr size_t interval_range = 100;
  const size_t size_ = std::distance(data_begin, data_end);
  std::uniform_int_distribution<size_t> dis(0, (size_ - 1) / interval_range);
  std::unordered_set<T> added_keys;
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  while (data_sample.size() < num_searches) {
    const size_t index_start_range = dis(rand_gen) * interval_range;
    const size_t index_end_range = std::min(index_start_range + interval_range - 1, size_ - 1);
    std::uniform_int_distribution<T> inner_dis(data_begin[index_start_range], data_begin[index_end_range]);
    T key = inner_dis(rand_gen);
    if (!std::binary_search(data_begin, data_end, key) && added_keys.insert(key).second) {
      data_sample.push_back(key);
    }
    if (++attempts > num_searches * 8) {
      break;
    }
  }

  // This can return a lower number of samples than requested
  return data_sample;
}

#if 0 // Unused
// Memory-efficient alternative to get_non_existing_keys for sorted ranges.
// Uses std::binary_search (O(log N)) instead of materializing an unordered_set
// of all N existing keys. Only allocates O(num_searches) memory.
// Requires: [data_begin, data_end) is sorted.
// already_found: keys already generated in a prior partial call; they are excluded.
template <class SortedIt>
std::vector<typename std::iterator_traits<SortedIt>::value_type>
get_non_existing_keys_sorted_range(
    const SortedIt data_begin, const SortedIt data_end, int num_searches,
    const std::unordered_set<typename std::iterator_traits<SortedIt>::value_type>& already_found = {})
{
  using T = typename std::iterator_traits<SortedIt>::value_type;
  if (data_begin == data_end || num_searches <= 0) return {};
  T min_ = *data_begin;
  T max_ = *std::prev(data_end);
  std::uniform_int_distribution<T> dis(min_, max_);
  // added_keys tracks uniqueness among the newly generated keys and already_found
  std::unordered_set<T> added_keys(already_found);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  int increases = 0;
  while ((int)data_sample.size() < num_searches) {
    T key = dis(rand_gen);
    if (!std::binary_search(data_begin, data_end, key) && added_keys.insert(key).second) {
      data_sample.push_back(key);
    }
    if (++attempts > num_searches * 2) {
      if (++increases > 100) {
        throw std::runtime_error("Unable to find enough non-existing keys.");
      }
      T half_domain = (max_ - min_) / 2;
      min_ = std::numeric_limits<T>::min() + half_domain < min_ ? min_ - half_domain : std::numeric_limits<T>::min();
      max_ = std::numeric_limits<T>::max() - half_domain > max_ ? max_ + half_domain : std::numeric_limits<T>::max();
      dis = std::uniform_int_distribution<T>(min_, max_);
      attempts = 0;
    }
  }
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_existing_keys_zipf(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  ScrambledZipfianGenerator zipf_gen(std::distance(data_begin, data_end), seed);
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    data_sample.push_back(data_begin[pos]);
  }
  return data_sample;
}
#endif

uint64_t timing(std::function<void()> fn) {
  const auto start = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
          .count();
}

}  // namespace utils
