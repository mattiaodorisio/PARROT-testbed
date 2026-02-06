// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include <unordered_set>
#include <random>
#include "zipf.h"

static constexpr uint64_t seed = 123456789;
std::mt19937_64 rand_gen(seed);

struct bench_config {
  std::ofstream& out_file;
  const std::string data_filename;
  const std::string& lookup_distribution;
  double time_limit;
  int batch_size;
  int max_batches;
  bool print_batch_stats;
  bool clear_cache;
  bool pareto;
  int min_size;
  int max_size;
};

namespace utils {

#define FAST_COMPILE
#define DELI_FAST_CONFIG

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

template <class T>
bool load_text_data(T array[], int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str());
  if (!is.is_open()) {
    return false;
  }
  int i = 0;
  std::string str;
  while (std::getline(is, str) && i < length) {
    std::istringstream ss(str);
    ss >> array[i];
    i++;
  }
  is.close();
  return true;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_existing_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  const size_t data_size = std::distance(data_begin, data_end);
  std::uniform_int_distribution<int> dis(0, data_size - 1);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  for (int i = 0; i < num_searches; i++) {
    int pos = dis(rand_gen);
    // Lower bound: Searches for the FIRST element which is not ordered before value
    while (pos > 0 && data_begin[pos].first == data_begin[pos - 1].first) --pos;
    data_sample.push_back(data_begin[pos]);
  }
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_non_existing_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  auto [min_it, max_it] = std::minmax_element(data_begin, data_end);
  T min_ = *min_it, max_ = *max_it;
  std::uniform_int_distribution<T> dis(min_, max_);
  std::unordered_set<T> existing_keys(data_begin, data_end);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  int increases = 0;
  while (data_sample.size() < num_searches) {
    T key = dis(rand_gen);
    if (existing_keys.find(key) == existing_keys.end()) {
      data_sample.push_back(key);
      existing_keys.insert(key);
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
  using T = typename std::iterator_traits<RandomIt>::value_type;
  auto [min_it, max_it] = std::minmax_element(data_begin, data_end);
  T min_ = *min_it, max_ = *max_it;
  constexpr size_t interval_range = 100;
  const size_t size_ = std::distance(data_begin, data_end);
  std::uniform_int_distribution<size_t> dis(0, (size_ - 1) / interval_range);
  std::unordered_set<T> existing_keys(data_begin, data_end);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  while (data_sample.size() < num_searches) {
    const size_t index_start_range = dis(rand_gen) * interval_range;
    const size_t index_end_range = std::min(index_start_range + interval_range - 1, size_ - 1);
    std::uniform_int_distribution<T> inner_dis(data_begin[index_start_range], data_begin[index_end_range]);
    T key = inner_dis(rand_gen);
    if (existing_keys.find(key) == existing_keys.end()) {
      data_sample.push_back(key);
      existing_keys.insert(key);
    }
    if (++attempts > num_searches * 8) {
      break;
    }
  }

  // This can return a lower number of samples than requested
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_existing_keys_zipf(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  ScrambledZipfianGenerator zipf_gen(std::distance(data_begin, data_end));
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    data_sample.push_back(data_begin[pos]);
  }
  return data_sample;
}

uint64_t timing(std::function<void()> fn) {
  const auto start = std::chrono::high_resolution_clock::now();
  fn();
  const auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
          .count();
}

}  // namespace utils
