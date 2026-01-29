// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include <unordered_set>
#include <random>
#include "zipf.h"

std::mt19937_64 rand_gen(std::random_device{}());

struct bench_config {
  std::ofstream& out_file;
  int batch_size;
  const std::string& lookup_distribution;
  double time_limit;
  bool print_batch_stats;
  int max_batches;
};

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
  std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<int> dis(0, std::distance(data_begin, data_end) - 1);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  for (int i = 0; i < num_searches; i++) {
    int pos = dis(gen);
    data_sample.push_back(data_begin[pos]);
  }
  return data_sample;
}

template <class RandomIt>
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_non_existing_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
  using T = typename std::iterator_traits<RandomIt>::value_type;
  auto [min_it, max_it] = std::minmax_element(data_begin, data_end);
  T min_ = *min_it, max_ = *max_it;
  std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<T> dis(min_, max_);
  std::unordered_set<T> existing_keys(data_begin, data_end);
  std::vector<T> data_sample;
  data_sample.reserve(num_searches);
  int attempts = 0;
  int increases = 0;
  while (data_sample.size() < num_searches) {
    T key = dis(gen);
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
