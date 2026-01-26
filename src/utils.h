// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include "zipf.h"

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
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_search_keys(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
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
std::vector<typename std::iterator_traits<RandomIt>::value_type> get_search_keys_zipf(const RandomIt data_begin, const RandomIt data_end, int num_searches) {
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
