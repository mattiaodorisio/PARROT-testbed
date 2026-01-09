#pragma once

#include "DeLI/include/DeLI/deli.h"

// Wrapper object

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDeLI {
  public:
    BenchmarkDeLI() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      // std::sort(values, values + num_keys,
      //           [](auto const& a, auto const& b) { return a.first < b.first; });
      index.bulk_load(values, num_keys);
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      return *index.lower_bound(key);
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.remove(key);
    }
  
  private:
    DeLI::DeLI<KEY_TYPE, 10> index;
};