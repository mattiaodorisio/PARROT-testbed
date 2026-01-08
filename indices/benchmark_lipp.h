#pragma once

#include "lipp/src/core/lipp.h"

// Wrapper object for LIPP

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkLIPP {
  public:
    BenchmarkLIPP() : index() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      // std::sort(values, values + num_keys,
      //           [](auto const& a, auto const& b) { return a.first < b.first; });
      index.bulk_load(values, num_keys);
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      return index.at(key);
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }
  
  private:
     LIPP<KEY_TYPE, PAYLOAD_TYPE> index;
};