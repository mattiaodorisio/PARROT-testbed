#pragma once

#include "DeLI/include/DeLI/deli.h"

// Wrapper object

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDeLI {
  public:
    BenchmarkDeLI() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::vector<KEY_TYPE> keys(num_keys);
      for (size_t i = 0; i < num_keys; ++i) {
        keys[i] = values[i].first;
      }
      std::sort(keys.begin(), keys.end());
      index.bulk_load(keys.begin(), keys.end());
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      auto res = index.find_next(key);
      return res ? res.value() : PAYLOAD_TYPE{};
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key);
    }

    void erase(const KEY_TYPE& key) {
      index.remove(key);
    }
  
  private:
    DeLI::DeLI<KEY_TYPE, 54> index;
};