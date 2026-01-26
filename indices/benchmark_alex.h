#pragma once

#include "ALEX/alex.h"

// Wrapper object

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkALEX {
  public:
    BenchmarkALEX() : index() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys,
                [](auto const& a, auto const& b) { return a.first < b.first; });
      index.bulk_load(values, num_keys);
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      return *index.get_payload(key);
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static bool is_dynamic() {
      return true;
    }

    static std::string name() {
      return "ALEX";
    }
  
  private:
    alex::Alex<KEY_TYPE, PAYLOAD_TYPE> index;
};