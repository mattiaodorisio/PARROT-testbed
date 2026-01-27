#pragma once

#include "ALEX/alex.h"
#include "../src/workload.h"

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
      return index.lower_bound(key) == index.end() ? PAYLOAD_TYPE{} : index.lower_bound(key).payload();
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

    static std::vector<Workload> supported_workloads() {
      return {LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION};
    }
  
  private:
    alex::Alex<KEY_TYPE, PAYLOAD_TYPE> index;
};