#pragma once

#include "DILI/src/dili/DILI.h"

// Wrapper object

template <typename KeyType, typename PayloadType>
class BenchmarkDILI {
  static_assert(std::is_same<KeyType, long>::value,
                "DILI only supports long keys");
  static_assert(std::is_same<PayloadType, long>::value,
                "DILI only supports long payloads");

  public:
    using KeyType = KeyType;
    using PayloadType = PayloadType;

    BenchmarkDILI() {}

    void bulk_load(std::pair<KeyType, PayloadType>* values, size_t num_keys) {
      std::vector<std::pair<long, long>> dili_values(num_keys);
      for (size_t i = 0; i < num_keys; i++) {
        dili_values[i].first = values[i].first;
        dili_values[i].second = values[i].second;
      }
      for (size_t i = 0; i < num_keys; i++) {
        index.insert(dili_values[i].first, dili_values[i].second);
      }
      // index.bulk_load(dili_values);
    }
  
    PayloadType lower_bound(const KeyType key) {
      return index.search(key);
    }
  
    void insert(const KeyType& key, const PayloadType& payload) {
      index.insert(key, payload);
    }

    void erase(const KeyType& key) {
      index.delete_key(key);
    }

    static std::vector<Workload> supported_workloads() {
      return {LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION, MIXED};
    }

    bool applicable(const std::string& data_filename) {
      return false;
    }
  
  private:
    DILI index;
};