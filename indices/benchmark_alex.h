#pragma once

#include "ALEX/alex.h"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkALEX {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkALEX() : index() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      index.bulk_load(values, num_keys);
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      auto it = index.lower_bound(key);
      return it.is_end() ? PAYLOAD_TYPE{} : it.payload();
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static std::string name() {
      return "ALEX";
    }

    static std::string variant() {
      return "none";
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
    alex::Alex<KEY_TYPE, PAYLOAD_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_alex(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>> key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION, MIXED };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkALEX<KeyType, PayloadType>>(config, key_values, wl);
  }
}
}  // namespace deli_testbed
