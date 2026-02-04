#pragma once

#include "lipp/src/core/lipp.h"
#include "../src/benchmark.h"

// Wrapper object for LIPP

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkLIPP {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkLIPP() : index() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys,
                [](auto const& a, auto const& b) { return a.first < b.first; });
      index.bulk_load(values, num_keys);
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      return index.at(key);
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static std::string name() {
      return "LIPP";
    }

    static std::string variant() {
      return "none";
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
      LIPP<KEY_TYPE, PAYLOAD_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_lipp(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>> key_values) {

  constexpr Workload supported_workloads[] = {LOOKUP_EXISTING, INSERT_IN_DISTRIBUTION};

  for (const auto& wl : supported_workloads) {
    run_benchmark<BenchmarkLIPP<KeyType, PayloadType>>(config, key_values, wl);
  }
}
}  // namespace deli_testbed
