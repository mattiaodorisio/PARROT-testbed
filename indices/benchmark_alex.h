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
  
    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      index.bulk_load(&*begin, std::distance(begin, end));
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
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED, SHIFTING };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkALEX<KeyType, PayloadType>>(config, key_values, wl, shifting_insert_key_values);
  }
}
}  // namespace deli_testbed
