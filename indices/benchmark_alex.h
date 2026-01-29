#pragma once

#include "ALEX/alex.h"
#include "../src/workload.h"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
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

  private:
    alex::Alex<KEY_TYPE, PAYLOAD_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_alex(std::ofstream& out_file, 
                    std::vector<std::pair<KeyType, PayloadType>> key_values,
                    int batch_size, const std::string& lookup_distribution, 
                    double time_limit, bool print_batch_stats, 
                    int max_batches = 10) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkALEX<KeyType, PayloadType>, KeyType, PayloadType>(
        out_file, key_values, batch_size, lookup_distribution, 
        wl, time_limit, print_batch_stats, max_batches);
  }
}
}  // namespace deli_testbed
