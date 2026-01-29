#pragma once

#include "DeLI/include/DeLI/deli.h"
#include "../src/workload.h"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDeLI {
  public:
    BenchmarkDeLI() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys, [](auto const& a, auto const& b) { return a.first < b.first; });

      // Unlike dynamic indexes (ALEX, LIPP, Dynamic-PGM) DeLI does not have payloads
      auto keys = std::ranges::subrange(values, values + num_keys) | std::ranges::views::transform([](auto const& p) { return p.first; });

      // Retain a copy of the data
      data.assign(values, values + num_keys);
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

    static bool is_dynamic() {
      return true;
    }

    static std::string name() {
      return "DeLI";
    }

  private:
    std::vector<std::pair<KEY_TYPE, PAYLOAD_TYPE>> data;
    DeLI::DeLI<true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KEY_TYPE, 10> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_deli(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>> key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType>, KeyType, PayloadType>(config, key_values, wl);
  }
}
}  // namespace deli_testbed