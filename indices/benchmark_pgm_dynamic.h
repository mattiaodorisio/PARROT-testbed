#pragma once

#include "PGM-index/include/pgm/pgm_index_dynamic.hpp"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDynamicPGM {
  public:
    BenchmarkDynamicPGM() : index() {}

    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys,
                [](auto const& a, auto const& b) { return a.first < b.first; });

      auto keys = std::ranges::subrange(values, values + num_keys) | std::ranges::views::transform([](auto const& p) { return p.first; });

      // Retain a copy of the data
      data.assign(keys.begin(), keys.end());
      // Re-initialize the index (PGMDynamic does not support copy/move, nor provide a bulk_load function)
      index.~DynamicPGMIndex();
      new (&index) decltype(index)(values, values + num_keys);
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      auto lower_bound_it = index.lower_bound(key);
      return lower_bound_it != index.end() ? lower_bound_it->second : PAYLOAD_TYPE{};
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert_or_assign(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static bool is_dynamic() {
      return true;
    }

    static std::string name() {
      return "Dynamic-PGM";
    }

  private:
    std::vector<KEY_TYPE> data;
    pgm::DynamicPGMIndex<KEY_TYPE, PAYLOAD_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_pgm_dynamic(const bench_config& config, 
                           std::vector<std::pair<KeyType, PayloadType>> key_values) {

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType>, KeyType, PayloadType>(config, key_values, wl);
  }
}
}  // namespace deli_testbed
