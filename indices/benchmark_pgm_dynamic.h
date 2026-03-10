#pragma once

#include "PGM-index/include/pgm/pgm_index_dynamic.hpp"
#include "../src/benchmark.h"
#include "../src/utils.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE, size_t epsilon>
class BenchmarkDynamicPGM {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkDynamicPGM() : index() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      auto keys = std::ranges::subrange(begin, end) | std::ranges::views::transform([](auto const& p) { return p.first; });

      // Retain a copy of the data
      data.assign(keys.begin(), keys.end());
      // Re-initialize the index (PGMDynamic does not support copy/move, nor provide a bulk_load function)
      index.~DynamicPGMIndex();
      new (&index) decltype(index)(&*begin, &*end);
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      auto lower_bound_it = index.lower_bound(key);
      return lower_bound_it != index.end() ? lower_bound_it->second : PAYLOAD_TYPE{};
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert_or_assign(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static std::string name() {
      return "Dynamic-PGM";
    }

    static std::string variant() {
      return std::to_string(epsilon);
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
    std::vector<KEY_TYPE> data;
    pgm::DynamicPGMIndex<KEY_TYPE, PAYLOAD_TYPE, pgm::PGMIndex<KEY_TYPE, epsilon>> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_pgm_dynamic(const bench_config& config, 
                           std::vector<std::pair<KeyType, PayloadType>>& key_values) {

  // Check for sentinel value
  constexpr KeyType sentinel = std::numeric_limits<KeyType>::has_infinity ? std::numeric_limits<KeyType>::infinity()
                                                                          : std::numeric_limits<KeyType>::max();
  if (std::any_of(key_values.begin(), key_values.end(), [sentinel](const auto& kv) { return kv.first == sentinel; })) {
    return;
  }

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 16>>(config, key_values, wl);

#ifndef FAST_COMPILE
    if (config.pareto) {
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 8>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 32>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 64>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 128>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 256>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 512>>(config, key_values, wl);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 1024>>(config, key_values, wl);
    }
#endif // FAST_COMPILE
  }
}
}  // namespace deli_testbed
