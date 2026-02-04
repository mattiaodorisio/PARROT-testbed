#pragma once

#include <string_view>

#include "DeLI/include/DeLI/deli.h"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE,
          bool dynamic,
          DeLI::RhtOptimization rht_opt,
          size_t rht_simd_unrolled,
          size_t rht_max_load_perc,
          DeLI::TopLevelOptimization opt,
          typename T,
          unsigned int high_bits>
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
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      auto res = index.find_next(key);
      return res ? res.value() : PAYLOAD_TYPE{};
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key);
    }

    void erase(const KEY_TYPE& key) {
      index.remove(key);
    }

    static std::string name() {
      return "DeLI";
    }

    static std::string variant() {
      constexpr std::string_view dynamic_str = dynamic ? "dynamic" : "static";
      constexpr std::string_view rht_opt_str =
          rht_opt == DeLI::RhtOptimization::none ? "none" :
          rht_opt == DeLI::RhtOptimization::slot_index ? "slot_index" :
          rht_opt == DeLI::RhtOptimization::gap_fill_predecessor ? "gap_fill_predecessor" :
          rht_opt == DeLI::RhtOptimization::gap_fill_successor ? "gap_fill_successor" :
          rht_opt == DeLI::RhtOptimization::gap_fill_both ? "gap_fill_both" : "unknown";
      constexpr std::string_view opt_str =
          opt == DeLI::TopLevelOptimization::none ? "none" :
          opt == DeLI::TopLevelOptimization::precompute ? "precompute" :
          opt == DeLI::TopLevelOptimization::bucket_index ? "bucket_index" : "unknown";

      std::stringstream ss;
      ss << dynamic_str << ";"
         << rht_opt_str << ";"
         << rht_simd_unrolled << ";"
         << rht_max_load_perc << ";"
         << opt_str << ";"
         << high_bits;

      return ss.str();
    }

  private:
    std::vector<std::pair<KEY_TYPE, PAYLOAD_TYPE>> data;
    DeLI::DeLI<dynamic, rht_opt, rht_simd_unrolled, rht_max_load_perc, opt, T, high_bits> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_deli(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>> key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 2>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 3>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 4>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 5>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 6>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 7>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 8>, KeyType, PayloadType>(config, key_values, wl);
    // deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 9>, KeyType, PayloadType>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 70, DeLI::TopLevelOptimization::none, KeyType, 10>, KeyType, PayloadType>(config, key_values, wl);
  }
}
}  // namespace deli_testbed