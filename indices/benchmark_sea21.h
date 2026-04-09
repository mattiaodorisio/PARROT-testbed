#pragma once

#include <tdc/pred/dynamic/dynamic_index.hpp>
#include <tdc/pred/dynamic/buckets/buckets.hpp>
#include "../src/benchmark.h"

#include <string>
#include <type_traits>
#include <vector>

namespace deli_testbed {

/// Template parameters:
///   KEY_TYPE      - key type used by the benchmark (uint32_t or uint64_t)
///   PAYLOAD_TYPE  - payload type (key_pairs_ps mode: same as KEY_TYPE)
///   SAMPLING      - universe-sampling bits (bucket suffix width)
///   BUCKET_TMPL   - bucket template (bucket_bv or bucket_hybrid)
///
/// DynIndex provides predecessor(x) = largest key <= x (SearchSemantics::PREDECESSOR).
/// Benchmark<> computes expected values using upper_bound-1 for this semantics.
template <typename KEY_TYPE, typename PAYLOAD_TYPE, uint8_t SAMPLING = 16,
          template<typename, uint8_t> typename BUCKET_TMPL = tdc::pred::dynamic::bucket_bv>
class BenchmarkSEA21 {
 public:
  using KeyType     = KEY_TYPE;
  using PayloadType = PAYLOAD_TYPE;
  using BucketType  = BUCKET_TMPL<uint64_t, SAMPLING>;

  // SEA21 returns predecessor (largest key <= query), not lower_bound.
  static constexpr SearchSemantics search_semantics = SearchSemantics::PREDECESSOR;

  BenchmarkSEA21() : index_() {}

  template <typename Iterator>
  void bulk_load(const Iterator begin, const Iterator end) {
    for (auto it = begin; it != end; ++it) {
      index_.insert(static_cast<uint64_t>(it->first));
    }
  }

  PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
    auto res = index_.predecessor(static_cast<uint64_t>(key));
    return res.exists ? static_cast<PAYLOAD_TYPE>(res.key) : PAYLOAD_TYPE{};
  }

  void insert(const KEY_TYPE& key, const PAYLOAD_TYPE&) {
    index_.insert(static_cast<uint64_t>(key));
  }

  void erase(const KEY_TYPE& key) {
    index_.remove(static_cast<uint64_t>(key));
  }

  static std::string name() { return "SEA21"; }

  static std::string variant() {
    using bv_t     = tdc::pred::dynamic::bucket_bv<uint64_t, SAMPLING>;
    using hybrid_t = tdc::pred::dynamic::bucket_hybrid<uint64_t, SAMPLING>;
    if constexpr (std::is_same_v<BucketType, bv_t>)     return "bv-"     + std::to_string(SAMPLING);
    if constexpr (std::is_same_v<BucketType, hybrid_t>) return "hybrid-" + std::to_string(SAMPLING);
    return "s" + std::to_string(SAMPLING);
  }

  bool applicable(const std::string&) { return true; }

 private:
  tdc::pred::dynamic::DynIndex<uint64_t, SAMPLING, BucketType> index_;
};

template <typename KeyType, typename PayloadType>
void benchmark_sea21(const bench_config& config,
                     std::vector<std::pair<KeyType, PayloadType>>& key_values,
                     const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_values) {
  constexpr Workload workloads[] = {
      LOOKUP_EXISTING,
      LOOKUP_IN_DISTRIBUTION,  // predecessor semantics — Benchmark<> validates against actual predecessor
      LOOKUP_UNIFORM,
      INSERT_IN_DISTRIBUTION,
      DELETE_EXISTING,
      MIXED,
      SHIFTING,
  };

  for (const auto& wl : workloads) {
    run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 16, tdc::pred::dynamic::bucket_bv>>(
      config, key_values, wl, shifting_key_values);

#ifndef FAST_COMPILE
    if (config.pareto) {
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 8, tdc::pred::dynamic::bucket_bv>>(
          config, key_values, wl, shifting_key_values);
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 24, tdc::pred::dynamic::bucket_bv>>(
          config, key_values, wl, shifting_key_values);
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 32, tdc::pred::dynamic::bucket_bv>>(
          config, key_values, wl, shifting_key_values);
    }
#endif

  }

  for (const auto& wl : workloads) {
    run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 16, tdc::pred::dynamic::bucket_hybrid>>(
        config, key_values, wl, shifting_key_values);

#ifndef FAST_COMPILE
    if (config.pareto) {
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 8, tdc::pred::dynamic::bucket_hybrid>>(
          config, key_values, wl, shifting_key_values);
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 24, tdc::pred::dynamic::bucket_hybrid>>(
          config, key_values, wl, shifting_key_values);
      run_benchmark<BenchmarkSEA21<KeyType, PayloadType, 32, tdc::pred::dynamic::bucket_hybrid>>(
          config, key_values, wl, shifting_key_values);
    }
#endif
  }
}

}  // namespace deli_testbed
