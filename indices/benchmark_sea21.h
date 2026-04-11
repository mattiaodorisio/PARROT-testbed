#pragma once

#include <tdc/pred/dynamic/dynamic_index.hpp>
#include <tdc/pred/dynamic/dynamic_index_map.hpp>
#include <tdc/pred/dynamic/yfast.hpp>
#include <tdc/pred/dynamic/buckets/buckets.hpp>
#include <tdc/pred/dynamic/buckets/yfast_buckets.hpp>
#include "../src/benchmark.h"

#include <cstddef>
#include <string>
#include <type_traits>
#include <vector>

namespace deli_testbed {

// Compile-time string for use as a non-type template parameter (C++20).
template <std::size_t N>
struct FixedString {
  char buf[N + 1]{};
  constexpr FixedString(const char (&s)[N + 1]) noexcept {
    for (std::size_t i = 0; i <= N; ++i) buf[i] = s[i];
  }
  operator std::string() const { return {buf, N}; }
};
template <std::size_t N>
FixedString(const char (&)[N]) -> FixedString<N - 1>;

template <typename KEY_TYPE, typename PAYLOAD_TYPE, typename INDEX_TYPE,
          auto NAME, auto VARIANT, bool UINT32_ONLY = false>
class BenchmarkSEA21 {
 public:
  using KeyType     = KEY_TYPE;
  using PayloadType = PAYLOAD_TYPE;

  static constexpr SearchSemantics search_semantics = SearchSemantics::PREDECESSOR;

  BenchmarkSEA21() : index_() {}

  template <typename Iterator>
  void bulk_load(const Iterator begin, const Iterator end) {
    for (auto it = begin; it != end; ++it)
      index_.insert(static_cast<uint64_t>(it->first));
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

  static std::string name()    { return NAME; }
  static std::string variant() { return VARIANT; }

  bool applicable(const std::string&) {
    return !UINT32_ONLY || std::is_same_v<KeyType, uint32_t>;
  }

 private:
  INDEX_TYPE index_;
};

template <typename KeyType, typename PayloadType>
void benchmark_sea21(const bench_config& config,
                     std::vector<std::pair<KeyType, PayloadType>>& key_values,
                     const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_values) {
  constexpr Workload workloads[] = {
      LOOKUP_EXISTING,
      LOOKUP_IN_DISTRIBUTION,
      LOOKUP_UNIFORM,
      INSERT_IN_DISTRIBUTION,
      DELETE_EXISTING,
      MIXED,
      SHIFTING,
  };

  namespace sea = tdc::pred::dynamic;
  constexpr uint8_t kw = static_cast<uint8_t>(sizeof(KeyType) * 8);

  for (const auto& wl : workloads) {
    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::DynIndex<uint64_t, 24, sea::bucket_bv<uint64_t, 24>>,
        FixedString{"SEA21"}, FixedString{"bv-24"}, true>>(
        config, key_values, wl, shifting_key_values);

    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::DynIndex<uint64_t, 20, sea::bucket_hybrid<uint64_t, 20>>,
        FixedString{"SEA21"}, FixedString{"hybrid-20"}, true>>(
        config, key_values, wl, shifting_key_values);

    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::DynIndexMap<uint64_t, 24, sea::map_bucket_bv<uint64_t, 24>>,
        FixedString{"SEA21-Map"}, FixedString{"bv-24"}, true>>(
        config, key_values, wl, shifting_key_values);

    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::DynIndexMap<uint64_t, 20, sea::map_bucket_hybrid<uint64_t, 20>>,
        FixedString{"SEA21-Map"}, FixedString{"hybrid-20"}, true>>(
        config, key_values, wl, shifting_key_values);

    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::YFastTrie<sea::yfast_bucket<uint64_t, 9, 2>, kw>,
        FixedString{"SEA21-YFast"}, FixedString{"ul-bw9"}>>(
        config, key_values, wl, shifting_key_values);

    run_benchmark<BenchmarkSEA21<KeyType, PayloadType,
        sea::YFastTrie<sea::yfast_bucket_sl<uint64_t, 9, 2>, kw>,
        FixedString{"SEA21-YFast"}, FixedString{"sl-bw9"}>>(
        config, key_values, wl, shifting_key_values);
  }
}

}  // namespace deli_testbed
