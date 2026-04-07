#pragma once

#include "PGM-index/include/pgm/pgm_index_dynamic.hpp"
#include "../src/benchmark.h"
#include "../src/utils.h"

// Wrapper object

namespace deli_testbed {

// BenchmarkDynamicPGM supports both key_value and predecesosr search mode
template <typename KEY_TYPE, typename PAYLOAD_TYPE, size_t epsilon,
          SearchMode mode = SearchMode::KEY_VALUE,
          size_t sampling = 16>
class BenchmarkDynamicPGM
    : public std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                PredecessorSearchBase<KEY_TYPE>,
                                EmptyBase>
{
  public:
    using KeyType     = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

  private:
    using InternalPayload = std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                               size_t, PayloadType>;
    using IndexType = pgm::DynamicPGMIndex<KEY_TYPE, InternalPayload,
                                           pgm::PGMIndex<KEY_TYPE, epsilon>>;
    IndexType index;

  public:
    BenchmarkDynamicPGM() : index() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        this->ps_init(begin, end);
        const auto& keys = this->sorted_keys_;

        std::vector<std::pair<KeyType, size_t>> samples;
        samples.reserve(keys.size() / sampling + 1);
        for (size_t i = 0; i < keys.size(); i += sampling)
          samples.emplace_back(keys[i], i);

        // DynamicPGMIndex has no copy/move, so re-initialize via placement new.
        index.~IndexType();
        new (&index) IndexType(samples.data(), samples.data() + samples.size());
      } else {
        // Retain a copy of the data (not needed for queries but kept for parity).
        index.~IndexType();
        new (&index) IndexType(&*begin, &*end);
      }
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        auto it = index.lower_bound(key);
        size_t pos;
        if (it == index.end()) {
          pos = this->sorted_keys_.empty() ? 0 : this->sorted_keys_.size() - 1;
        } else {
          pos = static_cast<size_t>(it->second);
        }
        const size_t lo = (pos >= sampling) ? pos - sampling : 0;
        const size_t hi = pos + 1;
        return static_cast<PAYLOAD_TYPE>(this->find_successor_in_range(key, lo, hi));
      } else {
        auto lb = index.lower_bound(key);
        return lb != index.end() ? lb->second : PAYLOAD_TYPE{};
      }
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload)
        requires (mode == SearchMode::KEY_VALUE) {
      index.insert_or_assign(key, payload);
    }

    void erase(const KEY_TYPE& key)
        requires (mode == SearchMode::KEY_VALUE) {
      index.erase(key);
    }

    static std::string name() { return "Dynamic-PGM"; }

    static std::string variant() {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH)
        return "ps-" + std::to_string(epsilon) + "-s" + std::to_string(sampling);
      return std::to_string(epsilon);
    }

    bool applicable(const std::string&) { return true; }
};

// ── Benchmark driver functions ────────────────────────────────────────────────

/// KEY_VALUE mode: all dynamic workloads.
template <typename KeyType, typename PayloadType>
void benchmark_pgm_dynamic(const bench_config& config,
                           std::vector<std::pair<KeyType, PayloadType>>& key_values,
                           const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {

  // Check for sentinel value
  constexpr KeyType sentinel = std::numeric_limits<KeyType>::has_infinity
      ? std::numeric_limits<KeyType>::infinity()
      : std::numeric_limits<KeyType>::max();
  if (std::any_of(key_values.begin(), key_values.end(),
                  [sentinel](const auto& kv) { return kv.first == sentinel; })) {
    return;
  }

  constexpr Workload supported_workloads[] = {
      LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION,
      INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED, SHIFTING};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 16>>(
        config, key_values, wl, shifting_insert_key_values);

#ifndef FAST_COMPILE
    if (config.pareto) {
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 8>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 32>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 64>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 128>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 256>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 512>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType, 1024>>(config, key_values, wl, shifting_insert_key_values);
    }
#endif // FAST_COMPILE
  }
}

/// PREDECESSOR_SEARCH mode: static lookup workloads only (sampling approach).
/// key_pairs_ps must have key == payload (i.e. the "key_keys" array from main).
template <typename KeyType, typename PayloadType>
void benchmark_pgm_dynamic_ps(const bench_config& config,
                               std::vector<std::pair<KeyType, PayloadType>>& key_pairs_ps,
                               const std::vector<std::pair<KeyType, PayloadType>>& /* shifting unused */) {
  constexpr KeyType sentinel = std::numeric_limits<KeyType>::has_infinity
      ? std::numeric_limits<KeyType>::infinity()
      : std::numeric_limits<KeyType>::max();
  if (std::any_of(key_pairs_ps.begin(), key_pairs_ps.end(),
                  [sentinel](const auto& kv) { return kv.first == sentinel; })) {
    return;
  }

  constexpr Workload supported_workloads[] = {LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<
        BenchmarkDynamicPGM<KeyType, PayloadType, 16, SearchMode::PREDECESSOR_SEARCH, 16>>(
        config, key_pairs_ps, wl, {});
  }
}

}  // namespace deli_testbed
