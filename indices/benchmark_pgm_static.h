#pragma once

#include <vector>
#include <ranges>
#include <algorithm>

#include "PGM-index/include/pgm/pgm_index.hpp"
#include "../src/benchmark.h"
#include "../src/utils.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE, size_t epsilon>
class BenchmarkStaticPGM : public PredecessorSearchBase<KEY_TYPE> {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

    BenchmarkStaticPGM() : index() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      this->ps_init(begin, end);
      index = pgm::PGMIndex<KEY_TYPE, epsilon>(this->sorted_keys_.begin(), this->sorted_keys_.end());
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) const {
      auto approx_pos = index.search(key);
      size_t hi = std::min(approx_pos.hi + 1, this->sorted_keys_.size());
      return static_cast<PAYLOAD_TYPE>(this->find_successor_in_range(key, approx_pos.lo, hi));
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      throw std::runtime_error("Insert not supported on static PGM index");
    }

    void erase(const KEY_TYPE& key) {
      throw std::runtime_error("Erase not supported on static PGM index");
    }

    static std::string name() {
      return "Static-PGM";
    }

    static std::string variant() {
      return std::to_string(epsilon);
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
    pgm::PGMIndex<KEY_TYPE, epsilon> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_pgm_static(const bench_config& config,
                           std::vector<std::pair<KeyType, PayloadType>>& key_values,
                           const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {

  // Check for sentinel value
  constexpr KeyType sentinel = std::numeric_limits<KeyType>::has_infinity ? std::numeric_limits<KeyType>::infinity()
                                                                          : std::numeric_limits<KeyType>::max();
  if (std::any_of(key_values.begin(), key_values.end(), [sentinel](const auto& kv) { return kv.first == sentinel; })) {
    return;
  }

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, LOOKUP_UNIFORM };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 64>>(config, key_values, wl, shifting_insert_key_values);

#ifndef FAST_COMPILE
  if (config.pareto) {
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 8>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 16>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 32>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 128>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 256>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 512>>(config, key_values, wl, shifting_insert_key_values);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 1024>>(config, key_values, wl, shifting_insert_key_values);
  }
#endif // FAST_COMPILE
  }
}
}  // namespace deli_testbed
