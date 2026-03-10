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
class BenchmarkStaticPGM {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkStaticPGM() : index() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      // Unlike dynamic indexes (ALEX, LIPP, Dynamic-PGM) PGM does not have payloads
      auto keys_iter = std::ranges::subrange(begin, end) | std::ranges::views::transform([](auto const& p) { return p.first; });
      keys.assign(keys_iter.begin(), keys_iter.end());
      index = pgm::PGMIndex<KEY_TYPE, epsilon>(keys.begin(), keys.end());
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      auto approx_pos = index.search(key);
      // Last mile search
      size_t end_limit = std::min(approx_pos.hi + 1, keys.size());
      auto it = std::lower_bound(keys.begin() + approx_pos.lo, keys.begin() + end_limit, key);
      if (it != keys.begin() + end_limit) {
        size_t index = std::distance(keys.begin(), it);
        return keys[index];
      } else {
        return PAYLOAD_TYPE{};
      }
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
    std::vector<KEY_TYPE> keys;
    pgm::PGMIndex<KEY_TYPE, epsilon> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_pgm_static(const bench_config& config, 
                           std::vector<std::pair<KeyType, PayloadType>>& key_values) {
  
  // Check for sentinel value
  constexpr KeyType sentinel = std::numeric_limits<KeyType>::has_infinity ? std::numeric_limits<KeyType>::infinity()
                                                                          : std::numeric_limits<KeyType>::max();
  if (std::any_of(key_values.begin(), key_values.end(), [sentinel](const auto& kv) { return kv.first == sentinel; })) {
    return;
  }
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 64>>(config, key_values, wl);

#ifndef FAST_COMPILE
  if (config.pareto) {
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 8>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 16>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 32>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 128>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 256>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 512>>(config, key_values, wl);
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 1024>>(config, key_values, wl);
  }  
#endif // FAST_COMPILE
  }
}
}  // namespace deli_testbed
