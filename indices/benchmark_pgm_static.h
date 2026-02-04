#pragma once

#include <vector>
#include <ranges>
#include <algorithm>

#include "PGM-index/include/pgm/pgm_index.hpp"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE, size_t epsilon>
class BenchmarkStaticPGM {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkStaticPGM() : index() {}

    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys, [](auto const& a, auto const& b) { return a.first < b.first; });
      
      // Unlike dynamic indexes (ALEX, LIPP, Dynamic-PGM) PGM does not have payloads
      auto keys_iter = std::ranges::subrange(values, values + num_keys) | std::ranges::views::transform([](auto const& p) { return p.first; });
      keys.assign(keys_iter.begin(), keys_iter.end());
      index = pgm::PGMIndex<KEY_TYPE>(keys.begin(), keys.end());
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
                           std::vector<std::pair<KeyType, PayloadType>> key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType, 64>>(config, key_values, wl);
  }
}
}  // namespace deli_testbed
