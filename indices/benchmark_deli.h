#pragma once

#include <string_view>

#include "DeLI/include/DeLI/deli.h"
#include "unordered_dense/include/ankerl/unordered_dense.h"
#include "../src/benchmark.h"
#include "../src/utils.h"

// Wrapper object

namespace deli_testbed {
template <bool has_payload,
          typename KEY_TYPE, typename PAYLOAD_TYPE,
          bool dynamic,
          DeLI::RhtOptimization rht_opt,
          size_t rht_simd_unrolled,
          size_t rht_max_load_perc,
          DeLI::TopLevelOptimization opt,
          unsigned int high_bits,
          SearchMode mode = SearchMode::KEY_VALUE,
          size_t sampling = 16>
class BenchmarkDeLI
    : public std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                PredecessorSearchBase<KEY_TYPE>,
                                EmptyBase>
{
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    static_assert(mode != SearchMode::PREDECESSOR_SEARCH || !dynamic,
                  "PREDECESSOR_SEARCH mode is only supported for static DeLI");

    // In PS mode the index stores size_t ranks as payloads internally.
    using InternalPayload = std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                               size_t, PayloadType>;

    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;
    using index_t_payload = DeLI::DeLI<dynamic, rht_opt, rht_simd_unrolled, rht_max_load_perc, opt, KeyType, high_bits, InternalPayload, sizeof(KeyType) * CHAR_BIT, ankerl::unordered_dense::map>;
    using index_t_no_payload = DeLI::DeLI<dynamic, rht_opt, rht_simd_unrolled, rht_max_load_perc, opt, KeyType, high_bits, DeLI::NoPayload, sizeof(KeyType) * CHAR_BIT, ankerl::unordered_dense::map>;
    using index_t = std::conditional_t<(has_payload || mode == SearchMode::PREDECESSOR_SEARCH), index_t_payload, index_t_no_payload>;

    BenchmarkDeLI() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        // Populate sorted_keys_ and build a sampled (key, rank) index.
        this->ps_init(begin, end);
        const auto& keys = this->sorted_keys_;
        std::vector<std::pair<KeyType, size_t>> samples;
        samples.reserve(keys.size() / sampling + 1);
        for (size_t i = 0; i < keys.size(); i += sampling)
          samples.emplace_back(keys[i], i);
        index.bulk_load(samples.begin(), samples.end());
      } else {
        if constexpr (has_payload) {
          index.bulk_load(begin, end);
        } else {
          auto keys = std::ranges::subrange(begin, end) | std::ranges::views::transform([](auto const& p) { return p.first; });
          index.bulk_load(keys.begin(), keys.end());
        }
      }
    }

    PayloadType lower_bound(const KeyType key) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        // Look up the nearest sampled position >= key, then refine with a
        // small linear search within [pos - sampling, pos + 1).
        auto res = index.find_next_iter(key);
        size_t pos;
        if (res == index.end()) {
          pos = this->sorted_keys_.empty() ? 0 : this->sorted_keys_.size() - 1;
        } else {
          pos = static_cast<size_t>(res.payload());
        }
        const size_t lo = (pos >= sampling) ? pos - sampling : 0;
        const size_t hi = pos + 1;  // exclusive: covers keys before this sample
        return static_cast<PayloadType>(this->find_successor_in_range(key, lo, hi));
      } else if constexpr (has_payload) {
        auto res = index.find_next_iter(key);
        return res != index.end() ? res.payload() : PayloadType{};
      } else {
        auto res = index.find_next(key);
        return res ? res.value() : PayloadType{};
      }
    }

    void insert(const KeyType& key, const PayloadType& payload) requires(dynamic) {
      if constexpr (has_payload) {
        index.insert(key, payload);
      } else {
        index.insert(key);
      }
    }

    void insert(const KeyType& key, const PayloadType& payload) requires(!dynamic) {
      throw std::logic_error("Insert not supported for static DeLI");
    }

    void erase(const KeyType& key) requires (dynamic) {
      index.remove(key);
    }

    void erase(const KeyType& key) requires (!dynamic) {
      throw std::logic_error("Erase not supported for static DeLI");
    }

    static std::string name() {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        return "DeLI-Static-PS";
      } else if constexpr (dynamic) {
        return has_payload ? "DeLI-Dynamic-Payload" : "DeLI-Dynamic";
      } else {
        return has_payload ? "DeLI-Static-Payload" : "DeLI-Static";
      }
    }

    static std::string variant() {
      constexpr std::string_view rht_opt_str =
          rht_opt == DeLI::RhtOptimization::none ? "N" :
          rht_opt == DeLI::RhtOptimization::slot_index ? "SI" :
          rht_opt == DeLI::RhtOptimization::gap_fill_predecessor ? "GFP" :
          rht_opt == DeLI::RhtOptimization::gap_fill_successor ? "GFS" :
          rht_opt == DeLI::RhtOptimization::gap_fill_both ? "GFB" : "unknown";
      constexpr std::string_view opt_str =
          opt == DeLI::TopLevelOptimization::none ? "N" :
          //opt == DeLI::TopLevelOptimization::precompute ? "P" :
          opt == DeLI::TopLevelOptimization::bucket_index ? "BI" : "unknown";

      std::stringstream ss;
      ss << rht_opt_str << ";"
         << rht_simd_unrolled << ";"
         << rht_max_load_perc << ";"
         << opt_str << ";"
         << high_bits << ";"
         << index_t::rht_simd_width;
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH)
        ss << ";" << sampling;

      return ss.str();
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
    index_t index;
};

template <bool has_payload, typename KeyType, typename PayloadType>
void benchmark_deli_dynamic(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {
  // Check if there are duplicates
  for (size_t i = 1; i < key_values.size(); ++i) {
    if (key_values[i].first == key_values[i - 1].first) {
      return;
    }
  }

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, LOOKUP_UNIFORM, INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED, SHIFTING };
  for (const auto& wl : supported_workloads) {
#ifdef FAST_COMPILE
    using bench_t = std::conditional_t<sizeof(KeyType) * CHAR_BIT == 64,
                                      BenchmarkDeLI<has_payload, KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::bucket_index, 48>,
                                      BenchmarkDeLI<has_payload, KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, 10>>;
    deli_testbed::run_benchmark<bench_t>(config, key_values, wl, shifting_insert_key_values);
#endif

    // Define high_bits
    /////////// This works with one parameter
    // constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};

    // if (config.pareto) {
    //   auto run_pareto = []<unsigned int... bits>(std::integer_sequence<unsigned int, bits...>, const bench_config& cfg, const std::vector<std::pair<KeyType, PayloadType>>& kv, Workload workload) {
    //     (deli_testbed::run_benchmark<BenchmarkDeLI<has_payload, KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, bits>>(cfg, kv, workload), ...);
    //   };
    //   run_pareto(high_bits, config, key_values, wl);
    // }
    /////////// End with one parameter

#ifndef FAST_COMPILE
    if (config.pareto) {

      static_assert(sizeof(KeyType) * CHAR_BIT == 32 || sizeof(KeyType) * CHAR_BIT == 64, "Unsupported key size");
      constexpr auto high_bits = std::conditional_t<sizeof(KeyType) * CHAR_BIT == 32,
                                 std::integer_sequence<unsigned int, 0, 4, 8, 12, 16, 20>,
                                 std::integer_sequence<unsigned int, 24, 32, 40, 48>>{};

      constexpr auto load_balance = std::integer_sequence<size_t, 30, 40, 50, 60, 70>{};
      constexpr auto rht_simd_unrolled = std::integer_sequence<size_t, 0, 1, 2, 4>{};
      constexpr auto rht_opts = std::integer_sequence<int, 0, 1>{}; // Rht Optimization 2, 3, 4 unsupported with dynamic
      constexpr auto top_opts = std::integer_sequence<int, 1>{};

      // Check the high_bits threshold to avoid to test slow configurations (surely not in pareto).
      const KeyType lcp_xor = key_values.front().first ^ key_values.back().first;
      const size_t high_bits_th = lcp_xor == 0
          ? sizeof(KeyType) * CHAR_BIT
          : static_cast<size_t>(__builtin_clzll(static_cast<unsigned long long>(lcp_xor))
                                - (sizeof(unsigned long long) - sizeof(KeyType)) * CHAR_BIT);

      auto run_pareto = [high_bits_th]<unsigned int... bits, size_t... loads, size_t... simd_unrolled, int... rht_optimizations, int... top_optimizations>(
          std::integer_sequence<unsigned int, bits...>,
          std::integer_sequence<size_t, loads...>,
          std::integer_sequence<size_t, simd_unrolled...>,
          std::integer_sequence<int, rht_optimizations...>,
          std::integer_sequence<int, top_optimizations...>,
          const bench_config& cfg,
          std::vector<std::pair<KeyType, PayloadType>>& kv,
          Workload workload,
          const std::vector<std::pair<KeyType, PayloadType>>& shifting_kv) {

        // 5-level nested cartesian product
        auto run_for_bits = [&]<unsigned int B>() {
          // For mix_gauss datasets, high_bits starts from 12 (uint32) or 40 (uint64)
          constexpr unsigned int mix_gauss_min_bits = sizeof(KeyType) * CHAR_BIT == 32 ? 12u : 40u;
          if (cfg.data_filename.find("mix_gauss") != std::string::npos && B < mix_gauss_min_bits) return;

          auto run_for_loads = [&]<size_t L>() {
            auto run_for_simd = [&]<size_t S>() {
              auto run_for_rht = [&]<int R>() {
                auto run_for_top = [&]<int T>() {
                  // Convert int values to enum types at compile time
                  constexpr DeLI::RhtOptimization rht_opt = static_cast<DeLI::RhtOptimization>(R);
                  constexpr DeLI::TopLevelOptimization top_opt = static_cast<DeLI::TopLevelOptimization>(T);

                  // Check constraint: slot_index optimization cannot be used with SIMD (S > 0)
                  if constexpr ((rht_opt != DeLI::RhtOptimization::slot_index || S == 0) &&
                  // Check constraint: high_bits > 24 requires bucket_index top-level optimization
                                (B <= 24 || top_opt == DeLI::TopLevelOptimization::bucket_index)) {
                    if (B >= high_bits_th) {
                      deli_testbed::run_benchmark<BenchmarkDeLI<has_payload, KeyType, PayloadType, true, rht_opt, S, L, top_opt, B>>(cfg, kv, workload, shifting_kv);
                    }
                  }

                };
                (run_for_top.template operator()<top_optimizations>(), ...);
              };
              (run_for_rht.template operator()<rht_optimizations>(), ...);
            };
            (run_for_simd.template operator()<simd_unrolled>(), ...);
          };
          (run_for_loads.template operator()<loads>(), ...);
        };
        (run_for_bits.template operator()<bits>(), ...);
      };
      run_pareto(high_bits, load_balance, rht_simd_unrolled, rht_opts, top_opts, config, key_values, wl, shifting_insert_key_values);
    }
#endif // FAST_COMPILE
}
}

template <bool has_payload, typename KeyType, typename PayloadType,
          SearchMode mode = SearchMode::KEY_VALUE, size_t sampling = 16>
void benchmark_deli_static(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values = {}) {
  // Check if there are duplicates
  for (size_t i = 1; i < key_values.size(); ++i) {
    if (key_values[i].first == key_values[i - 1].first) {
      return;
    }
  }

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, LOOKUP_UNIFORM };
  for (const auto& wl : supported_workloads) {

#ifdef FAST_COMPILE
    using bench_t = std::conditional_t<sizeof(KeyType) * CHAR_BIT == 64,
                                      BenchmarkDeLI<has_payload, KeyType, PayloadType, false, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::bucket_index, 48, mode, sampling>,
                                      BenchmarkDeLI<has_payload, KeyType, PayloadType, false, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, 10, mode, sampling>>;
    deli_testbed::run_benchmark<bench_t>(config, key_values, wl, shifting_insert_key_values);
#endif

    // Define high_bits
    /////////// This works with one parameter
    // constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};

    // if (config.pareto) {
    //   auto run_pareto = []<unsigned int... bits>(std::integer_sequence<unsigned int, bits...>, const bench_config& cfg, const std::vector<std::pair<KeyType, PayloadType>>& kv, Workload workload) {
    //     (deli_testbed::run_benchmark<BenchmarkDeLI<has_payload, KeyType, PayloadType, false, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, bits>>(cfg, kv, workload), ...);
    //   };
    //   run_pareto(high_bits, config, key_values, wl);
    // }
    /////////// End with one parameter

#ifndef FAST_COMPILE
    if (config.pareto) {

      static_assert(sizeof(KeyType) * CHAR_BIT == 32 || sizeof(KeyType) * CHAR_BIT == 64, "Unsupported key size");
      constexpr auto high_bits = std::conditional_t<sizeof(KeyType) * CHAR_BIT == 32,
                                 std::integer_sequence<unsigned int, 0, 4, 8, 12, 16, 20>,
                                 std::integer_sequence<unsigned int, 24, 32, 40, 48>>{};

      constexpr auto load_balance = std::integer_sequence<size_t, 30, 40, 50, 60, 70>{};
      constexpr auto rht_simd_unrolled = std::integer_sequence<size_t, 0, 1, 2, 4>{};
      constexpr auto rht_opts = std::integer_sequence<int, 0, 1, 4>{}; // Rht Optimization 2, 3, 4 unsupported with dynamic
      constexpr auto top_opts = std::integer_sequence<int, 1>{};

      // Check the high_bits threshold to avoid to test slow configurations (surely not in pareto).
      const KeyType lcp_xor = key_values.front().first ^ key_values.back().first;
      const size_t high_bits_th = lcp_xor == 0
          ? sizeof(KeyType) * CHAR_BIT
          : static_cast<size_t>(__builtin_clzll(static_cast<unsigned long long>(lcp_xor))
                                - (sizeof(unsigned long long) - sizeof(KeyType)) * CHAR_BIT);

      auto run_pareto = [high_bits_th]<unsigned int... bits, size_t... loads, size_t... simd_unrolled, int... rht_optimizations, int... top_optimizations>(
          std::integer_sequence<unsigned int, bits...>,
          std::integer_sequence<size_t, loads...>,
          std::integer_sequence<size_t, simd_unrolled...>,
          std::integer_sequence<int, rht_optimizations...>,
          std::integer_sequence<int, top_optimizations...>,
          const bench_config& cfg,
          std::vector<std::pair<KeyType, PayloadType>>& kv,
          Workload workload,
          const std::vector<std::pair<KeyType, PayloadType>>& shifting_kv) {

        // 5-level nested cartesian product
        auto run_for_bits = [&]<unsigned int B>() {
          // For mix_gauss datasets, high_bits starts from 12 (uint32) or 40 (uint64)
          constexpr unsigned int mix_gauss_min_bits = sizeof(KeyType) * CHAR_BIT == 32 ? 12u : 40u;
          if (cfg.data_filename.find("mix_gauss") != std::string::npos && B < mix_gauss_min_bits) return;

          auto run_for_loads = [&]<size_t L>() {
            auto run_for_simd = [&]<size_t S>() {
              auto run_for_rht = [&]<int R>() {
                auto run_for_top = [&]<int T>() {
                  // Convert int values to enum types at compile time
                  constexpr DeLI::RhtOptimization rht_opt = static_cast<DeLI::RhtOptimization>(R);
                  constexpr DeLI::TopLevelOptimization top_opt = static_cast<DeLI::TopLevelOptimization>(T);

                  // Check constraint: slot_index optimization cannot be used with SIMD (S > 0)
                  if constexpr ((rht_opt != DeLI::RhtOptimization::slot_index || S == 0) &&
                  // Check constraint: high_bits > 24 requires bucket_index top-level optimization
                                (B <= 24 || top_opt == DeLI::TopLevelOptimization::bucket_index)) {
                    if (B >= high_bits_th) {
                      deli_testbed::run_benchmark<BenchmarkDeLI<has_payload, KeyType, PayloadType, false, rht_opt, S, L, top_opt, B, mode, sampling>>(cfg, kv, workload, shifting_kv);
                    }
                  }

                };
                (run_for_top.template operator()<top_optimizations>(), ...);
              };
              (run_for_rht.template operator()<rht_optimizations>(), ...);
            };
            (run_for_simd.template operator()<simd_unrolled>(), ...);
          };
          (run_for_loads.template operator()<loads>(), ...);
        };
        (run_for_bits.template operator()<bits>(), ...);
      };
      run_pareto(high_bits, load_balance, rht_simd_unrolled, rht_opts, top_opts, config, key_values, wl, shifting_insert_key_values);
    }
#endif // FAST_COMPILE
}
}

/// PREDECESSOR_SEARCH mode: sweeps over sampling values and delegates to benchmark_deli_static.
template <typename KeyType, typename PayloadType>
void benchmark_deli_static_ps(const bench_config& config,
                               std::vector<std::pair<KeyType, PayloadType>>& key_pairs_ps,
                               const std::vector<std::pair<KeyType, PayloadType>>& /* shifting unused */) {

  benchmark_deli_static<true, KeyType, PayloadType, SearchMode::PREDECESSOR_SEARCH, 32>(config, key_pairs_ps);
#ifndef FAST_COMPILE
  if (config.pareto) {
    benchmark_deli_static<true, KeyType, PayloadType, SearchMode::PREDECESSOR_SEARCH, 16>(config, key_pairs_ps);
    benchmark_deli_static<true, KeyType, PayloadType, SearchMode::PREDECESSOR_SEARCH, 64>(config, key_pairs_ps);
    benchmark_deli_static<true, KeyType, PayloadType, SearchMode::PREDECESSOR_SEARCH, 128>(config, key_pairs_ps);
#endif
}

}  // namespace deli_testbed
