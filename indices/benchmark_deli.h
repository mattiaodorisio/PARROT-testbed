#pragma once

#include <string_view>

#include "DeLI/include/DeLI/deli.h"
#include "../src/benchmark.h"
#include "../src/utils.h"

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
    using index_t = DeLI::DeLI<dynamic, rht_opt, rht_simd_unrolled, rht_max_load_perc, opt, T, high_bits>;
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkDeLI() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
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
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) requires(dynamic) {
      index.insert(key);
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) requires(!dynamic) {
      throw std::logic_error("Insert not supported for static DeLI");
    }

    void erase(const KEY_TYPE& key) requires (dynamic) {
      index.remove(key);
    }

    void erase(const KEY_TYPE& key) requires (!dynamic) {
      throw std::logic_error("Erase not supported for static DeLI");
    }

    static std::string name() {
      if constexpr (dynamic) {
        return "DeLI-Dynamic";
      } else {
        return "DeLI-Static";
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
          opt == DeLI::TopLevelOptimization::precompute ? "P" :
          opt == DeLI::TopLevelOptimization::bucket_index ? "BI" : "unknown";

      std::stringstream ss;
      ss << rht_opt_str << ";"
         << rht_simd_unrolled << ";"
         << rht_max_load_perc << ";"
         << opt_str << ";"
         << high_bits <<";"
         << index_t::rht_simd_width;

      return ss.str();
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
    std::vector<std::pair<KEY_TYPE, PAYLOAD_TYPE>> data;
    index_t index;
};

template <typename KeyType, typename PayloadType>
void benchmark_deli_dynamic(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, KeyType, 10>>(config, key_values, wl);

    // Define high_bits
    /////////// This works with one parameter
    // constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};

    // if (config.pareto) {
    //   auto run_pareto = []<unsigned int... bits>(std::integer_sequence<unsigned int, bits...>, const bench_config& cfg, const std::vector<std::pair<KeyType, PayloadType>>& kv, Workload workload) {
    //     (deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, KeyType, bits>>(cfg, kv, workload), ...);
    //   };
    //   run_pareto(high_bits, config, key_values, wl);
    // }
    /////////// End with one parameter

#ifndef FAST_COMPILE
    if (config.pareto) {

      #ifdef DELI_FAST_CONFIG
        constexpr auto high_bits = std::integer_sequence<unsigned int, 4, 8, 12, 16>{};
      #else
        constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};
      #endif
      
      constexpr auto load_balance = std::integer_sequence<size_t, 20, 40, 60>{};
      constexpr auto rht_simd_unrolled = std::integer_sequence<size_t, 0, 1, 2, 4>{};
      constexpr auto rht_opts = std::integer_sequence<int, 0, 1>{}; // Rht Optimization 2, 3, 4 unsupported with dynamic
      constexpr auto top_opts = std::integer_sequence<int, 0, 2>{}; // Top optimization 1 unsupported with dynamic
    
      auto run_pareto = []<unsigned int... bits, size_t... loads, size_t... simd_unrolled, int... rht_optimizations, int... top_optimizations>(
          std::integer_sequence<unsigned int, bits...>, 
          std::integer_sequence<size_t, loads...>, 
          std::integer_sequence<size_t, simd_unrolled...>,
          std::integer_sequence<int, rht_optimizations...>,
          std::integer_sequence<int, top_optimizations...>,
          const bench_config& cfg, 
          std::vector<std::pair<KeyType, PayloadType>>& kv, 
          Workload workload) {
        
        // 5-level nested cartesian product
        auto run_for_bits = [&]<unsigned int B>() {
          auto run_for_loads = [&]<size_t L>() {
            auto run_for_simd = [&]<size_t S>() {
              auto run_for_rht = [&]<int R>() {
                auto run_for_top = [&]<int T>() {
                  // Convert int values to enum types at compile time
                  constexpr DeLI::RhtOptimization rht_opt = static_cast<DeLI::RhtOptimization>(R);
                  constexpr DeLI::TopLevelOptimization top_opt = static_cast<DeLI::TopLevelOptimization>(T);

                  // Check constraint: slot_index optimization (1) cannot be used with SIMD (S > 0)
                  if constexpr (rht_opt != DeLI::RhtOptimization::slot_index || S == 0) {
                    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, true, rht_opt, S, L, top_opt, KeyType, B>>(cfg, kv, workload);
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
      run_pareto(high_bits, load_balance, rht_simd_unrolled, rht_opts, top_opts, config, key_values, wl);
    }
#endif // FAST_COMPILE
}
}

template <typename KeyType, typename PayloadType>
void benchmark_deli_static(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values) {
  
  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, false, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, KeyType, 10>>(config, key_values, wl);

    // Define high_bits
    /////////// This works with one parameter
    // constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};

    // if (config.pareto) {
    //   auto run_pareto = []<unsigned int... bits>(std::integer_sequence<unsigned int, bits...>, const bench_config& cfg, const std::vector<std::pair<KeyType, PayloadType>>& kv, Workload workload) {
    //     (deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, false, DeLI::RhtOptimization::none, 2, 80, DeLI::TopLevelOptimization::none, KeyType, bits>>(cfg, kv, workload), ...);
    //   };
    //   run_pareto(high_bits, config, key_values, wl);
    // }
    /////////// End with one parameter

#ifndef FAST_COMPILE
    if (config.pareto) {

      #ifdef DELI_FAST_CONFIG
        constexpr auto high_bits = std::integer_sequence<unsigned int, 4, 8, 12, 16>{};
      #else
        constexpr auto high_bits = std::make_integer_sequence<unsigned int, 11>{};
      #endif
      
      constexpr auto load_balance = std::integer_sequence<size_t, 20, 40, 60>{};
      constexpr auto rht_simd_unrolled = std::integer_sequence<size_t, 0, 1, 2, 4>{};
      constexpr auto rht_opts = std::integer_sequence<int, 0, 1, 2, 3, 4>{}; // Rht Optimization 2, 3, 4 unsupported with dynamic
      constexpr auto top_opts = std::integer_sequence<int, 0, 1, 2>{}; // Top optimization 1 unsupported with dynamic
    
      auto run_pareto = []<unsigned int... bits, size_t... loads, size_t... simd_unrolled, int... rht_optimizations, int... top_optimizations>(
          std::integer_sequence<unsigned int, bits...>, 
          std::integer_sequence<size_t, loads...>, 
          std::integer_sequence<size_t, simd_unrolled...>,
          std::integer_sequence<int, rht_optimizations...>,
          std::integer_sequence<int, top_optimizations...>,
          const bench_config& cfg, 
          const std::vector<std::pair<KeyType, PayloadType>>& kv, 
          Workload workload) {
        
        // 5-level nested cartesian product
        auto run_for_bits = [&]<unsigned int B>() {
          auto run_for_loads = [&]<size_t L>() {
            auto run_for_simd = [&]<size_t S>() {
              auto run_for_rht = [&]<int R>() {
                auto run_for_top = [&]<int T>() {
                  // Convert int values to enum types at compile time
                  constexpr DeLI::RhtOptimization rht_opt = static_cast<DeLI::RhtOptimization>(R);
                  constexpr DeLI::TopLevelOptimization top_opt = static_cast<DeLI::TopLevelOptimization>(T);

                  // Check constraint: slot_index optimization (1) cannot be used with SIMD (S > 0)
                  if constexpr (rht_opt != DeLI::RhtOptimization::slot_index || S == 0) {
                    deli_testbed::run_benchmark<BenchmarkDeLI<KeyType, PayloadType, false, rht_opt, S, L, top_opt, KeyType, B>>(cfg, kv, workload);
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
      run_pareto(high_bits, load_balance, rht_simd_unrolled, rht_opts, top_opts, config, key_values, wl);
    }
#endif // FAST_COMPILE
}
}
}  // namespace deli_testbed
