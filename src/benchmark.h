#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <chrono>
#include <vector>
#include <ranges>
#include <iomanip>
#include <random>
#include <unordered_set>
#include <queue>
#include <numeric>

#include "utils.h"

namespace deli_testbed {
enum Workload : int {
  LOOKUP_EXISTING,
  LOOKUP_IN_DISTRIBUTION,
  INSERT_IN_DISTRIBUTION,
  DELETE_EXISTING,
  MIXED,
  
  NUM_WORKLOADS,  // TODO: to be implemented
  SHIFTING,
};

std::string workload_name(Workload workload) {
  switch (workload) {
    case LOOKUP_EXISTING: return "LOOKUP_EXISTING";
    case LOOKUP_IN_DISTRIBUTION: return "LOOKUP_IN_DISTRIBUTION";
    case INSERT_IN_DISTRIBUTION: return "INSERT_IN_DISTRIBUTION";
    case DELETE_EXISTING: return "DELETE_EXISTING";
    case MIXED: return "MIXED";
    case SHIFTING: return "SHIFTING";
    default: return "UNKNOWN_WORKLOAD";
  }
}

template <typename KeyType, typename PayloadType>
class Benchmark {
 public:
  Benchmark(std::vector<std::pair<KeyType, PayloadType>>& key_values)
      : key_values_(key_values) {
    // Initialize cache clearing buffer - allocate ~64MB for cache flushing
    constexpr size_t cache_flush_size = 64 * 1024 * 1024 / sizeof(uint64_t);
    memory_.resize(cache_flush_size);
    // Initialize with some values to avoid potential issues
    std::iota(memory_.begin(), memory_.end(), 0);
  }

  template <class Index>
  void BulkLoad(Index& index) {
    if (!std::is_sorted(key_values_.begin(), key_values_.end(),
                        [](auto const& a, auto const& b) { return a.first < b.first; })) {
      throw std::runtime_error("Data must be sorted by key before bulk loading");
    }
    index.bulk_load(key_values_.data(), key_values_.size());
  }

  // Initialize shifting window state - must be called after BulkLoad
  void InitializeShiftingWindow() {
    if (!shifting_initialized_) {
      // Find the maximum key from bulk loaded data
      auto max_it = std::max_element(key_values_.begin(), key_values_.end(),
                                   [](const auto& a, const auto& b) { return a.first < b.first; });
      next_insert_key_ = (max_it != key_values_.end()) ? max_it->first + 1 : KeyType{1};
      
      // Populate priority queue with all current keys
      for (const auto& kv : key_values_) {
        current_keys_.push(kv.first);
      }
      
      shifting_initialized_ = true;
    }
  }

  void PrintResult(const std::string& index_name,
                   const std::string& index_variant,
                   const std::string& workload_name,
                   int batch_no, size_t init_num_keys, size_t batch_operations,
                   const std::string& lookup_distribution, double batch_time,
                   std::ofstream& out_file) {
    
    bool error = (batch_time == std::numeric_limits<uint64_t>::max());
    double batch_overall_throughput = (batch_time > 0) ? (batch_operations / batch_time * 1e9) : 0.0;

    // Log results in the same format as main.cpp
    out_file << "RESULT "
            << "index_name=" << index_name << " "
            << "index_variant=" << index_variant << " "
            << "batch_no=" << batch_no << " "
            << "workload_type=" << workload_name << " "
            << "init_num_keys=" << init_num_keys << " "
            << "batch_operations=" << batch_operations << " "
            << "lookup_distribution=" << lookup_distribution << " "
            << std::fixed << std::setprecision(2) << "throughput=" << (!error ? std::to_string(batch_overall_throughput) : "error") << " "
            << std::fixed << std::setprecision(6) << "total_time=" << (!error ? std::to_string(1.0 * batch_time / 1e9) : "error") << std::endl;
  }

  template <Workload W, class Index>
  uint64_t RunWorkload(Index& index, const bench_config& config) {
    if constexpr (W == LOOKUP_EXISTING) {
      return DoLookupExisting(index, config);
    } else if constexpr (W == LOOKUP_IN_DISTRIBUTION) {
      return DoLookupInDistribution(index, config);
    } else if constexpr (W == INSERT_IN_DISTRIBUTION) {
      return DoInsertInDistribution(index, config);
    } else if constexpr (W == DELETE_EXISTING) {
      return DoDeleteExisting(index, config);
    } else if constexpr (W == MIXED) {
      return DoMixed(index, config);
    } else if constexpr (W == SHIFTING) {
      return DoShifting(index, config);
    } else {
      throw std::runtime_error("Workload not implemented");
    }
    return 0;
  }

  private:
  template <class Index>
  uint64_t DoLookupExisting(Index& index, const bench_config& config) {
    // Get existing keys from the dataset
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs = 
        utils::get_existing_keys(key_values_.begin(), key_values_.end(), config.batch_size);

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, lookup_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoEqualityLookupsCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, lookup_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoEqualityLookupsCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

  template <class Index>
  uint64_t DoLookupInDistribution(Index& index, const bench_config& config) {
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs;
    
    // Prepare the lookup pair vector
    {
      auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
      std::vector<KeyType> lookup_keys = utils::get_non_existing_keys_in_distribution(keys.begin(), keys.end(), config.batch_size);

      if (lookup_keys.size() < config.batch_size) {
        // existing keys are the ones in keys and in insert_keys
        std::vector<KeyType> existing_keys;
        existing_keys.reserve(keys.size() + lookup_keys.size());
        existing_keys.insert(existing_keys.end(), keys.begin(), keys.end());
        existing_keys.insert(existing_keys.end(), lookup_keys.begin(), lookup_keys.end());

        std::vector<KeyType> extra_keys =
            utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - lookup_keys.size());
        lookup_keys.insert(lookup_keys.end(), extra_keys.begin(), extra_keys.end());
      }

      lookup_pairs.reserve(lookup_keys.size());
      for (const auto& key : lookup_keys) {
        auto expected_payload_it = std::lower_bound(key_values_.begin(), key_values_.end(), key,
          [](const auto& pair, const KeyType& k) { return pair.first < k; });
        PayloadType expected_payload = (expected_payload_it != key_values_.end()) ? expected_payload_it->second : PayloadType{};
        lookup_pairs.emplace_back(key, expected_payload);
      }
    }

    if (config.clear_cache)
      return DoCoreLoop<Index, true, true>(index, lookup_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoEqualityLookupsCoreLoop<Index, true, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, lookup_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoEqualityLookupsCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

  template <class Index>
  uint64_t DoDeleteExisting(Index& index, const bench_config& config) {
    // Get existing keys from the dataset (opposite of insert workload which gets non-existing keys)
    std::vector<std::pair<KeyType, PayloadType>> delete_pairs = 
        utils::get_existing_keys(key_values_.begin(), key_values_.end(), config.batch_size);

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, delete_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoDeletesCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, delete_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoDeletesCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

private:
  bool CheckResults(PayloadType actual, PayloadType expected, KeyType lookup_key) {
    if (actual != expected) {
       // TODO: Check if further cheks are needed here

      std::cerr << "equality lookup returned wrong result:" << std::endl;
      std::cerr << "lookup key: " << lookup_key << std::endl;
      std::cerr << "actual: " << actual << ", expected: " << expected
                << std::endl;

      return false;
    }

    return true;
  }

  // Generic operation function to be used for timing a "core loop"
  template <class Index, bool fence, bool clear_cache>
  uint64_t DoCoreLoop(Index& index, std::vector<std::pair<KeyType, PayloadType>>& pairs_, 
                        std::function<void(Index&, bool&, std::vector<std::pair<KeyType, PayloadType>>&)> core_loop_func) {
    bool run_failed = false;

    if constexpr (clear_cache) std::cout << "rsum was: " << random_sum_ << std::endl;

    random_sum_ = 0;
    individual_ns_sum_ = 0;

    uint64_t ns = utils::timing([&] {
      core_loop_func(index, run_failed, pairs_);
    });

    if constexpr (clear_cache) {
      ns = individual_ns_sum_;
    }

    if (run_failed) {
      return std::numeric_limits<uint64_t>::max();
    }

    return ns;
  }

  template <class Index, bool fence, bool clear_cache>
  void DoEqualityLookupsCoreLoop(Index& index, bool& run_failed,
                                 std::vector<std::pair<KeyType, PayloadType>>& lookups_) {
    size_t qualifying;
    uint64_t result;

    for (unsigned int idx = 0; idx < lookups_.size(); ++idx) {
      // Compute the actual index for debugging.
      const volatile KeyType lookup_key = lookups_[idx].first;
      const volatile PayloadType expected = lookups_[idx].second;

      if constexpr (clear_cache) {
        // Make sure that all cache lines from large buffer are loaded
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        PayloadType lb;
        const auto timing = utils::timing([&] {
          lb = index.lower_bound(lookup_key);
        });

        if (!CheckResults(lb, expected, lookup_key)) {
          run_failed = true;
          return;
        }

        individual_ns_sum_ += timing;

      } else {
        // Not tracking errors, measure the lookup time.
        const auto lb = index.lower_bound(lookup_key);
        if (!CheckResults(lb, expected, lookup_key)) {
          run_failed = true;
          return;
        }
        volatile auto dummy = lb;
        (void)dummy;
      }

      if constexpr (fence) __sync_synchronize();
    }
  }

  template <class Index, bool fence, bool clear_cache>
  void DoInsertsCoreLoop(Index& index, bool& run_failed,
                         std::vector<std::pair<KeyType, PayloadType>>& inserts_) {
    for (unsigned int idx = 0; idx < inserts_.size(); ++idx) {
      const KeyType insert_key = inserts_[idx].first;
      const PayloadType insert_payload = inserts_[idx].second;

      if constexpr (clear_cache) {
        // Make sure that all cache lines from large buffer are loaded
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        const auto timing = utils::timing([&] {
          index.insert(insert_key, insert_payload);
        });

        individual_ns_sum_ += timing;

      } else {
        // Not tracking individual timing, just do the insert
        index.insert(insert_key, insert_payload);
      }

      if constexpr (fence) __sync_synchronize();
    }
  }

  template <class Index, bool fence, bool clear_cache>
  void DoDeletesCoreLoop(Index& index, bool& run_failed,
                         std::vector<std::pair<KeyType, PayloadType>>& deletes_) {
    for (unsigned int idx = 0; idx < deletes_.size(); ++idx) {
      const KeyType delete_key = deletes_[idx].first;

      if constexpr (clear_cache) {
        // Make sure that all cache lines from large buffer are loaded
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        const auto timing = utils::timing([&] {
          index.erase(delete_key);
        });

        individual_ns_sum_ += timing;

      } else {
        // Not tracking individual timing, just do the delete
        index.erase(delete_key);
      }

      if constexpr (fence) __sync_synchronize();
    }
  }

  template <class Index, bool fence, bool clear_cache>
  void DoMixedOperationsCoreLoop(Index& index, bool& run_failed,
                                 std::vector<std::pair<KeyType, PayloadType>>& mixed_pairs_) {
    // Each iteration performs one insert and one lookup, stepping by 2
    for (unsigned int i = 0; i + 1 < mixed_pairs_.size(); i += 2) {
      const KeyType insert_key = mixed_pairs_[i].first;
      const PayloadType insert_payload = mixed_pairs_[i].second;
      const KeyType lookup_key = mixed_pairs_[i + 1].first;
      // For lookups in mixed workload, we don't validate results since we're looking up 
      // keys that haven't been inserted yet - this is expected behavior

      if constexpr (clear_cache) {
        // Make sure that all cache lines from large buffer are loaded
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        const auto timing = utils::timing([&] {
          // Insert key i
          index.insert(insert_key, insert_payload);
          
          // Lookup key i+1
          PayloadType lb = index.lower_bound(lookup_key);
          // For mixed workload, we don't check the result of the lookup
          // as it potentially depends on keys that have just been inserted
          volatile auto dummy = lb;
          (void)dummy;
        });

        individual_ns_sum_ += timing;

      } else {
        // Insert key i
        index.insert(insert_key, insert_payload);
        
        // Lookup key i+1  
        PayloadType lb = index.lower_bound(lookup_key);
        // No checks, as above
        volatile auto dummy = lb;
        (void)dummy;
      }

      if constexpr (fence) __sync_synchronize();
    }
  }

  template <class Index, bool fence, bool clear_cache>
  void DoShiftingCoreLoop(Index& index, bool& run_failed,
                         std::vector<std::pair<KeyType, PayloadType>>& shifting_pairs_,
                         const std::vector<bool>& is_insert_op) {
    for (unsigned int idx = 0; idx < shifting_pairs_.size(); ++idx) {
      const KeyType key = shifting_pairs_[idx].first;
      const PayloadType payload = shifting_pairs_[idx].second;
      const bool is_insert = is_insert_op[idx];

      if constexpr (clear_cache) {
        // Cache clearing logic
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        const auto timing = utils::timing([&] {
          if (is_insert) {
            index.insert(key, payload);
          } else {
            index.erase(key);
          }
        });

        individual_ns_sum_ += timing;
      } else {
        if (is_insert) {
          index.insert(key, payload);
        } else {
          index.erase(key);
        }
      }

      if constexpr (fence) __sync_synchronize();
    }
  }

  template <class Index>
  uint64_t DoInsertInDistribution(Index& index, const bench_config& config) {
    auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> insert_keys = utils::get_non_existing_keys_in_distribution(keys.begin(), keys.end(), config.batch_size);

    if (insert_keys.size() < config.batch_size) {
      // existing keys are the ones in keys and in insert_keys
      std::vector<KeyType> existing_keys;
      existing_keys.reserve(keys.size() + insert_keys.size());
      existing_keys.insert(existing_keys.end(), keys.begin(), keys.end());
      existing_keys.insert(existing_keys.end(), insert_keys.begin(), insert_keys.end());

      std::vector<KeyType> extra_keys =
          utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - insert_keys.size());
      insert_keys.insert(insert_keys.end(), extra_keys.begin(), extra_keys.end());
    }
    
    std::vector<std::pair<KeyType, PayloadType>> insert_pairs;
    for (const auto& key : insert_keys) {
      insert_pairs.emplace_back(key, rand_gen());
    }

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, insert_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoInsertsCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, insert_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoInsertsCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

  template <class Index>
  uint64_t DoMixed(Index& index, const bench_config& config) {
    auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> mixed_keys = utils::get_non_existing_keys_in_distribution(keys.begin(), keys.end(), config.batch_size);

    if (mixed_keys.size() < config.batch_size) {
      // existing keys are the ones in keys and in mixed_keys
      std::vector<KeyType> existing_keys;
      existing_keys.reserve(keys.size() + mixed_keys.size());
      existing_keys.insert(existing_keys.end(), keys.begin(), keys.end());
      existing_keys.insert(existing_keys.end(), mixed_keys.begin(), mixed_keys.end());

      std::vector<KeyType> extra_keys =
          utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - mixed_keys.size());
      mixed_keys.insert(mixed_keys.end(), extra_keys.begin(), extra_keys.end());
    }
    
    // Create pairs with random payloads for mixed operations
    std::vector<std::pair<KeyType, PayloadType>> mixed_pairs;
    for (const auto& key : mixed_keys) {
      mixed_pairs.emplace_back(key, rand_gen());
    }

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, mixed_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoMixedOperationsCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, mixed_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoMixedOperationsCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

  template <class Index>
  uint64_t DoShifting(Index& index, const bench_config& config) {
    InitializeShiftingWindow();
    
    // Generate batch operations: alternating insert/delete
    std::vector<std::pair<KeyType, PayloadType>> shifting_pairs;
    std::vector<bool> is_insert_op;  // true = insert, false = delete
    
    for (size_t i = 0; i < config.batch_size; ++i) {
      if (i % 2 == 0) {  // Insert operation
        KeyType new_key = next_insert_key_++;
        PayloadType payload = rand_gen();
        shifting_pairs.emplace_back(new_key, payload);
        current_keys_.push(new_key);  // Track for future deletions
        is_insert_op.push_back(true);
      } else {  // Delete operation
        if (!current_keys_.empty()) {
          KeyType key_to_delete = current_keys_.top();
          current_keys_.pop();
          shifting_pairs.emplace_back(key_to_delete, PayloadType{});
          is_insert_op.push_back(false);
        } else {
          // No keys to delete, make it an insert instead
          KeyType new_key = next_insert_key_++;
          PayloadType payload = rand_gen();
          shifting_pairs.emplace_back(new_key, payload);
          current_keys_.push(new_key);
          is_insert_op.push_back(true);
        }
      }
    }

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, shifting_pairs, 
        [this, &is_insert_op](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoShiftingCoreLoop<Index, false, true>(idx, failed, pairs, is_insert_op);
        });
    else
      return DoCoreLoop<Index, false, false>(index, shifting_pairs, 
        [this, &is_insert_op](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoShiftingCoreLoop<Index, false, false>(idx, failed, pairs, is_insert_op);
        });
  }

  std::vector<std::pair<KeyType, PayloadType>>& key_values_;
  uint64_t random_sum_ = 0;
  uint64_t individual_ns_sum_ = 0;
  std::vector<uint64_t> memory_;  // Some memory used to flush the cache
  
  // Shifting window state
  KeyType next_insert_key_{};  // Next key to insert (always increasing)
  std::priority_queue<KeyType, std::vector<KeyType>, std::greater<KeyType>> current_keys_;  // Min-heap for deletions
  bool shifting_initialized_ = false;
};


template<class IndexWrapper>
void run_benchmark(const bench_config& config, 
                   std::vector<std::pair<typename IndexWrapper::KeyType, typename IndexWrapper::PayloadType>>& key_values,
                   Workload workload) {
  
    using KeyType = typename IndexWrapper::KeyType;
    using PayloadType = typename IndexWrapper::PayloadType;

    // Create the benchmark instance
    deli_testbed::Benchmark<KeyType, PayloadType> benchmark(key_values);
    
    // Create the index and bulk load initial keys
    IndexWrapper index;

    if (!index.applicable(config.data_filename)) {
      return;
    }

    benchmark.BulkLoad(index);

    // Run workload
    auto batch_start_time = std::chrono::high_resolution_clock::now();

    for (int batch_no = 0; batch_no < config.max_batches; ++batch_no) {
      uint64_t batch_time;
      switch (workload) {
        case LOOKUP_EXISTING: 
          batch_time = benchmark.template RunWorkload<LOOKUP_EXISTING, IndexWrapper>(index, config); 
          break;
        case LOOKUP_IN_DISTRIBUTION: 
          batch_time = benchmark.template RunWorkload<LOOKUP_IN_DISTRIBUTION, IndexWrapper>(index, config); 
          break;
        case INSERT_IN_DISTRIBUTION: 
          batch_time = benchmark.template RunWorkload<INSERT_IN_DISTRIBUTION, IndexWrapper>(index, config); 
          break;
        case DELETE_EXISTING: 
          batch_time = benchmark.template RunWorkload<DELETE_EXISTING, IndexWrapper>(index, config); 
          break;
        case MIXED: 
          batch_time = benchmark.template RunWorkload<MIXED, IndexWrapper>(index, config); 
          break;
        case SHIFTING: 
          batch_time = benchmark.template RunWorkload<SHIFTING, IndexWrapper>(index, config); 
          break;
        default: 
          throw std::runtime_error("Workload not implemented");
      }

        if (config.print_batch_stats) {
          std::cout << std::scientific << std::setprecision(3);
          std::cout << index.name() << " " << workload_name(workload) << ": " << config.batch_size << " operations completed\n";
          std::cout << "Total time: " << batch_time / 1e9 << " seconds\n";
          double batch_overall_throughput = (batch_time > 0) ? (1. * config.batch_size / batch_time * 1e9) : 0.0;
          std::cout << "Throughput: " << batch_overall_throughput << " ops/sec\n";
        }

        // Use the benchmark's print method for consistent formatting
        benchmark.PrintResult(IndexWrapper::name(), IndexWrapper::variant(), workload_name(workload), batch_no, key_values.size(), 
                              config.batch_size, config.lookup_distribution, batch_time, config.out_file);

        // Check time limit
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - batch_start_time)
                .count();
        if (workload_elapsed_time > config.time_limit * 1e9 * 60) {
            break;
        }
    }
}

}  // namespace deli_testbed
