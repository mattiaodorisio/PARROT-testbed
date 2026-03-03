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

#include "utils.h"

namespace deli_testbed {
enum Workload : int {
  LOOKUP_EXISTING,
  LOOKUP_IN_DISTRIBUTION,
  INSERT_IN_DISTRIBUTION,
  MIXED,
  
  NUM_WORKLOADS,  // TODO: to be implemented
  SHIFTING,
};

std::string workload_name(Workload workload) {
  switch (workload) {
    case LOOKUP_EXISTING: return "LOOKUP_EXISTING";
    case LOOKUP_IN_DISTRIBUTION: return "LOOKUP_IN_DISTRIBUTION";
    case INSERT_IN_DISTRIBUTION: return "INSERT_IN_DISTRIBUTION";
    case MIXED: return "MIXED";
    case SHIFTING: return "SHIFTING";
    default: return "UNKNOWN_WORKLOAD";
  }
}

template <typename KeyType, typename PayloadType>
class Benchmark {
 public:
  Benchmark(std::vector<std::pair<KeyType, PayloadType>>& key_values)
      : key_values_(key_values) {}

  template <class Index>
  void BulkLoad(Index& index) {
    // Sort the data before bulk loading
    std::sort(key_values_.begin(), key_values_.end(), 
              [](auto const& a, auto const& b) { return a.first < b.first; });
    index.bulk_load(key_values_.data(), key_values_.size());
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
    } else if constexpr (W == MIXED) {
      return DoMixed(index, config);
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
      return DoEqualityLookups<Index, false, true>(index, lookup_pairs);
    else
      return DoEqualityLookups<Index, false, false>(index, lookup_pairs);
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
      return DoEqualityLookups<Index, true, true>(index, lookup_pairs);
    else
      return DoEqualityLookups<Index, false, false>(index, lookup_pairs);
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

  template <class Index, bool fence, bool clear_cache>
  uint64_t DoEqualityLookups(Index& index, std::vector<std::pair<KeyType, PayloadType>>& lookups_) {
    bool run_failed = false;

    if constexpr (clear_cache) std::cout << "rsum was: " << random_sum_ << std::endl;

    random_sum_ = 0;
    individual_ns_sum_ = 0;

    uint64_t ns = utils::timing([&] {
      DoEqualityLookupsCoreLoop<Index, fence, clear_cache>(
          index, run_failed, lookups_);
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
  uint64_t DoInserts(Index& index, std::vector<std::pair<KeyType, PayloadType>>& inserts_) {
    bool run_failed = false;

    if constexpr (clear_cache) std::cout << "rsum was: " << random_sum_ << std::endl;

    random_sum_ = 0;
    individual_ns_sum_ = 0;

    uint64_t ns = utils::timing([&] {
      DoInsertsCoreLoop<Index, fence, clear_cache>(
          index, run_failed, inserts_);
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
      return DoInserts<Index, false, true>(index, insert_pairs);
    else
      return DoInserts<Index, false, false>(index, insert_pairs);
  }

  template <class Index>
  uint64_t DoMixed(Index& index, const bench_config& config) {
    // Placeholder for mixed workload - to be implemented
    throw std::runtime_error("Mixed workload not yet implemented");
  }

  std::vector<std::pair<KeyType, PayloadType>>& key_values_;
  uint64_t random_sum_ = 0;
  uint64_t individual_ns_sum_ = 0;
  std::vector<uint64_t> memory_;  // Some memory used to flush the cache
};


template<class IndexWrapper>
void run_benchmark(const bench_config& config, 
                   std::vector<std::pair<typename IndexWrapper::KeyType, typename IndexWrapper::PayloadType>> key_values,
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
        case MIXED: 
          batch_time = benchmark.template RunWorkload<MIXED, IndexWrapper>(index, config); 
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
