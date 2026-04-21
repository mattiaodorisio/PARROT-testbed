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
#include <cmath>
#include <memory>

#include "utils.h"

namespace deli_testbed {

/// How a wrapper is used in the benchmark.
enum class SearchMode {
  PREDECESSOR_SEARCH,  // find predecessor/successor in the original sorted array
  KEY_VALUE,           // true key-value store (payload detached from key order)
};

/// What a wrapper's lower_bound() semantically returns.
enum class SearchSemantics {
  SUCCESSOR,    // smallest key >= query
  PREDECESSOR,  // largest  key <= query (predecessor - used by SEA21)
};

// Empty base (used for conditional inheritance)
struct EmptyBase {};

// PredecessorSearchBase
template <typename KeyType>
class PredecessorSearchBase {
protected:
  std::vector<KeyType> sorted_keys_;

  /// Populate sorted_keys_ from an iterator range of (key, payload) pairs.
  /// Assumes the range is already sorted by key (same invariant as bulk_load).
  template <typename Iterator>
  void ps_init(Iterator begin, Iterator end) {
    sorted_keys_.clear();
    sorted_keys_.reserve(static_cast<size_t>(std::distance(begin, end)));
    for (auto it = begin; it != end; ++it)
      sorted_keys_.push_back(it->first);
  }

  /// Return the smallest key in sorted_keys_[lo, hi) that is >= query,
  /// or KeyType{} if no such key exists.
  KeyType find_successor_in_range(KeyType query, size_t lo, size_t hi) const {
    hi = std::min(hi, sorted_keys_.size());
    auto it = std::lower_bound(sorted_keys_.begin() + lo,
                               sorted_keys_.begin() + hi, query);
    return (it != sorted_keys_.begin() + hi) ? *it : KeyType{};
  }
};

// ── Workload enum ─────────────────────────────────────────────────────────────
enum Workload : int {
  LOOKUP_EXISTING,
  LOOKUP_IN_DISTRIBUTION,
  LOOKUP_UNIFORM,
  INSERT_IN_DISTRIBUTION,
  DELETE_EXISTING,
  MIXED,
  SHIFTING,
  INSERT_DELETE,
};

std::string workload_name(Workload workload) {
  switch (workload) {
    case LOOKUP_EXISTING: return "LOOKUP_EXISTING";
    case LOOKUP_IN_DISTRIBUTION: return "LOOKUP_IN_DISTRIBUTION";
    case LOOKUP_UNIFORM: return "LOOKUP_UNIFORM";
    case INSERT_IN_DISTRIBUTION: return "INSERT_IN_DISTRIBUTION";
    case DELETE_EXISTING: return "DELETE_EXISTING";
    case MIXED: return "MIXED";
    case SHIFTING: return "SHIFTING";
    case INSERT_DELETE: return "INSERT_DELETE";
    default: return "UNKNOWN_WORKLOAD";
  }
}

template <typename KeyType, typename PayloadType,
          SearchSemantics semantics = SearchSemantics::SUCCESSOR>
class Benchmark {
 public:
  Benchmark(const std::vector<std::pair<KeyType, PayloadType>>& shifting_keys) : 
        shifting_insert_key_values_(shifting_keys) {
    // Initialize cache clearing buffer - allocate ~64MB for cache flushing
    constexpr size_t cache_flush_size = 64 * 1024 * 1024 / sizeof(uint64_t);
    memory_.resize(cache_flush_size);
    // Initialize with some values to avoid potential issues
    std::iota(memory_.begin(), memory_.end(), 0);
  }

  void init(const std::vector<std::pair<KeyType, PayloadType>>& key_values, size_t shifting_cursor = std::numeric_limits<size_t>::max()) {
    shifting_cursor = std::min(shifting_cursor, key_values.size());
    std::copy(key_values.begin(), key_values.begin() + shifting_cursor, std::back_inserter(key_values_));
    shifting_insert_cursor_ = shifting_cursor;
  }

  void PrintResult(const std::string& index_name,
                   const std::string& index_variant,
                   const std::string& workload_name,
                   int batch_no, size_t init_num_keys, size_t batch_operations,
                   double batch_time,
                   std::ofstream& out_file,
                   size_t convergence_samples,
                   double convergence_mean_throughput,
                   double convergence_rse,
                   bool adaptive_stop_triggered) {
    
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
            << std::fixed << std::setprecision(2) << "throughput=" << (!error ? std::to_string(batch_overall_throughput) : "error") << " "
            << std::fixed << std::setprecision(6) << "total_time=" << (!error ? std::to_string(1.0 * batch_time / 1e9) : "error") << " "
            << "conv_samples=" << convergence_samples << " "
            << std::fixed << std::setprecision(2) << "conv_mean_throughput=" << (convergence_samples > 0 ? std::to_string(convergence_mean_throughput) : "na") << " "
            << std::fixed << std::setprecision(6) << "conv_rse=" << (std::isfinite(convergence_rse) ? std::to_string(convergence_rse) : "na") << " "
            << "adaptive_stop=" << (adaptive_stop_triggered ? "1" : "0")
            << std::endl;
  }

  template <Workload W, class Index>
  uint64_t RunWorkload(Index& index, const bench_config& config) {
    if constexpr (W == LOOKUP_EXISTING) {
      return DoLookupExisting(index, config);
    } else if constexpr (W == LOOKUP_IN_DISTRIBUTION) {
      return DoLookupInDistribution(index, config);
    } else if constexpr (W == LOOKUP_UNIFORM) {
      return DoLookupUniform(index, config);
    } else if constexpr (W == INSERT_IN_DISTRIBUTION) {
      if constexpr (requires(Index& i, typename Index::KeyType k, typename Index::PayloadType p) {i.insert(k, p); }) {
        return DoInsertInDistribution(index, config);
      } else {
        throw std::runtime_error("INSERT_IN_DISTRIBUTION not supported by this index");
      }
    } else if constexpr (W == DELETE_EXISTING) {
      if constexpr (requires(Index& i, typename Index::KeyType k) { i.erase(k); }) {
        return DoDeleteExisting(index, config);
      } else {
        throw std::runtime_error("DELETE_EXISTING not supported by this index");
      }
    } else if constexpr (W == MIXED) {
      if constexpr (requires(Index& i, typename Index::KeyType k, typename Index::PayloadType p) {
                      i.insert(k, p); }) {
        return DoMixed(index, config);
      } else {
        throw std::runtime_error("MIXED not supported by this index");
      }
    } else if constexpr (W == SHIFTING) {
      if constexpr (requires(Index& i, typename Index::KeyType k, typename Index::PayloadType p) {
                      i.insert(k, p); }
                   && requires(Index& i, typename Index::KeyType k) { i.erase(k); }) {
        return DoShifting(index, config);
      } else {
        throw std::runtime_error("SHIFTING not supported by this index");
      }
    } else if constexpr (W == INSERT_DELETE) {
      if constexpr (requires(Index& i, typename Index::KeyType k, typename Index::PayloadType p) { i.insert(k, p); }
                   && requires(Index& i, typename Index::KeyType k) { i.erase(k); }) {
        return DoInsertDelete(index, config);
      } else {
        throw std::runtime_error("INSERT_DELETE not supported by this index");
      }
    } else {
      throw std::runtime_error("Workload not implemented");
    }
    return 0;
  }

  size_t current_num_keys_() const {
    return key_values_.size();
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
        // utils::get_non_existing_keys expects a sorted array
        std::sort(existing_keys.begin(), existing_keys.end());

        std::vector<KeyType> extra_keys =
            utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - lookup_keys.size());
        lookup_keys.insert(lookup_keys.end(), extra_keys.begin(), extra_keys.end());
      }

      lookup_pairs.reserve(lookup_keys.size());
      for (const auto& key : lookup_keys) {
        PayloadType expected_payload;
        if constexpr (semantics == SearchSemantics::PREDECESSOR) {
          // Predecessor: largest key <= query
          auto it = std::upper_bound(key_values_.begin(), key_values_.end(), key,
            [](const KeyType& k, const auto& pair) { return k < pair.first; });
          expected_payload = (it != key_values_.begin()) ? std::prev(it)->second : PayloadType{};
        } else {
          // Successor / lower_bound: smallest key >= query
          auto it = std::lower_bound(key_values_.begin(), key_values_.end(), key,
            [](const auto& pair, const KeyType& k) { return pair.first < k; });
          expected_payload = (it != key_values_.end()) ? it->second : PayloadType{};
        }
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
  uint64_t DoLookupUniform(Index& index, const bench_config& config) {
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs;

    {
      auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
      std::vector<KeyType> lookup_keys = utils::get_non_existing_keys(keys.begin(), keys.end(), config.batch_size);

      lookup_pairs.reserve(lookup_keys.size());
      for (const auto& key : lookup_keys) {
        PayloadType expected_payload;
        if constexpr (semantics == SearchSemantics::PREDECESSOR) {
          auto it = std::upper_bound(key_values_.begin(), key_values_.end(), key,
            [](const KeyType& k, const auto& pair) { return k < pair.first; });
          expected_payload = (it != key_values_.begin()) ? std::prev(it)->second : PayloadType{};
        } else {
          auto it = std::lower_bound(key_values_.begin(), key_values_.end(), key,
            [](const auto& pair, const KeyType& k) { return pair.first < k; });
          expected_payload = (it != key_values_.end()) ? it->second : PayloadType{};
        }
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
        utils::get_existing_keys(key_values_.begin(), key_values_.end(), config.batch_size, false);

    uint64_t result;
    if (config.clear_cache)
      result = DoCoreLoop<Index, false, true>(index, delete_pairs,
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoDeletesCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      result = DoCoreLoop<Index, false, false>(index, delete_pairs,
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoDeletesCoreLoop<Index, false, false>(idx, failed, pairs);
        });

    // Remove deleted keys from key_values_ so that subsequent batches do not
    // attempt to re-delete the same keys (symmetric to DoInsertInDistribution).
    std::sort(delete_pairs.begin(), delete_pairs.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::vector<std::pair<KeyType, PayloadType>> remaining;
    remaining.reserve(key_values_.size());
    std::set_difference(key_values_.begin(), key_values_.end(),
                        delete_pairs.begin(), delete_pairs.end(),
                        std::back_inserter(remaining),
                        [](const auto& a, const auto& b) { return a.first < b.first; });
    key_values_ = std::move(remaining);

    return result;
  }

private:
  bool CheckResults(PayloadType actual, PayloadType expected, KeyType lookup_key) {
    if (actual != expected) {
      std::cerr << "Equality lookup returned wrong result:\n" 
                << "Lookup key: " << lookup_key << "\n"
                << "Actual: " << actual << ", expected: " << expected
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
                         std::vector<std::pair<KeyType, PayloadType>>& shifting_pairs_) {
    for (unsigned int idx = 0; idx < shifting_pairs_.size(); ++idx) {
      const KeyType key = shifting_pairs_[idx].first;
      const PayloadType payload = shifting_pairs_[idx].second;
      const int op = idx % 3;  // 0=insert, 1=lookup, 2=delete

      if constexpr (clear_cache) {
        for (uint64_t& iter : memory_) {
          random_sum_ += iter;
        }
        _mm_mfence();

        const auto timing = utils::timing([&] {
          if (op == 0) {
            index.insert(key, payload);
          } else if (op == 1) {
            volatile auto dummy = index.lower_bound(key);
            (void)dummy;
          } else {
            index.erase(key);
          }
        });

        individual_ns_sum_ += timing;
      } else {
        if (op == 0) {
          index.insert(key, payload);
        } else if (op == 1) {
          volatile auto dummy = index.lower_bound(key);
          (void)dummy;
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
      // utils::get_non_existing_keys expects a sorted array
      std::sort(existing_keys.begin(), existing_keys.end());

      std::vector<KeyType> extra_keys =
          utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - insert_keys.size());
      insert_keys.insert(insert_keys.end(), extra_keys.begin(), extra_keys.end());
    }
    
    std::vector<std::pair<KeyType, PayloadType>> insert_pairs;
    for (const auto& key : insert_keys) {
      insert_pairs.emplace_back(key, rand_gen());
    }

    // Add insert keys to the dataset for future batches, as they are now part of the index
    std::sort(insert_pairs.begin(), insert_pairs.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    key_values_.insert(key_values_.end(), insert_pairs.begin(), insert_pairs.end());
    std::inplace_merge(key_values_.begin(), key_values_.end() - insert_pairs.size(), key_values_.end(), [](auto const& a, auto const& b) { return a.first < b.first; });

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
      // utils::get_non_existing_keys expects a sorted array
      std::sort(existing_keys.begin(), existing_keys.end());

      std::vector<KeyType> extra_keys =
          utils::get_non_existing_keys(existing_keys.begin(), existing_keys.end(), config.batch_size - mixed_keys.size());
      mixed_keys.insert(mixed_keys.end(), extra_keys.begin(), extra_keys.end());
    }
    
    // Create pairs with random payloads for mixed operations
    std::vector<std::pair<KeyType, PayloadType>> mixed_pairs;
    for (const auto& key : mixed_keys) {
      mixed_pairs.emplace_back(key, rand_gen());
    }

      // Add insert keys to the dataset for future batches, as they are now part of the index
    std::vector<std::pair<KeyType, PayloadType>> insert_pairs;
    for (size_t i = 0; i < mixed_pairs.size(); i += 2) {
      insert_pairs.push_back(mixed_pairs[i]);  // Even indices are inserts
    }
    std::sort(insert_pairs.begin(), insert_pairs.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    key_values_.insert(key_values_.end(), insert_pairs.begin(), insert_pairs.end());
    std::inplace_merge(key_values_.begin(), key_values_.end() - insert_pairs.size(), key_values_.end(), [](auto const& a, auto const& b) { return a.first < b.first; });

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
    assert(!shifting_insert_key_values_.empty());
    assert(std::is_sorted(key_values_.begin(), key_values_.end(), [](auto const& a, auto const& b) { return a.first < b.first; }));
    const size_t n_inserts = (config.batch_size + 2) / 3;
    if (shifting_insert_cursor_ + n_inserts > shifting_insert_key_values_.size()) {
      // Not enough keys to shift
      return std::numeric_limits<uint64_t>::max();
    } 
    
    // Pre-generate non-existing in-distribution keys for the lookup slots.
    const size_t n_lookups = (config.batch_size + 1) / 3;
    auto keys_view = key_values_ | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> lookup_keys = utils::get_non_existing_keys_in_distribution(
        keys_view.begin(), keys_view.end(), static_cast<int>(n_lookups));
    if (lookup_keys.size() < n_lookups) {
      std::vector<KeyType> existing_keys(keys_view.begin(), keys_view.end());
      existing_keys.insert(existing_keys.end(), lookup_keys.begin(), lookup_keys.end());
      // utils::get_non_existing_keys expects a sorted array
      std::sort(existing_keys.begin(), existing_keys.end());
      auto extra = utils::get_non_existing_keys(
          existing_keys.begin(), existing_keys.end(),
          static_cast<int>(n_lookups - lookup_keys.size()));
      lookup_keys.insert(lookup_keys.end(), extra.begin(), extra.end());
    }

    // Generate batch operations: triplets of (insert, lookup, delete)
    std::vector<std::pair<KeyType, PayloadType>> shifting_pairs;
    shifting_pairs.reserve(config.batch_size);
    size_t delete_index = 0;
    size_t lookup_cursor = 0;
    const size_t init_keys_size = key_values_.size();

    for (size_t i = 0; i < config.batch_size; ++i) {
      if (i % 3 == 0) {  // Insert
        const auto& [new_key, payload] = shifting_insert_key_values_[shifting_insert_cursor_++];
        shifting_pairs.emplace_back(new_key, payload);
        key_values_.push_back({new_key, payload});
      } else if (i % 3 == 1) {  // Lookup (non-existing, in-distribution)
        shifting_pairs.emplace_back(lookup_keys[lookup_cursor++], PayloadType{});
      } else {  // Delete
        shifting_pairs.emplace_back(key_values_[delete_index].first, key_values_[delete_index].second);
        ++delete_index;
      }
    }

    // Update key_values_ to the current keys
    if (delete_index < init_keys_size) {
      // Sort newly inserted keys (same as sorting key_values_, but more efficient)
      std::sort(key_values_.begin() + init_keys_size, key_values_.end());
      std::inplace_merge(key_values_.begin() + delete_index, key_values_.begin() + init_keys_size, key_values_.end());
      key_values_.erase(key_values_.begin(), key_values_.begin() + delete_index);
      assert(std::is_sorted(key_values_.begin(), key_values_.end()));
    } else {
      key_values_.erase(key_values_.begin(), key_values_.begin() + delete_index);
      std::sort(key_values_.begin(), key_values_.end());
    }

    if (config.clear_cache)
      return DoCoreLoop<Index, false, true>(index, shifting_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoShiftingCoreLoop<Index, false, true>(idx, failed, pairs);
        });
    else
      return DoCoreLoop<Index, false, false>(index, shifting_pairs, 
        [this](Index& idx, bool& failed, std::vector<std::pair<KeyType, PayloadType>>& pairs) {
          DoShiftingCoreLoop<Index, false, false>(idx, failed, pairs);
        });
  }

  template <class Index>
  uint64_t DoInsertDelete(Index& index, const bench_config& config) {
    // Insert all keys in key_values_ order, then delete in the same order.
    // key_values_ holds the original file-order pairs set by run_benchmark before the batch loop.
    const size_t n = key_values_.size();

    if (config.clear_cache) {
      individual_ns_sum_ = 0;
      for (size_t i = 0; i < n; ++i) {
        for (uint64_t& v : memory_) random_sum_ += v;
        _mm_mfence();
        individual_ns_sum_ += utils::timing([&] {
          index.insert(key_values_[i].first, key_values_[i].second);
        });
      }
      for (size_t i = 0; i < n; ++i) {
        for (uint64_t& v : memory_) random_sum_ += v;
        _mm_mfence();
        individual_ns_sum_ += utils::timing([&] {
          index.erase(key_values_[i].first);
        });
      }
      return individual_ns_sum_;
    } else {
      return utils::timing([&] {
        for (size_t i = 0; i < n; ++i)
          index.insert(key_values_[i].first, key_values_[i].second);
        for (size_t i = 0; i < n; ++i)
          index.erase(key_values_[i].first);
      });
    }
  }

  std::vector<std::pair<KeyType, PayloadType>> key_values_;
  const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values_;
  uint64_t random_sum_ = 0;
  uint64_t individual_ns_sum_ = 0;
  std::vector<uint64_t> memory_;  // Some memory used to flush the cache
  size_t shifting_insert_cursor_ = 0; // Shifting window state
};


template<class IndexWrapper>
void run_benchmark(const bench_config& config, 
                   std::vector<std::pair<typename IndexWrapper::KeyType, typename IndexWrapper::PayloadType>>& key_values,
                   Workload workload,
                   const std::vector<std::pair<typename IndexWrapper::KeyType, typename IndexWrapper::PayloadType>>& shifting_insert_key_values = {}) {
  
    using KeyType = typename IndexWrapper::KeyType;
    using PayloadType = typename IndexWrapper::PayloadType;
    constexpr SearchSemantics semantics = IndexWrapper::search_semantics;

    // Check that there are enough keys for the delete workload
    if (workload == Workload::DELETE_EXISTING && key_values.size() < static_cast<size_t>(config.batch_size * 2)) {
      return;
    }

    if (workload == Workload::INSERT_IN_DISTRIBUTION || workload == Workload::DELETE_EXISTING) {
      return;
    }

    // For INSERT_DELETE each batch performs N inserts + N deletes.
    const size_t batch_ops = (workload == Workload::INSERT_DELETE)
        ? 2 * key_values.size()
        : static_cast<size_t>(config.batch_size);

    // Create the index and bulk load initial keys
    IndexWrapper index;

    if (!index.applicable(config.data_filename)) {
      return;
    }

    // Create the benchmark instance
    deli_testbed::Benchmark<KeyType, PayloadType, semantics> benchmark(shifting_insert_key_values);

    if (workload == Workload::SHIFTING) {
      benchmark.init(shifting_insert_key_values, key_values.size());
      index.bulk_load(shifting_insert_key_values.begin(), shifting_insert_key_values.begin() + key_values.size());
    } else if (workload == Workload::INSERT_DELETE) {
      // Store keys in their supplied (original file) order; index starts empty — no bulk load.
      benchmark.init(key_values);
    } else {
      benchmark.init(key_values);
      index.bulk_load(key_values.begin(), key_values.end());
    }

    // Run workload
    auto batch_start_time = std::chrono::high_resolution_clock::now();
    std::vector<double> measured_throughputs;
    measured_throughputs.reserve(static_cast<size_t>(config.max_batches));

    // First run is treated as warm-up and discarded from reporting/statistics.
    bool break_loop = false;
    for (int run_no = 0; run_no < config.max_batches + 1 && !break_loop; ++run_no) {
      uint64_t batch_time;
      switch (workload) {
        case LOOKUP_EXISTING: 
          batch_time = benchmark.template RunWorkload<LOOKUP_EXISTING, IndexWrapper>(index, config); 
          break;
        case LOOKUP_IN_DISTRIBUTION:
          batch_time = benchmark.template RunWorkload<LOOKUP_IN_DISTRIBUTION, IndexWrapper>(index, config);
          break;
        case LOOKUP_UNIFORM:
          batch_time = benchmark.template RunWorkload<LOOKUP_UNIFORM, IndexWrapper>(index, config);
          break;
        case INSERT_IN_DISTRIBUTION:
          batch_time = benchmark.template RunWorkload<INSERT_IN_DISTRIBUTION, IndexWrapper>(index, config); 
          break;
        case DELETE_EXISTING:
        {
          if (benchmark.current_num_keys_() < static_cast<size_t>(config.batch_size)) {
            break_loop = true;
            continue;
          }
          batch_time = benchmark.template RunWorkload<DELETE_EXISTING, IndexWrapper>(index, config); 
          break;
        }
        case MIXED: 
          batch_time = benchmark.template RunWorkload<MIXED, IndexWrapper>(index, config); 
          break;
        case SHIFTING:
          batch_time = benchmark.template RunWorkload<SHIFTING, IndexWrapper>(index, config);
          break;
        case INSERT_DELETE:
        {
          // Destroy and reconstruct the index in-place so each batch starts
          // from a clean empty state without requiring copy/move.
          std::destroy_at(&index);
          std::construct_at(&index);
          batch_time = benchmark.template RunWorkload<INSERT_DELETE, IndexWrapper>(index, config);
          break;
        }
        default:
          throw std::runtime_error("Workload not implemented");
      }

        const bool is_warmup_run = (run_no == 0);
        if (is_warmup_run) {
          continue;
        }

        const int measured_batch_no = run_no - 1;
        const bool run_error = (batch_time == std::numeric_limits<uint64_t>::max());
        if (!run_error && batch_time > 0) {
          measured_throughputs.push_back(1. * batch_ops / batch_time * 1e9);
        }

        double running_mean = 0.0;
        double running_rse = std::numeric_limits<double>::quiet_NaN();
        bool adaptive_stop_triggered = false;

        if (!measured_throughputs.empty()) {
          const double sum = std::accumulate(measured_throughputs.begin(), measured_throughputs.end(), 0.0);
          const double n = static_cast<double>(measured_throughputs.size());
          running_mean = sum / n;

          if (measured_throughputs.size() >= 2 && running_mean > 0.0) {
            double sq_sum = 0.0;
            for (const double x : measured_throughputs) {
              const double d = x - running_mean;
              sq_sum += d * d;
            }
            const double sample_stddev = std::sqrt(sq_sum / (n - 1.0));
            const double standard_error = sample_stddev / std::sqrt(n);
            running_rse = standard_error / running_mean;
          }
        }

        if (measured_batch_no + 1 >= config.min_batches && std::isfinite(running_rse) &&
            running_rse <= config.rse_target) {
          adaptive_stop_triggered = true;
        }

        if (config.print_batch_stats) {
          std::cout << std::scientific << std::setprecision(3);
          std::cout << index.name() << " " << workload_name(workload) << ": " << batch_ops << " operations completed\n";
          std::cout << "Total time: " << batch_time / 1e9 << " seconds\n";
          double batch_overall_throughput = (batch_time > 0) ? (1. * batch_ops / batch_time * 1e9) : 0.0;
          std::cout << "Throughput: " << batch_overall_throughput << " ops/sec\n";
          std::cout << "Convergence: n=" << measured_throughputs.size()
                    << ", mean=" << running_mean
                    << ", rse=" << (std::isfinite(running_rse) ? running_rse : -1.0)
                    << "\n";
        }

        // Use the benchmark's print method for consistent formatting
        benchmark.PrintResult(IndexWrapper::name(), IndexWrapper::variant(), workload_name(workload), measured_batch_no, key_values.size(),
                              batch_ops, batch_time, config.out_file,
                              measured_throughputs.size(), running_mean, running_rse, adaptive_stop_triggered);

        // Adaptive stop based on relative standard error (RSE) of throughput.
        if (adaptive_stop_triggered) {
          break;
        }

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
