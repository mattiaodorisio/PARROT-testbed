#pragma once

#include "DILI/src/dili/DILI.h"
#undef PAYLOAD_TYPE
#undef KEY_TYPE
#include "../src/benchmark.h"
#include <filesystem>

namespace deli_testbed {

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDILI {
  public:
    using KeyType     = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

    BenchmarkDILI() {}

    template <typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      std::vector<std::pair<long, long>> data;
      data.reserve(std::distance(begin, end));
      for (auto it = begin; it != end; ++it)
        data.emplace_back(static_cast<long>(it->first),
                          static_cast<long>(it->second));
      // mirror_dir_ is set in applicable(); DILI uses it to cache the
      // BU-tree layout so the expensive computation runs only once per dataset.
      index.set_mirror_dir(mirror_dir_);
      index.bulk_load(data);
    }

    PayloadType lower_bound(const KeyType key) {
      long result = index.search(static_cast<long>(key));
      return result == -1 ? PayloadType{} : static_cast<PayloadType>(result);
    }

    void insert(const KeyType& key, const PayloadType& payload) {
      index.insert(static_cast<long>(key), static_cast<long>(payload));
    }

    void erase(const KeyType& key) {
      index.erase(static_cast<long>(key));
    }

    static std::string name()    { return "DILI"; }
    static std::string variant() { return "none"; }

    bool applicable(const std::string& data_filename) {
      // DILI's internal keyType is `long` (int64_t).
      // uint64_t may exceed INT64_MAX; skip those datasets for safety.
      if (!std::is_same_v<KeyType, uint32_t>) return false;
      // Derive a dataset-specific mirror directory so DILI can cache the
      // expensive BU-tree layout computation across benchmark runs.
      mirror_dir_ = "/tmp/dili_mirror_" +
                    std::filesystem::path(data_filename).stem().string();
      return true;
    }

  private:
    DILI index;
    std::string mirror_dir_;
};

template <typename KeyType, typename PayloadType>
void benchmark_dili(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_values) {
  // LOOKUP_IN_DISTRIBUTION is excluded: DILI is exact-match only;
  constexpr Workload supported_workloads[] = {
      LOOKUP_EXISTING, INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED, SHIFTING};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkDILI<KeyType, PayloadType>>(
        config, key_values, wl, shifting_key_values);
  }
}

}  // namespace deli_testbed
