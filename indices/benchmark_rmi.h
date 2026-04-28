#pragma once

#include "../src/benchmark.h"
#include "../src/utils.h"

// RMI benchmarks are compiled only when COMPILE_RMI is defined (see src/utils.h).
// Comment out `#define COMPILE_RMI` in utils.h to exclude all RMI code from
// the build (useful when no models have been generated yet, or to reduce
// compile time).
#ifdef COMPILE_RMI

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "rmi/all_rmis.h"

// ── RMI integration ───────────────────────────────────────────────────────────
//
// RMI (Recursive Model Index) is a static, dataset-specific learned index.
// Models are pre-generated offline with the Rust compiler in indices/RMI/ and
// stored as C++ source files in indices/rmi/.  See indices/rmi/all_rmis.h for
// the workflow to add new datasets.
//
// At benchmark time, each model loads its trained parameters from disk
// (directory given by $DELI_RMI_PATH, default: "rmi_data/").  lookup() returns
// an approximate position and an error bound; a standard binary search is then
// performed on the sorted key array within [pos−err, pos+err].
//
// Because models are trained on the full dataset, the search bound is silently
// widened to [0, n) when the current run uses fewer keys than the training set.
//
// ─────────────────────────────────────────────────────────────────────────────

namespace deli_testbed {

template <typename KEY_TYPE, typename PAYLOAD_TYPE, int rmi_variant,
          uint64_t (*RMI_LOOKUP)(uint64_t, size_t*),
          bool     (*RMI_LOAD)(char const*),
          void     (*RMI_CLEANUP)()>
class BenchmarkRMI : public PredecessorSearchBase<KEY_TYPE> {
 public:
  using KeyType    = KEY_TYPE;
  using PayloadType = PAYLOAD_TYPE;
  static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

  BenchmarkRMI() : loaded_(false) {}

  ~BenchmarkRMI() {
    if (loaded_) RMI_CLEANUP();
  }

  template <typename Iterator>
  void bulk_load(const Iterator begin, const Iterator end) {
    this->ps_init(begin, end);

    const char* env_path = std::getenv("DELI_RMI_PATH");
    const std::string rmi_path = (env_path != nullptr) ? env_path : "../rmi_data";

    if (!RMI_LOAD(rmi_path.c_str())) {
      throw std::runtime_error(
          "RMI: failed to load model parameters from '" + rmi_path +
          "'. Set DELI_RMI_PATH to the directory containing the *_L*_PARAMETERS files.");
    }
    loaded_ = true;
  }

  PAYLOAD_TYPE lower_bound(const KEY_TYPE key) const {
    size_t err = 0;
    uint64_t guess = RMI_LOOKUP(static_cast<uint64_t>(key), &err);

    const size_t n  = this->sorted_keys_.size();
    size_t lo = (static_cast<uint64_t>(err) <= guess)
                    ? static_cast<size_t>(guess - err)
                    : 0;
    size_t hi = static_cast<size_t>(guess + err + 1);

    // The model may have been trained on more keys than are loaded in this run.
    // Clamp to a valid range; fall back to a full scan if the guess is beyond n.
    if (lo >= n) { lo = 0; hi = n; }
    hi = std::min(hi, n);

    return static_cast<PAYLOAD_TYPE>(this->find_successor_in_range(key, lo, hi));
  }

  void insert(const KEY_TYPE&, const PAYLOAD_TYPE&) {
    throw std::runtime_error("RMI: insert not supported (static index)");
  }
  void erase(const KEY_TYPE&) {
    throw std::runtime_error("RMI: erase not supported (static index)");
  }

  static std::string name()    { return "RMI"; }
  static std::string variant() { return std::to_string(rmi_variant); }

  // applicable() always returns true here: the outer benchmark_rmi() function
  // already guards dispatch behind #ifdef DELI_HAVE_RMI_<dataset>.
  bool applicable(const std::string&) { return true; }

 private:
  bool loaded_;
};

// ── Internal helpers ──────────────────────────────────────────────────────────

// Run LOOKUP_EXISTING and LOOKUP_IN_DISTRIBUTION for a single RMI type.
template <typename RMIType, typename KeyType, typename PayloadType>
static void rmi_run_workloads(
    const bench_config& config,
    std::vector<std::pair<KeyType, PayloadType>>& key_values,
    const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_values) {
  constexpr Workload supported_workloads[] = {LOOKUP_EXISTING,
                                              LOOKUP_IN_DISTRIBUTION,
                                              LOOKUP_UNIFORM};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<RMIType>(config, key_values, wl,
                                         shifting_key_values);
  }
}

// Convenience alias: instantiate BenchmarkRMI for a given namespace + variant.
// Usage: DELI_RMI_RUN(ns_prefix, variant_number, KeyType, PayloadType, ...)
#define DELI_RMI_RUN(ns, var, KT, PT, cfg, kv, skv)            \
  deli_testbed::rmi_run_workloads<                              \
      deli_testbed::BenchmarkRMI<KT, PT, var,                  \
                                 ns##_##var::lookup,             \
                                 ns##_##var::load,               \
                                 ns##_##var::cleanup>>(         \
      cfg, kv, skv)

// Dispatch variant 0 unconditionally; variants 1-9 only when pareto=true.
#define DELI_RMI_DISPATCH(ns, KT, PT, cfg, kv, skv)  \
  do {                                                \
    DELI_RMI_RUN(ns, 0, KT, PT, cfg, kv, skv);       \
    if ((cfg).pareto) {                               \
      DELI_RMI_RUN(ns, 1, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 2, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 3, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 4, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 5, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 6, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 7, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 8, KT, PT, cfg, kv, skv);     \
      DELI_RMI_RUN(ns, 9, KT, PT, cfg, kv, skv);     \
    }                                                 \
  } while (0)

// ── Public benchmark entry point ──────────────────────────────────────────────

template <typename KeyType, typename PayloadType>
void benchmark_rmi(
    const bench_config& config,
    std::vector<std::pair<KeyType, PayloadType>>& key_values,
    const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_values) {

  const char* rmi_env = std::getenv("DELI_RMI_PATH");
  const std::string rmi_data_path = (rmi_env != nullptr) ? rmi_env : "../rmi_data";
  if (!std::filesystem::exists(rmi_data_path)) return;

  const std::string dataset =
      std::filesystem::path(config.data_filename).stem().string();

  // Each block is compiled only when the corresponding models have been
  // generated and registered in indices/rmi/all_rmis.h.

#ifdef DELI_HAVE_RMI_books_200M_uint32
  if (dataset == "books_200M_uint32" && key_values.size() == 200'000'000) {
    DELI_RMI_DISPATCH(books_200M_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_books_800M_uint64
  if (dataset == "books_800M_uint64" && key_values.size() == 800'000'000) {
    DELI_RMI_DISPATCH(books_800M_uint64, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_fb_200M_uint64
  if (dataset == "fb_200M_uint64" && key_values.size() == 200'000'000) {
    DELI_RMI_DISPATCH(fb_200M_uint64, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_osm_cellids_800M_uint64
  if (dataset == "osm_cellids_800M_uint64" && key_values.size() == 800'000'000) {
    DELI_RMI_DISPATCH(osm_cellids_800M_uint64, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_wiki_ts_200M_uint64
  if (dataset == "wiki_ts_200M_uint64" && key_values.size() == 200'000'000) {
    DELI_RMI_DISPATCH(wiki_ts_200M_uint64, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_uniform_uint32
  if (dataset == "uniform_uint32" && key_values.size() == 50'000'000) {
    DELI_RMI_DISPATCH(uniform_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_normal_uint32
  if (dataset == "normal_uint32" && key_values.size() == 50'000'000) {
    DELI_RMI_DISPATCH(normal_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_mix_gauss_uint32
  if (dataset == "mix_gauss_uint32" && key_values.size() == 50'000'000) {
    DELI_RMI_DISPATCH(mix_gauss_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_exponential_uint32
  if (dataset == "exponential_uint32" && key_values.size() == 50'000'000) {
    DELI_RMI_DISPATCH(exponential_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

#ifdef DELI_HAVE_RMI_zipf_uint32
  if (dataset == "zipf_uint32" && key_values.size() == 50'000'000) {
    DELI_RMI_DISPATCH(zipf_uint32, KeyType, PayloadType,
                      config, key_values, shifting_key_values);
    return;
  }
#endif

  // No model available for this dataset — silently skip.
}

#undef DELI_RMI_RUN
#undef DELI_RMI_DISPATCH

}  // namespace deli_testbed

#else  // COMPILE_RMI not defined — provide an empty stub

namespace deli_testbed {
template <typename KeyType, typename PayloadType>
void benchmark_rmi(const bench_config&,
                   std::vector<std::pair<KeyType, PayloadType>>&,
                   const std::vector<std::pair<KeyType, PayloadType>>&) {}
}  // namespace deli_testbed

#endif  // COMPILE_RMI
