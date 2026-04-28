// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../indices/benchmark_alex.h"
#include "../indices/benchmark_lipp.h"
#include "../indices/benchmark_parrot.h"
#include "../indices/benchmark_pgm_static.h"
#include "../indices/benchmark_pgm_dynamic.h"
#include "../indices/benchmark_rs.h"
#include "../indices/benchmark_tlx.h"
#include "../indices/benchmark_sea21.h"
#include "../indices/benchmark_rmi.h"
#ifdef ENABLE_SWIX
#include "../indices/benchmark_swix.h"
#endif

#include <iomanip>
#include <sstream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <vector>
#include <ranges>
#include <filesystem>
#include <unordered_set>

#include "flags.h"
#include "utils.h"
#include "benchmark.h"

template <typename KeyType, typename PayloadType>
void execute(const bench_config& config, const std::unordered_set<std::string>& allowed_indices) {

  // Read keys from file
  std::vector<KeyType> keys;
  if (config.data_filename.ends_with(".txt")) {
    keys = utils::load_text_data<KeyType>(config.data_filename);
  } else {
    keys = utils::load_binary_data<KeyType>(config.data_filename);
  }

  if (keys.empty()) {
    std::cerr << "Error: No keys loaded from file: " << config.data_filename << std::endl;
    return;
  }

  std::cout << "\n=== Running benchmarks" << (config.entire_dataset ? " on entire dataset" : " with exponentially increasing init_num_keys") << " ===" << std::endl;

  // Define index types and names
  const std::vector<std::string> index_names = {
      // ── KEY_VALUE mode ──────────────────────────────────────────────────────
      // True key-value stores: return the payload associated with the successor key.
      "ALEX",              // dynamic learned index (KEY_VALUE)
      "LIPP",              // dynamic learned index (KEY_VALUE)
      "DeLI-Static-Payload",
      "DeLI-Dynamic-Payload",
      "PGM-Dynamic",       // dynamic learned index (KEY_VALUE)
      // ── PREDECESSOR_SEARCH mode ─────────────────────────────────────────────
      // All return the successor/predecessor key itself (payload == key).
      "DeLI-Static",       // exact successor, native
      "DeLI-Dynamic",      // exact successor, native
      "TLX",               // exact successor via B-tree lower_bound
      "SEA21",             // exact predecessor, vector top-level (uint32_t only)
      "RS",                // approximate position → shared range search
      "PGM-Static",        // approximate position → shared range search
      "ALEX-PS",           // approximate position via sampling → shared range search
      "PGM-Dynamic-PS",    // approximate position via sampling → shared range search
      "RMI",                // approximate position via recursive model index → shared range search
#ifdef ENABLE_SWIX
      // ── SWIX (sliding-window temporal index) ──────────────────────────────
      "SWIX",              // sliding window — SHIFTING only
#endif
  };

  // Compute how many keys are needed for the shifting suffix (independent of init size).
  const size_t inserts_per_batch = static_cast<size_t>((config.batch_size + 2) / 3);
  const size_t required_append_keys = inserts_per_batch * static_cast<size_t>(config.max_batches + 1);

  // Build the list of dataset sizes to iterate over.
  // Default: powers of two from 2^min_size to 2^max_size (capped at keys.size()).
  // With --entire_dataset: a single run using all keys except the last required_append_keys,
  // which are reserved for the shifting suffix.
  std::vector<size_t> sizes_to_run;
  if (config.entire_dataset) {
    if (keys.size() <= required_append_keys) {
      std::cerr << "Error: dataset is too small to reserve " << required_append_keys
                << " keys for the shifting suffix." << std::endl;
      return;
    }
    sizes_to_run.push_back(keys.size() - required_append_keys);
  } else {
    for (size_t s = (size_t(1) << config.min_size); s <= (size_t(1) << config.max_size); s *= 2) {
      if (s >= keys.size()) {
        sizes_to_run.push_back(keys.size());
        break;
      }
      sizes_to_run.push_back(s);
    }
  }

  for (const size_t current_init_key_size : sizes_to_run) {
    std::cout << "\n=== Testing with " << current_init_key_size << " initial keys ===" << std::endl;

    // key_pairs_kv: key → random payload  (KEY_VALUE mode: ALEX, LIPP, PGM-Dynamic, DeLI-Payload)
    // key_pairs_ps: key → key             (PREDECESSOR_SEARCH mode: all others; payload==key so
    //                                      the benchmark can validate returned successor against
    //                                      the expected successor key stored as payload)
    std::vector<std::pair<KeyType, PayloadType>> key_pairs_kv(current_init_key_size);
    std::vector<std::pair<KeyType, PayloadType>> key_pairs_ps(current_init_key_size);

    // original_key_pairs: keys in their input-file order (not sorted), used for INSERT_DELETE.
    // Payload is set to key so it works for both KEY_VALUE and PREDECESSOR_SEARCH indices without
    // consuming extra random values that would shift payloads for the sorted pairs.
    std::vector<std::pair<KeyType, PayloadType>> original_key_pairs(current_init_key_size);
    for (size_t i = 0; i < current_init_key_size; ++i)
      original_key_pairs[i] = {keys[i], static_cast<PayloadType>(keys[i])};

    // Build a full sorted shifting stream: initial prefix + append suffix
    std::vector<std::pair<KeyType, PayloadType>> shifting_key_pairs_kv;
    std::vector<std::pair<KeyType, PayloadType>> shifting_key_pairs_ps;
    
    {
      std::vector<KeyType> sorted_keys(keys.begin(), keys.begin() + current_init_key_size);
      std::sort(sorted_keys.begin(), sorted_keys.end());

      for (size_t i = 0; i < current_init_key_size; ++i) {
        key_pairs_kv[i].first  = sorted_keys[i];
        key_pairs_kv[i].second = static_cast<PayloadType>(rand_gen());

        key_pairs_ps[i].first = key_pairs_ps[i].second = sorted_keys[i];
      }

      // Build a full sorted shifting stream: initial prefix + append suffix
      shifting_key_pairs_kv = key_pairs_kv;
      shifting_key_pairs_ps = key_pairs_ps;
      shifting_key_pairs_kv.reserve(current_init_key_size + required_append_keys);
      shifting_key_pairs_ps.reserve(current_init_key_size + required_append_keys);
      if (keys.size() >= current_init_key_size && required_append_keys > 0) {
        std::unordered_set<KeyType> added_keys;
        added_keys.reserve(required_append_keys);

        for (size_t i = current_init_key_size; i < keys.size(); ++i) {
          if (shifting_key_pairs_kv.size() >= current_init_key_size + required_append_keys) {
            break;
          }
          const KeyType key = keys[i];
          if (std::binary_search(sorted_keys.begin(), sorted_keys.end(), key) || 
              !added_keys.insert(key).second) {
            continue;
          }
          shifting_key_pairs_kv.emplace_back(key, static_cast<PayloadType>(rand_gen()));
          shifting_key_pairs_ps.emplace_back(key, key);
        }
      }
    }

    // Do NOT sort shifting_key_pairs_kv and shifting_key_pairs_ps
    // The first init_key_size are already sorted (they are bulk loaded)
    // The rest is as it appears in the file as expected
    
    for (const auto& index_name : index_names) {
      if (!allowed_indices.empty() && !allowed_indices.count(index_name)) continue;
      std::cout << "--- Running workloads for " << index_name << " (init_keys=" << current_init_key_size << ") ---" << std::endl;
      
      // ── KEY_VALUE mode ──────────────────────────────────────────────────────
      if (index_name == "ALEX") {
        deli_testbed::benchmark_alex<KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv, original_key_pairs);
      }
      else if (index_name == "LIPP") {
        deli_testbed::benchmark_lipp<KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv);
      }
      // else if (index_name == "DILI") {
      //   deli_testbed::benchmark_dili<KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv);
      // }
      else if (index_name == "DeLI-Dynamic-Payload") {
        deli_testbed::benchmark_deli_dynamic<true, KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv, original_key_pairs);
      }
      else if (index_name == "DeLI-Static-Payload") {
        deli_testbed::benchmark_deli_static<true, KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv);
      }
      else if (index_name == "PGM-Dynamic") {
        deli_testbed::benchmark_pgm_dynamic<KeyType, PayloadType>(config, key_pairs_kv, shifting_key_pairs_kv, original_key_pairs);
      }
      // ── PREDECESSOR_SEARCH mode ─────────────────────────────────────────────
      else if (index_name == "DeLI-Dynamic") {
        deli_testbed::benchmark_deli_dynamic<false, KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps, original_key_pairs);
      }
      else if (index_name == "DeLI-Static") {
        deli_testbed::benchmark_deli_static<false, KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
      }
      else if (index_name == "TLX") {
        deli_testbed::benchmark_tlx<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps, original_key_pairs);
      }
      else if (index_name == "SEA21") {
        deli_testbed::benchmark_sea21<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps, original_key_pairs);
      }
      else if (index_name == "RS") {
        if (config.entire_dataset) {
          deli_testbed::benchmark_rs<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
        }
      }
      else if (index_name == "PGM-Static") {
        deli_testbed::benchmark_pgm_static<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
      }
      else if (index_name == "ALEX-PS") {
        deli_testbed::benchmark_alex_ps<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
      }
      else if (index_name == "DeLI-Static-PS") {
        deli_testbed::benchmark_deli_static_ps<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
      }
      else if (index_name == "PGM-Dynamic-PS") {
        deli_testbed::benchmark_pgm_dynamic_ps<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
      } 
      else if (index_name == "RMI") {
        if (config.entire_dataset) {
          deli_testbed::benchmark_rmi<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
        }
      }
#ifdef ENABLE_SWIX
      else if (index_name == "SWIX") {
        if (config.data_filename.find("USA") != std::string::npos) {
          deli_testbed::benchmark_swix<KeyType, PayloadType>(config, key_pairs_ps, shifting_key_pairs_ps);
        }
      }
#endif
      else {
        throw std::runtime_error("Unsupported index: " + index_name);
      }
    }
  }
}

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --batch_size             number of operations per batch
 * --output_folder          folder path for benchmark output
 * --max_batches            maximum number of measured batches before adaptive stop (default: 3)
 *
 * Optional flags:
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 * --clear_cache            whether to clear cache before each batch
 * --pareto                 whether to run the benchmark for all the parameters
 * --min_batches            minimum number of measured batches before adaptive stop (default: 2)
 * --rse_target             target relative standard error for early stop (default: 0.03)
 * --min_size               log of the minimum number of initial keys (default: 8 -> 2^8)
 * --max_size               log of the maximum number of initial keys (default: 20 -> 2^20)
 * --indices                comma-separated list of index names to run (default: all)
 * --entire_dataset         run a single experiment on the full dataset (mutually exclusive with min_size/max_size)
 * --full_dataset_batch     (LOOKUP_EXISTING/IN_DISTRIBUTION/UNIFORM only) set batch_size = dataset size
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  auto batch_size = stoi(get_required(flags, "batch_size"));
  auto num_batches = get_with_default(flags, "num_batches", "");
  auto min_batches = stoi(get_with_default(flags, "min_batches", "2"));
  auto max_batches = stoi(get_with_default(flags, "max_batches", "3"));
  auto rse_target = stod(get_with_default(flags, "rse_target", "0.05"));
  std::string output_folder = get_required(flags, "output_folder");
  auto time_limit = stod(get_with_default(flags, "time_limit", "2"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  bool clear_cache = get_boolean_flag(flags, "clear_cache");
  bool pareto = get_boolean_flag(flags, "pareto");
  bool entire_dataset = get_boolean_flag(flags, "entire_dataset");
  bool full_dataset_batch = get_boolean_flag(flags, "full_dataset_batch");
  auto min_size = stoi(get_with_default(flags, "min_size", "8"));
  auto max_size = stoi(get_with_default(flags, "max_size", "20"));
  std::string indices_str = get_with_default(flags, "indices", "");
  std::unordered_set<std::string> allowed_indices;
  if (!indices_str.empty()) {
    std::istringstream ss_idx(indices_str);
    std::string token;
    while (std::getline(ss_idx, token, ',')) {
      if (!token.empty()) allowed_indices.insert(token);
    }
  }

  if (min_batches < 1) {
    throw std::runtime_error("--min_batches must be >= 1");
  }
  if (max_batches < min_batches) {
    throw std::runtime_error("--max_batches must be >= --min_batches");
  }
  if (rse_target <= 0.0) {
    throw std::runtime_error("--rse_target must be > 0");
  }
  if (!num_batches.empty()) {
    throw std::runtime_error("--num_batches is deprecated. Please use --min_batches and --max_batches instead.");
  }
  if (entire_dataset && (flags.count("min_size") || flags.count("max_size"))) {
    throw std::runtime_error("--entire_dataset is mutually exclusive with --min_size and --max_size");
  }

  // Check if input file does exist
  {
    std::ifstream infile(keys_file_path);
    if (!infile.good()) {
      throw std::runtime_error("The specified keys_file does not exist: " + keys_file_path);
    }
  }

  // Extract dataset name from input file path
  std::filesystem::path input_path(keys_file_path);
  std::string dataset_name = input_path.stem().string();  // filename without extension
  
  // Create output folder if it doesn't exist
  std::filesystem::path output_dir(output_folder);
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directories(output_dir);
  }
  
  // Create output filename with dataset name and timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << "benchmark_" << dataset_name << "_" << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S") << ".txt";
  
  std::filesystem::path out_filename = output_dir / ss.str();
  
  // Check if output file already exists
  if (std::filesystem::exists(out_filename)) {
    std::cerr << "Error: Output file already exists: " << out_filename << std::endl;
    std::cerr << "Benchmark terminated to avoid overwriting existing results." << std::endl;
    return 1;
  }
  
  std::ofstream out_file(out_filename);
  if (!out_file.is_open()) {
    throw std::runtime_error("Failed to create the benchmark output file: " + out_filename.string());
  }

  std::cout << "Benchmark results will be written to: " << out_filename << std::endl;

  // Prepare benchmark config object
  bench_config config {
      out_file: out_file,
      data_filename: keys_file_path,
      time_limit: time_limit,
      batch_size: batch_size,
      min_batches: min_batches,
      max_batches: max_batches,
      rse_target: rse_target,
      print_batch_stats: print_batch_stats,
      clear_cache: clear_cache,
      pareto: pareto,
      min_size: min_size,
      max_size: max_size,
      entire_dataset: entire_dataset,
      full_dataset_batch: full_dataset_batch
  };

  // Call execute with appropriate key type based on filename suffix
  if (keys_file_path.ends_with("_uint32")) {
    execute<uint32_t, uint32_t>(config, allowed_indices);
  } else if (keys_file_path.ends_with("_uint64")) {
    std::cout << "Unsupported uint64 key types in this build";
    // execute<uint64_t, uint64_t>(config, allowed_indices);
  } else if (keys_file_path.ends_with(".txt")) {
    std::cout << "Assuming uint64 key type for .txt files, unsupported in this build";
    // execute<uint64_t, uint64_t>(config, allowed_indices); // For text files, we assume uint64 keys
  } else {
    throw std::runtime_error("Unsupported key type in filename. Expected suffixes: _uint32, _uint64, or .txt");
  }

  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;
  
  return 0;
}
