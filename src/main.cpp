// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../indices/benchmark_alex.h"
#include "../indices/benchmark_lipp.h"
// #include "../indices/benchmark_dili.h"
#include "../indices/benchmark_deli.h"
#include "../indices/benchmark_pgm_static.h"
#include "../indices/benchmark_pgm_dynamic.h"

#include <iomanip>
#include <fstream>
#include <chrono>
#include <ctime>
#include <vector>
#include <ranges>
#include <filesystem>

#include "flags.h"
#include "utils.h"
#include "benchmark.h"

// TODO:
// - Implement missing workloads
// - Fix batches

static constexpr size_t NUM_BATCHES = 1;

template <typename KeyType, typename PayloadType>
void execute(const std::string& keys_file_path,
             int batch_size,
             const std::string& lookup_distribution,
             double time_limit,
             bool print_batch_stats,
             std::ofstream& out_file,
             bool clear_cache) {

  // Read keys from file
  std::vector<KeyType> keys = utils::load_binary_data<KeyType>(keys_file_path);

  // Generate exponentially increasing init_num_keys sizes
  constexpr size_t min_size = 1 << 8;   // Starting size
  constexpr size_t max_size = 1 << 20;  // Maximum size
  
  std::cout << "\n=== Running benchmarks with exponentially increasing init_num_keys ===" << std::endl;

  // Define index types and names
  std::vector<std::string> index_names = {"ALEX", "LIPP", "DeLI", "PGM-Static", "PGM-Dynamic"};

  // Prepare benchmark config object
  bench_config config {
      out_file,
      batch_size,
      lookup_distribution,
      time_limit,
      print_batch_stats,
      NUM_BATCHES
  };
  
  for (size_t current_init_key_size = min_size; current_init_key_size <= max_size; current_init_key_size *= 2) {
    std::cout << "\n=== Testing with " << current_init_key_size << " initial keys ===" << std::endl;

    // Create values array for current init size
    // Two vectors to be used depending on the index... TODO: improve this
    std::vector<std::pair<KeyType, PayloadType>> key_values(current_init_key_size);
    std::vector<std::pair<KeyType, PayloadType>> key_keys(current_init_key_size);
    for (size_t i = 0; i < key_values.size(); ++i) {
      key_values[i].first = keys[i];
      key_values[i].second = static_cast<PayloadType>(rand_gen());

      key_keys[i].first = key_keys[i].second = keys[i];
    }

    for (const auto& index_name : index_names) {
      std::cout << "--- Running workloads for " << index_name << " (init_keys=" << current_init_key_size << ") ---" << std::endl;
      
      if (index_name == "ALEX") {
        deli_testbed::benchmark_alex<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "LIPP") {
        deli_testbed::benchmark_lipp<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "DeLI") {
        deli_testbed::benchmark_deli<KeyType, PayloadType>(config, key_keys);
      }
      else if (index_name == "PGM-Static") {
        deli_testbed::benchmark_pgm_static<KeyType, PayloadType>(config, key_keys);
      }
      else if (index_name == "PGM-Dynamic") {
        deli_testbed::benchmark_pgm_dynamic<KeyType, PayloadType>(config, key_values);
      }
      else {
        throw std::runtime_error("Unsupported index: " + index_name);
      }
    }
  }
}

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --batch_size             number of operations (lookup or insert) per batch
 * --output_folder          folder path for benchmark output (default: results/)
 *
 * Optional flags:
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 * --clear_cache            whether to clear cache before each batch
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  auto batch_size = stoi(get_required(flags, "batch_size"));
  std::string output_folder = get_required(flags, "output_folder");
  std::string lookup_distribution = get_with_default(flags, "lookup_distribution", "uniform");
  auto time_limit = stod(get_with_default(flags, "time_limit", "0.5"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  bool clear_cache = get_boolean_flag(flags, "clear_cache");

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

  // Call execute with appropriate key type based on filename suffix
  if (keys_file_path.ends_with("_uint32")) {
    execute<uint32_t, uint32_t>(keys_file_path, batch_size, lookup_distribution, time_limit, print_batch_stats, out_file, clear_cache);
  } else if (keys_file_path.ends_with("_uint64")) {
    execute<uint64_t, uint64_t>(keys_file_path, batch_size, lookup_distribution, time_limit, print_batch_stats, out_file, clear_cache);
  } else {
    throw std::runtime_error("Unsupported key type in filename. Expected suffixes: _uint32 or _uint64");
  }

  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;
  
  return 0;
}
