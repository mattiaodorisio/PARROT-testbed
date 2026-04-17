#include <iostream>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <string>
#include <iomanip>
#include <bit>

#include "utils.h"

template <typename KeyType>
void analyze_lcp(const std::string& dataset_name, const std::string& dataset_path) {
    std::vector<KeyType> keys;
    if (dataset_path.ends_with(".txt")) {
        keys = utils::load_text_data<KeyType>(dataset_path);
    } else {
        keys = utils::load_binary_data<KeyType>(dataset_path);
    }

    if (keys.empty()) {
        std::cerr << "Warning: No keys loaded from: " << dataset_path << std::endl;
        return;
    }

    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

    constexpr int bits = static_cast<int>(sizeof(KeyType) * 8);
    std::vector<size_t> lcp_hist(bits + 1, 0);

    for (size_t i = 1; i < keys.size(); ++i) {
        const KeyType xor_val = keys[i] ^ keys[i - 1];
        const int lcp = (xor_val == 0) ? bits : std::countl_zero(xor_val);
        lcp_hist[lcp]++;
    }

    const size_t total = keys.size() - 1;
    double mean_lcp = 0.0;
    size_t max_count = 0;
    for (int b = 0; b <= bits; ++b) {
        mean_lcp += static_cast<double>(b) * static_cast<double>(lcp_hist[b]);
        max_count = std::max(max_count, lcp_hist[b]);
    }
    if (total > 0) mean_lcp /= static_cast<double>(total);

    constexpr int bar_width = 50;
    std::cout << "\n=== " << dataset_name << " (" << keys.size() << " keys, mean LCP = "
              << std::fixed << std::setprecision(2) << mean_lcp << " bits) ===\n";
    std::cout << std::setw(4) << "bit" << "  " << std::setw(10) << "count"
              << "  " << std::setw(7) << "frac" << "  histogram\n";
    std::cout << std::string(4 + 2 + 10 + 2 + 7 + 2 + bar_width, '-') << "\n";

    for (int b = 0; b <= bits; ++b) {
        if (lcp_hist[b] == 0) continue;
        const double frac = static_cast<double>(lcp_hist[b]) / static_cast<double>(total);
        const int bar_len = static_cast<int>(frac * bar_width / (static_cast<double>(max_count) / static_cast<double>(total)));
        std::cout << std::setw(4) << b << "  "
                  << std::setw(10) << lcp_hist[b] << "  "
                  << std::setw(6) << std::fixed << std::setprecision(3) << frac * 100.0 << "%  "
                  << std::string(bar_len, '#') << "\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <data_folder>" << std::endl;
        return 1;
    }

    const std::filesystem::path data_dir(argv[1]);
    if (!std::filesystem::is_directory(data_dir)) {
        std::cerr << "Error: Not a directory: " << data_dir << std::endl;
        return 1;
    }

    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        if (!entry.is_regular_file()) continue;

        const std::string path_str = entry.path().string();
        const std::string name = entry.path().filename().string();

        if (path_str.ends_with("_uint32")) {
            analyze_lcp<uint32_t>(name, path_str);
        } else if (path_str.ends_with("_uint64")) {
            analyze_lcp<uint64_t>(name, path_str);
        } else if (path_str.ends_with(".txt")) {
            analyze_lcp<uint64_t>(name, path_str);
        }
    }

    return 0;
}
