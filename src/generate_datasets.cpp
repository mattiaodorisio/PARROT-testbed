// Taken from https://github.com/LorenzoBellomo/SortedStaticIndexBenchmark/blob/main/scripts/elaborate_datasets.cpp

#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <cassert>
#include <random>
#include <stdint.h>
#include <cmath>
#include "zipf.h"
#include "flags.h"

#define M50 50000000
#define M1 1000000

std::vector<uint32_t> read_bin32_file(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);

    uint64_t N = 0;
    ifs.read(reinterpret_cast<char*>(&N), sizeof(uint64_t));

    std::vector<uint32_t> buf(N);
    ifs.read(reinterpret_cast<char*>(buf.data()), N * sizeof(uint32_t));

    // CHANGING LAST VALUES FROM UINT32_MAX to UINT32_MAX-1 to avoid issues for libraries that use it as "inf".
    for (auto i = buf.size() - 1; i >= 0; i--) {
        if (buf[i] == UINT32_MAX)
            buf[i] = UINT32_MAX - 1;
        else
            break;
    }
    return buf;
}

std::vector<uint64_t> read_bin64_file(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);

    uint64_t N = 0;
    ifs.read(reinterpret_cast<char*>(&N), sizeof(uint64_t));

    std::vector<uint64_t> buf(N);
    ifs.read(reinterpret_cast<char*>(buf.data()), N * sizeof(uint64_t));
    // CHANGING LAST VALUES FROM UINT64_MAX to UINT64_MAX-1 to avoid issues for libraries that use it as "inf".
    for (auto i = buf.size() - 1; i >= 0; i--) {
        if (buf[i] == UINT64_MAX)
            buf[i] = UINT64_MAX - 1;
        else
            break;
    }
    return buf;
}

void write_bin32_file(std::string fname, std::vector<uint32_t> data) {
    std::ofstream out(fname, std::ios_base::binary);
    uint64_t size = data.size();
    out.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    out.write(reinterpret_cast<char*>(data.data()), size * sizeof(uint32_t));
}

void write_bin64_file(std::string fname, std::vector<uint64_t> data) {
    std::ofstream out(fname, std::ios_base::binary);
    uint64_t size = data.size();
    out.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    out.write(reinterpret_cast<char*>(data.data()), size * sizeof(uint64_t));
}

std::vector<uint32_t> generate_uniform_distr(size_t size) {
    std::uniform_int_distribution<uint32_t> distr(0, UINT32_MAX);
    std::default_random_engine generator(0);
    std::vector<uint32_t> data(size);
    for (auto x = 0; x < size; x++) {
        data[x] = distr(generator); 
    }
    return data;
}

std::vector<uint32_t> generate_normal_distr(size_t size) {

    std::default_random_engine generator(1);
    std::normal_distribution<double> distribution(UINT32_MAX/2,UINT32_MAX/4);
    std::vector<uint32_t> data(size);
    for (auto x = 0; x < size; x++) {
        double sample;
        do{
            sample = distribution(generator);
        } while( sample<0 || sample>(UINT32_MAX >> 1));
        data[x] = (int) sample;
    }
    return data;
}

std::vector<uint32_t> generate_lognormal_distr(size_t size) {

    std::default_random_engine generator(2);
    std::lognormal_distribution<double> distribution(0, 0.5);
    std::vector<uint32_t> data(size);
    for (auto x = 0; x < size; x++) {
        double sample;
        do{
            sample = distribution(generator) * UINT32_MAX/5;
        } while( sample<0 || sample>(UINT32_MAX >> 1) );
        data[x] = (int) sample;
    }
    return data;
}

std::vector<uint32_t> generate_exponential_distr(size_t size) {

    std::default_random_engine generator(3);
    std::exponential_distribution<double> distribution(2);
    std::vector<uint32_t> data(size);
    for (auto x = 0; x < size; x++) {
        double sample;
        do{
            sample = distribution(generator) * UINT32_MAX/5;
        } while( sample<0 || sample>(UINT32_MAX >> 1) );
        data[x] = (int) sample;
    }
    return data;
}

std::vector<uint32_t> generate_chisquared_distr(size_t size) {

    std::default_random_engine generator(4);
    std::chi_squared_distribution<double> distribution(1);
    std::vector<uint32_t> data(size);
    for (auto x = 0; x < size; x++) {
        double sample;
        do{
            sample = distribution(generator) * UINT32_MAX/10;
        } while( sample<0 || sample>(UINT32_MAX >> 1) );
        data[x] = (int) sample;
    }
    return data;
}

std::vector<uint32_t> generate_zipf_distr(size_t size) {
    std::vector<uint32_t> data(size);
    ScrambledZipfianGenerator zipf_gen(size);
    for (size_t i = 0; i < size; ++i) {
      data[i] = zipf_gen.nextValue();
    }
    return data;
}

template<typename T>
void print_stats(std::vector<T> data) {
    std::cout << "SIZE: " << data.size() << std::endl;
    auto num_dup = 0;
    uint64_t tot_gap = 0;
    auto curr = data.begin() + 1;
    T prev_elem = data[0];
    while(curr != data.end()) {
        if (prev_elem == *curr) 
            num_dup++;
        
        assert (*curr >= prev_elem);
        tot_gap += (*curr - prev_elem);
        prev_elem = *curr;
        curr++;
    }
    std::cout << "AVERAGE GAP: " << (tot_gap / (double)(data.size() - 1)) << std::endl;
    std::cout << "# duplicates: " << num_dup << std::endl;
    std::cout << "===========================================" << std::endl;
}

int main(int argc, char* argv[]) {
    auto flags = parse_flags(argc, argv);
    std::string data_path = get_required(flags, "data_dir");
    bool print_stats_flag = get_boolean_flag(flags, "print_stats");

    std::vector<uint32_t> data = generate_normal_distr(M50);
    write_bin32_file("../data/normal_50M_uint32", data);
    std::cout << "normal_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_exponential_distr(M50);
    write_bin32_file("../data/exponential_50M_uint32", data);
    std::cout << "exponential_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_lognormal_distr(M50);
    write_bin32_file("../data/lognormal_50M_uint32", data);
    std::cout << "lognormal_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_zipf_distr(M50);
    write_bin32_file("../data/zipf_50M_uint32", data);
    std::cout << "zipf_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_uniform_distr(M50);
    write_bin32_file("../data/uniform_50M_uint32", data);
    std::cout << "uniform_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = read_bin32_file("../data/books_200M_uint32");
    // REWRITING THE FILE ON DISK TO REPLACE UINT32_MAX with UINT32_MAX - 1 for safety for some libs
    for (size_t i = data.size() - 1; i >= 0; --i) {
        if (data[i] == UINT32_MAX)
            data[i] = UINT32_MAX - 1;
        else
            break;
    }
    write_bin32_file("../data/books_200M_uint32", data); 
    std::cout << "books_200M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    return 0;
}
