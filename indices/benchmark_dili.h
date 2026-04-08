#pragma once

#include "DILI/src/dili/DILI.h"

// Wrapper object

template <typename KeyType, typename PayloadType>
class BenchmarkDILI {
  static_assert(std::is_same<KeyType, long>::value,
                "DILI only supports long keys");
  static_assert(std::is_same<PayloadType, long>::value,
                "DILI only supports long payloads");

  public:
    using KeyType = KeyType;
    using PayloadType = PayloadType;

    BenchmarkDILI() {}

    void bulk_load(std::pair<KeyType, PayloadType>* values, size_t num_keys) {
      // TODO
    }
  
    PayloadType lower_bound(const KeyType key) {
      // TODO
      return PayloadType{}
    }
  
    void insert(const KeyType& key, const PayloadType& payload) {
      // TODO
    }

    void erase(const KeyType& key) {
      // TODO
    }


    bool applicable(const std::string& data_filename) {
      return false;
    }
  
  private:
    // TODO
    // DILI index;
};