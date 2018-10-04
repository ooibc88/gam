// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_RANDOM_GENERATOR_H__
#define __DATABASE_TPCC_RANDOM_GENERATOR_H__

#include "TpccConstants.h"
#include <string>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <unordered_set>

namespace Database {
namespace TpccBenchmark {
class TpccRandomGenerator {
 public:
  static int GenerateNonuniformInteger(const int &a, const int &x,
                                       const int &y) {
    int c = 0;
    if (a == 255) {
      c = cLast_;
    } else if (a == 1023) {
      c = cId_;
    } else if (a == 8191) {
      c = orderlineItemId_;
    }
    return (((GenerateInteger(0, a) | GenerateInteger(x, y)) + c) % (y - x + 1))
        + x;
  }

  //generate integer that falls inside [min, max].
  static int GenerateInteger(const int &min, const int &max) {
    // TODO: needs a better, cross-platform random generator!
    if (RAND_MAX <= 32767) {
      return ((rand() << 16) + rand()) % (max - min + 1) + min;
    } else {
      return rand() % (max - min + 1) + min;
    }
  }

  //generate integer that falls inside [min, max] but not equal to excluding.
  static int GenerateIntegerExcluding(const int &min, const int &max,
                                      const int &excluding) {
    int number = GenerateInteger(min, max - 1);
    if (number >= excluding) {
      number += 1;
    }
    return number;
  }

  static double GenerateFixedPoint(const int &decimal_places,
                                   const double &minimum,
                                   const double &maximum) {
    int multiplier = 1;
    for (int i = 0; i < decimal_places; ++i) {
      multiplier *= 10;
    }

    int int_min = (int) (minimum * multiplier + 0.5);
    int int_max = (int) (maximum * multiplier + 0.5);

    return (double) (GenerateInteger(int_min, int_max) / (double) multiplier);
  }

  //A random alphabetic string with length in range [minimum_length, maximum_length].
  static std::string GenerateAString(const int &min_length,
                                     const int &max_length) {
    return GenerateRandomString(min_length, max_length, 'a', 26);
  }

  static std::string GenerateNString(const int &min_length,
                                     const int &max_length) {
    return GenerateRandomString(min_length, max_length, '0', 10);
  }

  static std::string GenerateRandomString(const int &min_length,
                                          const int &max_length,
                                          const char base,
                                          const int &num_characters) {
    int length = GenerateInteger(min_length, max_length);
    int base_byte = int(base);
    std::string str;
    str.resize(length);
    for (int i = 0; i < length; ++i) {
      str.at(i) = char(base_byte + GenerateInteger(0, num_characters - 1));
    }
    return str;
  }

  //A last name as defined by TPC-C 4.3.2.3. Not actually random. The input number should fall into [0, 999].
  static std::string GenerateLastName(const int &number) {
    int index1 = number / 100;
    int index2 = (number / 10) % 10;
    int index3 = number % 10;
    return syllables_[index1] + syllables_[index2] + syllables_[index3];
  }

  //A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to maxCID.
  static std::string GenerateRandomLastName(const int &max_cid) {
    int min_cid = 999;
    if (max_cid - 1 < min_cid) {
      min_cid = max_cid - 1;
    }
    return GenerateLastName(999);
    // TODO: require randomized last name.
    //return GenerateLastName(GenerateNonuniformInteger(255, 0, min_cid));
  }

  static void SelectUniqueIds(const int &numUnique, const int &minimum,
                              const int &maximum,
                              std::unordered_set<int> &rows) {
    for (int i = 0; i < numUnique; ++i) {
      int num = -1;
      while (num == -1 || rows.find(num) != rows.end()) {
        num = GenerateInteger(minimum, maximum);
      }
      rows.insert(num);
    }
  }

  static void GenerateAddress(char *street_1, char *street_2, char *city,
                              char *state, char *zip) {
    std::string street1_str = TpccRandomGenerator::GenerateAString(MIN_STREET,
                                                                   MAX_STREET);
    memcpy(street_1, street1_str.c_str(), street1_str.size());
    std::string street2_str = TpccRandomGenerator::GenerateAString(MIN_STREET,
                                                                   MAX_STREET);
    memcpy(street_2, street2_str.c_str(), street2_str.size());
    std::string city_str = TpccRandomGenerator::GenerateAString(MIN_CITY,
                                                                MAX_CITY);
    memcpy(city, city_str.c_str(), city_str.size());
    std::string state_str = TpccRandomGenerator::GenerateAString(STATE, STATE);
    memcpy(state, state_str.c_str(), state_str.size());
    int length = ZIP_LENGTH - ZIP_SUFFIX.size();
    std::string zip_str = TpccRandomGenerator::GenerateNString(length, length)
        + ZIP_SUFFIX;
    memcpy(zip, zip_str.c_str(), zip_str.size());
  }

  static std::string FillOriginal(std::string data) {
    int original_length = ORIGINAL_STRING.length();
    int position = TpccRandomGenerator::GenerateInteger(
        0, data.length() - original_length);
    return data.substr(0, position) + ORIGINAL_STRING
        + data.substr(position + original_length);
  }

  static double GenerateTax() {
    return TpccRandomGenerator::GenerateFixedPoint(TAX_DECIMALS, MIN_TAX,
                                                   MAX_TAX);
  }

  static int64_t GenerateCurrentTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
  }

  static int GenerateWarehouseId(const int &starting_warehouse,
                                 const int &ending_warehouse) {
    return TpccRandomGenerator::GenerateInteger(starting_warehouse,
                                                ending_warehouse);
  }

  static int GenerateDistrictId(const int &num_districts_per_warehouse) {
    return TpccRandomGenerator::GenerateInteger(1, num_districts_per_warehouse);
  }

  static int GenerateCustomerId(const int &num_customers_per_district) {
    return TpccRandomGenerator::GenerateNonuniformInteger(
        1023, 1, num_customers_per_district);
  }

  static int GenerateItemId(const int &num_items) {
    return TpccRandomGenerator::GenerateNonuniformInteger(8191, 1, num_items);
  }

  ///// conflict with TPCC specification, just for testing
  static int GenerateCustomerIdUniform(int num_customers_per_district) {
    return TpccRandomGenerator::GenerateInteger(1, num_customers_per_district);
  }
  static int GenerateItemIdUniform(int num_items) {
    return TpccRandomGenerator::GenerateInteger(1, num_items);
  }

 private:
  const static std::string syllables_[10];
  const static int cLast_;
  const static int cId_;
  const static int orderlineItemId_;
};
}
}

#endif
