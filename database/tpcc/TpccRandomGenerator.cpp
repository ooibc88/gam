// NOTICE: this file is adapted from Cavalia
#include "TpccRandomGenerator.h"

namespace Database {
namespace TpccBenchmark {
const std::string TpccRandomGenerator::syllables_[10] = { "BAR", "OUGHT",
    "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
const int TpccRandomGenerator::cLast_ = TpccRandomGenerator::GenerateInteger(
    0, 255);
const int TpccRandomGenerator::cId_ = TpccRandomGenerator::GenerateInteger(
    0, 1023);
const int TpccRandomGenerator::orderlineItemId_ =
    TpccRandomGenerator::GenerateInteger(0, 8191);

}
}
