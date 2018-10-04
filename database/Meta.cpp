#include "Meta.h"

namespace Database {
GAlloc** gallocators = NULL;
GAlloc* default_gallocator = NULL;
size_t gThreadCount = 0;
size_t gParamBatchSize = 1000;
}
