// Copyright (c) 2018 The GAM Authors 

#include "settings.h"
#ifdef GFUNC_SUPPORT

#include "gfunc.h"
#include "log.h"
#include "gallocator.h"
#include "util.h"

void IncrDouble(void* ptr, uint64_t arg) {
  double a = force_cast<double>(arg);
  epicLog(LOG_WARNING, "a = %lf", a);
  if (arg == 0) {
    (*(char*) ptr)++;
  } else {
    (*(char*) ptr) += a;
  }
}

void Incr(void* ptr, uint64_t arg) {
  if (arg == 0) {
    (*(char*) ptr)++;
  } else {
    (*(char*) ptr) += arg;
  }
}

/** import from Graph to support GFunc that can operate on Vertex
 * TODO: move Storage of graph engine to GAM
 */
struct Vertex {
  // general
  uint64_t vertex_id;bool active, active_minor_step;
  // currently assume data type is only double
  // app data
  double value;
  // vertex program
  double accumulator;
  // for scatter
  double delta;
};

void GatherPagerank(void *ptr, uint64_t argc) {
  // for simplicity and performance, we use push-based weight propagation instead of pull-based gathering
  // here, reduce value in vertex.accumulator atomically
  Vertex *vertex = reinterpret_cast<Vertex*>(ptr);
  double value = force_cast<double>(argc);
  vertex->accumulator += value;
}

void ApplyPagerank(void *ptr, uint64_t argc) {
  const static double REST_PROB = 0.15;
  const static double TOLERANCE = 0.01;
  Vertex *vertex_data = reinterpret_cast<Vertex*>(ptr);
  if (vertex_data->active) {
    vertex_data->active = false;
    double new_pagerank = vertex_data->accumulator * (1.0 - REST_PROB)
        + REST_PROB;
    vertex_data->delta = new_pagerank - vertex_data->value;
    vertex_data->active_minor_step = std::abs(vertex_data->delta) > TOLERANCE;
    vertex_data->value = new_pagerank;
  } else {
    vertex_data->active_minor_step = false;
  }
}
void ScatterPagerank(void *ptr, uint64_t argc) {
  double delta = force_cast<double>(argc);
  Vertex *vertex_data = reinterpret_cast<Vertex*>(ptr);
  vertex_data->accumulator += delta;
  vertex_data->active = true;
}

int GetGFuncID(GFunc* gfunc) {
  if (!gfunc)
    return -1;
  for (int i = 0; i < sizeof(GAllocFactory::gfuncs); i++) {
    if (GAllocFactory::gfuncs[i] == gfunc) {
      return i;
    }
  }
  epicLog(LOG_WARNING, "cannot find the gfunc in the gfunc directory");
  return -1;
}

GFunc* GetGFunc(int id) {
  if (id == -1)
    return nullptr;
  epicAssert(id < sizeof(GAllocFactory::gfuncs));
  return GAllocFactory::gfuncs[id];
}

#endif

