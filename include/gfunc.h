// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_GFUNC_H_
#define INCLUDE_GFUNC_H_

#include <stdint.h>
#include "settings.h"
#ifdef GFUNC_SUPPORT

typedef void GFunc(void* addr, uint64_t arg);
void Incr(void* ptr, uint64_t);
void IncrDouble(void* ptr, uint64_t);

// register graph engine GFunc, which will be applied in homenode 
void GatherPagerank(void *ptr, uint64_t);
void ApplyPagerank(void *ptr, uint64_t);
void ScatterPagerank(void *ptr, uint64_t);

int GetGFuncID(GFunc* gfunc);
GFunc* GetGFunc(int id);

#endif

#endif /* INCLUDE_GFUNC_H_ */
