#ifndef __DATABASE_STORAGE_GAM_OBJECT_H__
#define __DATABASE_STORAGE_GAM_OBJECT_H__

#include "gallocator.h"

namespace Database{
class GAMObject {
public:
    // Write the content to the global memory addr
    virtual void Serialize(const GAddr& addr, GAlloc *gallocator) = 0;
    // Read the content from the global memory addr
    virtual void Deserialize(const GAddr& addr, GAlloc *gallocator) = 0;
};
}

#endif
