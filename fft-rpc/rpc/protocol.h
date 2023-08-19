#pragma once

#include <photon/rpc/serialize.h>

#include <cstdint>
#include <complex>
  /**
  * @brief interface means class with member deleivers `IID` as
  * interface.
  */
  struct Interface {
    const static uint32_t IID = 0x222;
  };

  struct Msi : public Interface {
    const static uint32_t FID = 0x001;
    struct Request : public photon::rpc::Message {
      uint32_t work_id;
      uint64_t gaddr;
      uint32_t n;
      uint32_t l;
      uint32_t k;
      std::complex<float> T;
      PROCESS_FIELDS(work_id,gaddr,n,l,k,T);
    };
    struct Response : public photon::rpc::Message {
      bool result;
      PROCESS_FIELDS(result);
    };
  };


    struct Add : public Interface {
    const static uint32_t FID = 0x002;
    struct Request : public photon::rpc::Message {
      uint32_t a;
      uint32_t work_id;
      uint64_t gaddr;
      PROCESS_FIELDS(a, work_id, gaddr);
    };
    struct Response : public photon::rpc::Message {
      bool result;
      PROCESS_FIELDS(result);
    };
  };

