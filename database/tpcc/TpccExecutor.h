// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_EXECUTOR_H__
#define __DATABASE_TPCC_EXECUTOR_H__

#include "TransactionExecutor.h"
#include "TpccProcedure.h"

namespace Database {
namespace TpccBenchmark {
class TpccExecutor : public TransactionExecutor {
 public:
  TpccExecutor(IORedirector* const redirector,
               StorageManager *storage_manager, size_t thread_count_)
      : TransactionExecutor(redirector, storage_manager, thread_count_) {
  }
  ~TpccExecutor() {
  }

  virtual void PrepareProcedures() {
    registers_[TupleType::DELIVERY] = []() {
      DeliveryProcedure* procedure = new DeliveryProcedure();
      return procedure;
    };
    registers_[TupleType::NEW_ORDER] = []() {
      NewOrderProcedure* procedure = new NewOrderProcedure();
      return procedure;
    };
    registers_[TupleType::PAYMENT] = []() {
      PaymentProcedure* procedure = new PaymentProcedure();
      return procedure;
    };
    registers_[TupleType::ORDER_STATUS] = []() {
      OrderStatusProcedure* procedure = new OrderStatusProcedure();
      return procedure;
    };
    registers_[TupleType::STOCK_LEVEL] = []() {
      StockLevelProcedure* procedure = new StockLevelProcedure();
      return procedure;
    };

    deregisters_[TupleType::DELIVERY] = [](StoredProcedure* procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::NEW_ORDER] = [](StoredProcedure* procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::PAYMENT] = [](StoredProcedure* procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::ORDER_STATUS] = [](StoredProcedure* procedure) {
      delete procedure;
      procedure = NULL;
    };
    deregisters_[TupleType::STOCK_LEVEL] = [](StoredProcedure* procedure) {
      delete procedure;
      procedure = NULL;
    };
  }

};
}
}
#endif
