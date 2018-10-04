// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_TXN_PARAMS_H__
#define __DATABASE_TPCC_TXN_PARAMS_H__

#include "CharArray.h"
#include "TpccConstants.h"
#include <string>

namespace Database {
namespace TpccBenchmark {

class DeliveryParam : public TxnParam {
 public:
  DeliveryParam() {
    type_ = DELIVERY;
  }
  virtual ~DeliveryParam() {
  }

 public:
  int w_id_;
  int o_carrier_id_;
  int64_t ol_delivery_d_;
  //#if defined(SLICE)
  // additional parameters
  int no_o_ids_[DISTRICTS_PER_WAREHOUSE];
  double sums_[DISTRICTS_PER_WAREHOUSE];
  int c_ids_[DISTRICTS_PER_WAREHOUSE];
  //#endif
};

class NewOrderParam : public TxnParam {
 public:
  NewOrderParam() {
    type_ = NEW_ORDER;
  }
  virtual ~NewOrderParam() {
  }

 public:
  int w_id_;
  int d_id_;
  int c_id_;
  int64_t o_entry_d_;
  size_t ol_cnt_;
  int i_ids_[15];
  int i_w_ids_[15];
  int i_qtys_[15];

  // to change read ratio
  size_t item_access_type_[15];
  size_t stock_access_type_[15];
  size_t warehouse_access_type_;
  size_t district_access_type_;
  size_t customer_access_type_;

  // change inserts to read or write
  size_t new_order_access_type_;
  size_t order_access_type_;
  size_t order_line_access_type_[15];

  //#if defined(SLICE)
  // additional parameters
  int next_o_id_;
  std::string s_dists_[15];
  double ol_amounts_[15];
  //#endif
};

class PaymentParam : public TxnParam {
 public:
  PaymentParam() {
    type_ = PAYMENT;
  }
  virtual ~PaymentParam() {
  }

 public:
  int w_id_;
  int d_id_;
  double h_amount_;
  int c_w_id_;
  int c_d_id_;
  int c_id_;
  std::string c_last_;
  int64_t h_date_;

  // to modify read ratio
  size_t warehouse_access_type_;
  size_t district_access_type_;
  size_t customer_access_type_;
  // change insert to read or write
  size_t history_access_type_;

};

class OrderStatusParam : public TxnParam {
 public:
  OrderStatusParam() {
    type_ = ORDER_STATUS;
  }
  virtual ~OrderStatusParam() {
  }

 public:
  int w_id_;
  int d_id_;
  std::string c_last_;
  int c_id_;
};

class StockLevelParam : public TxnParam {
 public:
  StockLevelParam() {
    type_ = STOCK_LEVEL;
  }
  virtual ~StockLevelParam() {
  }

 public:
  int w_id_;
  int d_id_;
};
}
}

#endif
