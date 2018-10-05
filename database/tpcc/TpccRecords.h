// NOTICE: this file is adapted from Cavalia
#ifndef _DATABASE_TPCC_RECORDS_H__
#define _DATABASE_TPCC_RECORDS_H__

#include <string>
#include <sstream>

namespace Database {
namespace TpccBenchmark {

/******************** records **********************/
struct ItemRecord {
  int i_id_;
  int i_im_id_;
  char i_name_[32];
  double i_price_;
  char i_data_[64];
  // for replicate 
  int w_id_;
};

static std::string ItemToString(ItemRecord &item) {
  std::stringstream item_str;
  item_str << item.i_id_ << ";";
  item_str << item.i_im_id_ << ";";
  item_str << item.i_name_ << ";";
  item_str << item.i_price_ << ";";
  item_str << item.i_data_;
  return item_str.str();
}

struct WarehouseRecord {
  int w_id_;
  char w_name_[16];
  char w_street_1_[32];
  char w_street_2_[32];
  char w_city_[32];
  char w_state_[2];
  char w_zip_[9];
  double w_tax_;
  double w_ytd_;
};

static std::string WarehouseToString(WarehouseRecord &warehouse) {
  std::stringstream warehouse_str;
  warehouse_str << warehouse.w_id_ << ";";
  warehouse_str << warehouse.w_name_ << ";";
  warehouse_str << warehouse.w_street_1_ << ";";
  warehouse_str << warehouse.w_street_2_ << ";";
  warehouse_str << warehouse.w_city_ << ";";
  warehouse_str << warehouse.w_state_ << ";";
  warehouse_str << warehouse.w_zip_ << ";";
  warehouse_str << warehouse.w_tax_ << ";";
  warehouse_str << warehouse.w_ytd_;
  return warehouse_str.str();
}

struct DistrictRecord {
  int d_id_;
  int d_w_id_;
  char d_name_[16];
  char d_street_1_[32];
  char d_street_2_[32];
  char d_city_[32];
  char d_state_[2];
  char d_zip_[9];
  double d_tax_;
  double d_ytd_;
  int d_next_o_id_;
};

static std::string DistrictToString(DistrictRecord &district) {
  std::stringstream district_str;
  district_str << district.d_id_ << ";";
  district_str << district.d_w_id_ << ";";
  district_str << district.d_name_ << ";";
  district_str << district.d_street_1_ << ";";
  district_str << district.d_street_2_ << ";";
  district_str << district.d_city_ << ";";
  district_str << district.d_state_ << ";";
  district_str << district.d_zip_ << ";";
  district_str << district.d_tax_ << ";";
  district_str << district.d_ytd_ << ";";
  district_str << district.d_next_o_id_;
  return district_str.str();
}

struct CustomerRecord {
  int c_id_;
  int c_d_id_;
  int c_w_id_;
  char c_first_[32];
  char c_middle_[2];
  char c_last_[32];
  char c_street_1_[32];
  char c_street_2_[32];
  char c_city_[32];
  char c_state_[2];
  char c_zip_[9];
  char c_phone_[32];
  int64_t c_since_;
  char c_credit_[2];
  double c_credit_lim_;
  double c_discount_;
  double c_balance_;
  double c_ytd_payment_;
  int c_payment_cnt_;
  int c_delivery_cnt_;
  char c_data_[500];
};

static std::string CustomerToString(CustomerRecord &customer) {
  std::stringstream customer_str;
  customer_str << customer.c_id_ << ";";
  customer_str << customer.c_d_id_ << ";";
  customer_str << customer.c_w_id_ << ";";
  customer_str << customer.c_first_ << ";";
  customer_str << customer.c_middle_ << ";";
  customer_str << customer.c_last_ << ";";
  customer_str << customer.c_street_1_ << ";";
  customer_str << customer.c_street_2_ << ";";
  customer_str << customer.c_city_ << ";";
  customer_str << customer.c_state_ << ";";
  customer_str << customer.c_zip_ << ";";
  customer_str << customer.c_phone_ << ";";
  customer_str << customer.c_since_ << ";";
  customer_str << customer.c_credit_ << ";";
  customer_str << customer.c_credit_lim_ << ";";
  customer_str << customer.c_discount_ << ";";
  customer_str << customer.c_balance_ << ";";
  customer_str << customer.c_ytd_payment_ << ";";
  customer_str << customer.c_payment_cnt_ << ";";
  customer_str << customer.c_delivery_cnt_ << ";";
  customer_str << customer.c_data_;
  return customer_str.str();
}

struct OrderRecord {
  int o_id_;
  int o_c_id_;
  int o_d_id_;
  int o_w_id_;
  int64_t o_entry_d_;
  int o_carrier_id_;
  int o_ol_cnt_;
  int o_all_local_;
};

static std::string OrderToString(OrderRecord &order) {
  std::stringstream order_str;
  order_str << order.o_id_ << ";";
  order_str << order.o_c_id_ << ";";
  order_str << order.o_d_id_ << ";";
  order_str << order.o_w_id_ << ";";
  order_str << order.o_entry_d_ << ";";
  order_str << order.o_carrier_id_ << ";";
  order_str << order.o_ol_cnt_ << ";";
  order_str << order.o_all_local_;
  return order_str.str();
}

struct NewOrderRecord {
  int o_id_;
  int d_id_;
  int w_id_;
};

static std::string NewOrderToString(NewOrderRecord &new_order) {
  std::stringstream new_order_str;
  new_order_str << new_order.o_id_ << ";";
  new_order_str << new_order.d_id_ << ";";
  new_order_str << new_order.w_id_;
  return new_order_str.str();
}

struct OrderLineRecord {
  int ol_o_id_;
  int ol_d_id_;
  int ol_w_id_;
  int ol_number_;
  int ol_i_id_;
  int ol_supply_w_id_;
  int64_t ol_delivery_d_;
  int ol_quantity_;
  double ol_amount_;
  char ol_dist_info_[32];
};

static std::string OrderLineToString(OrderLineRecord &order_line) {
  std::stringstream order_line_str;
  order_line_str << order_line.ol_o_id_ << ";";
  order_line_str << order_line.ol_d_id_ << ";";
  order_line_str << order_line.ol_w_id_ << ";";
  order_line_str << order_line.ol_number_ << ";";
  order_line_str << order_line.ol_i_id_ << ";";
  order_line_str << order_line.ol_supply_w_id_ << ";";
  order_line_str << order_line.ol_delivery_d_ << ";";
  order_line_str << order_line.ol_quantity_ << ";";
  order_line_str << order_line.ol_amount_ << ";";
  order_line_str << order_line.ol_dist_info_;
  return order_line_str.str();
}

struct HistoryRecord {
  int h_c_id_;
  int h_c_d_id_;
  int h_c_w_id_;
  int h_d_id_;
  int h_w_id_;
  int64_t h_date_;
  double h_amount_;
  char h_data_[32];
};

static std::string HistoryToString(HistoryRecord &history) {
  std::stringstream history_str;
  history_str << history.h_c_id_ << ";";
  history_str << history.h_c_d_id_ << ";";
  history_str << history.h_c_w_id_ << ";";
  history_str << history.h_d_id_ << ";";
  history_str << history.h_w_id_ << ";";
  history_str << history.h_date_ << ";";
  history_str << history.h_amount_ << ";";
  history_str << history.h_data_;
  return history_str.str();
}

struct StockRecord {
  int s_i_id_;
  int s_w_id_;
  int s_quantity_;
  char s_dists_[10][32];
  int s_ytd_;
  int s_order_cnt_;
  int s_remote_cnt_;
  char s_data_[64];
};

static std::string StockToString(StockRecord &stock) {
  std::stringstream stock_str;
  stock_str << stock.s_i_id_ << ";";
  stock_str << stock.s_w_id_ << ";";
  stock_str << stock.s_quantity_ << ";";
  for (int i = 0; i < 10; ++i) {
    stock_str << stock.s_dists_[i] << ";";
  }
  stock_str << stock.s_ytd_ << ";";
  stock_str << stock.s_order_cnt_ << ";";
  stock_str << stock.s_remote_cnt_ << ";";
  stock_str << stock.s_data_;
  return stock_str.str();
}

struct DistrictNewOrderRecord {
  int w_id_;
  int d_id_;
  int o_id_;
};

static std::string DistrictNewOrderToString(
    DistrictNewOrderRecord &district_new_order) {
  std::stringstream district_new_order_str;
  district_new_order_str << district_new_order.w_id_ << ";";
  district_new_order_str << district_new_order.d_id_ << ";";
  district_new_order_str << district_new_order.o_id_;
  return district_new_order_str.str();
}
}
}

#endif
