// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_POPULATOR_H__
#define __DATABASE_TPCC_POPULATOR_H__

#include "gallocator.h"
#include "BenchmarkPopulator.h"
#include "TpccRandomGenerator.h"
#include "TpccKeyGenerator.h"

namespace Database {
namespace TpccBenchmark {
class TpccPopulator : public BenchmarkPopulator {
 public:
  TpccPopulator(StorageManager *storage_manager,
                TpccScaleParams* scale_params)
      : BenchmarkPopulator(storage_manager),
        scale_params_(scale_params) {
  }

  virtual void StartPopulate() {
    GAlloc* gallocator = default_gallocator;
    ItemRecord* item_record = new ItemRecord();
    WarehouseRecord* warehouse_record = new WarehouseRecord();
    DistrictRecord* district_record = new DistrictRecord();
    CustomerRecord* customer_record = new CustomerRecord();
    HistoryRecord* history_record = new HistoryRecord();
    DistrictNewOrderRecord* district_new_order_record =
        new DistrictNewOrderRecord();
    OrderRecord* order_record = new OrderRecord();
    NewOrderRecord* new_order_record = new NewOrderRecord();
    OrderLineRecord* order_line_record = new OrderLineRecord();
    StockRecord* stock_record = new StockRecord();

    Record *item_record_buf = new Record(
        storage_manager_->tables_[ITEM_TABLE_ID]->GetSchema());
    Record *warehouse_record_buf = new Record(
        storage_manager_->tables_[WAREHOUSE_TABLE_ID]->GetSchema());
    Record *district_record_buf = new Record(
        storage_manager_->tables_[DISTRICT_TABLE_ID]->GetSchema());
    Record *customer_record_buf = new Record(
        storage_manager_->tables_[CUSTOMER_TABLE_ID]->GetSchema());
    Record *history_record_buf = new Record(
        storage_manager_->tables_[HISTORY_TABLE_ID]->GetSchema());
    Record *district_new_order_record_buf = new Record(
        storage_manager_->tables_[DISTRICT_NEW_ORDER_TABLE_ID]->GetSchema());
    Record *order_record_buf = new Record(
        storage_manager_->tables_[ORDER_TABLE_ID]->GetSchema());
    Record *new_order_record_buf = new Record(
        storage_manager_->tables_[NEW_ORDER_TABLE_ID]->GetSchema());
    Record *order_line_record_buf = new Record(
        storage_manager_->tables_[ORDER_LINE_TABLE_ID]->GetSchema());
    Record *stock_record_buf = new Record(
        storage_manager_->tables_[STOCK_TABLE_ID]->GetSchema());

    // load items
    std::unordered_set<int> original_rows;
    TpccRandomGenerator::SelectUniqueIds(scale_params_->num_items_ / 10, 1,
                                         scale_params_->num_items_,
                                         original_rows);
    for (int item_id = 1; item_id <= scale_params_->num_items_; ++item_id) {
      bool is_origin = (original_rows.find(item_id) != original_rows.end());
      for (int w_id = scale_params_->starting_warehouse_;
          w_id <= scale_params_->ending_warehouse_; ++w_id) {
        // replicate in every warehouse
        // generate items
        GenerateItemRecord(item_id, w_id, is_origin, item_record);
        InsertItemRecord(item_record, item_record_buf, 0);
      }
    }
    // load warehouses
    for (int w_id = scale_params_->starting_warehouse_;
        w_id <= scale_params_->ending_warehouse_; ++w_id) {
      // generate warehouses
      GenerateWarehouseRecord(w_id, warehouse_record);
      InsertWarehouseRecord(warehouse_record, warehouse_record_buf, 0);
      for (int d_id = 1; d_id <= scale_params_->num_districts_per_warehouse_;
          ++d_id) {
        int d_next_o_id = scale_params_->num_customers_per_district_ + 1;
        // generate districts
        GenerateDistrictRecord(w_id, d_id, d_next_o_id, district_record);
        InsertDistrictRecord(district_record, district_record_buf, 0);

        std::unordered_set<int> selected_rows;
        TpccRandomGenerator::SelectUniqueIds(
            scale_params_->num_customers_per_district_ / 10, 1,
            scale_params_->num_customers_per_district_, selected_rows);

        for (int c_id = 1; c_id <= scale_params_->num_customers_per_district_;
            ++c_id) {
          bool bad_credit = (selected_rows.find(c_id) != selected_rows.end());
          // generate customers
          GenerateCustomerRecord(w_id, d_id, c_id, bad_credit, customer_record);
          InsertCustomerRecord(customer_record, customer_record_buf, 0);
          // generate histories
          GenerateHistoryRecord(w_id, d_id, c_id, history_record);
          InsertHistoryRecord(history_record, history_record_buf, 0);
        }

        // generate district new order
        // assume each customer has an order.
        // that is, num_customers_per_district == num_orders_per_district
        int initial_new_order_id = scale_params_->num_customers_per_district_
            - scale_params_->num_new_orders_per_district_ + 1;
        GenerateDistrictNewOrderRecord(w_id, d_id, initial_new_order_id,
                                       district_new_order_record);
        InsertDistrictNewOrderRecord(district_new_order_record,
                                     district_new_order_record_buf, 0);
        for (int o_id = 1; o_id <= scale_params_->num_customers_per_district_;
            ++o_id) {
          int o_ol_cnt = TpccRandomGenerator::GenerateInteger(MIN_OL_CNT,
                                                              MAX_OL_CNT);
          bool is_new_order = (o_id >= initial_new_order_id);
          // generate orders
          GenerateOrderRecord(w_id, d_id, o_id, o_id, o_ol_cnt, is_new_order,
                              order_record);
          InsertOrderRecord(order_record, order_record_buf, 0);

          // generate order lines
          for (int ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
            GenerateOrderLineRecord(w_id, d_id, o_id, ol_number,
                                    scale_params_->num_items_, is_new_order,
                                    order_line_record);
            InsertOrderLineRecord(order_line_record, order_line_record_buf, 0);
          }

          if (is_new_order) {
            // generate new orders
            GenerateNewOrderRecord(w_id, d_id, o_id, new_order_record);
            InsertNewOrderRecord(new_order_record, new_order_record_buf, 0);
          }
        }
      }
      std::unordered_set<int> selected_rows;
      TpccRandomGenerator::SelectUniqueIds(scale_params_->num_items_ / 10, 1,
                                           scale_params_->num_items_,
                                           selected_rows);
      for (int i_id = 1; i_id <= scale_params_->num_items_; ++i_id) {
        bool original = (selected_rows.find(i_id) != selected_rows.end());
        // generate stocks
        GenerateStockRecord(w_id, i_id, original, stock_record);
        InsertStockRecord(stock_record, stock_record_buf, 0);
      }
    }
    delete item_record;
    item_record = NULL;
    delete warehouse_record;
    warehouse_record = NULL;
    delete district_record;
    district_record = NULL;
    delete customer_record;
    customer_record = NULL;
    delete history_record;
    history_record = NULL;
    delete district_new_order_record;
    district_new_order_record = NULL;
    delete order_record;
    order_record = NULL;
    delete new_order_record;
    new_order_record = NULL;
    delete order_line_record;
    order_line_record = NULL;
    delete stock_record;
    stock_record = NULL;

    delete item_record_buf;
    item_record_buf = nullptr;
    delete warehouse_record_buf;
    warehouse_record_buf = nullptr;
    delete district_record_buf;
    district_record_buf = nullptr;
    delete customer_record_buf;
    customer_record_buf = nullptr;
    delete history_record_buf;
    history_record_buf = nullptr;
    delete district_new_order_record_buf;
    district_new_order_record_buf = nullptr;
    delete order_record_buf;
    order_record_buf = nullptr;
    delete new_order_record_buf;
    new_order_record_buf = nullptr;
    delete order_line_record_buf;
    order_line_record_buf = nullptr;
    delete stock_record_buf;
    stock_record_buf = nullptr;

    for (size_t i = 0; i < kTableCount; ++i) {
      storage_manager_->tables_[i]->ReportTableSize();
    }
  }

 private:
  TpccScaleParams* scale_params_;

  void GenerateItemRecord(const int &item_id, const int&w_id, bool original,
                          ItemRecord* record) const {
    record->i_id_ = item_id;
    record->w_id_ = w_id;
    record->i_im_id_ = TpccRandomGenerator::GenerateInteger(MIN_IM, MAX_IM);
    std::string name = TpccRandomGenerator::GenerateAString(MIN_I_NAME,
                                                            MAX_I_NAME);
    memcpy(record->i_name_, name.c_str(), name.size());
    record->i_price_ = TpccRandomGenerator::GenerateFixedPoint(MONEY_DECIMALS,
                                                               MIN_PRICE,
                                                               MAX_PRICE);
    std::string data = TpccRandomGenerator::GenerateAString(MIN_I_DATA,
                                                            MAX_I_DATA);
    memcpy(record->i_data_, data.c_str(), data.size());
  }

  void GenerateWarehouseRecord(const int &w_id, WarehouseRecord* record) const {
    record->w_id_ = w_id;
    std::string name = TpccRandomGenerator::GenerateAString(MIN_NAME, MAX_NAME);
    memcpy(record->w_name_, name.c_str(), name.size());
    TpccRandomGenerator::GenerateAddress(record->w_street_1_,
                                         record->w_street_2_, record->w_city_,
                                         record->w_state_, record->w_zip_);
    record->w_tax_ = TpccRandomGenerator::GenerateTax();
    record->w_ytd_ = INITIAL_W_YTD;
  }

  void GenerateDistrictRecord(const int &d_w_id, const int &d_id,
                              const int &d_next_o_id,
                              DistrictRecord* record) const {
    record->d_id_ = d_id;
    record->d_w_id_ = d_w_id;
    std::string name = TpccRandomGenerator::GenerateAString(MIN_NAME, MAX_NAME);
    memcpy(record->d_name_, name.c_str(), name.size());
    TpccRandomGenerator::GenerateAddress(record->d_street_1_,
                                         record->d_street_2_, record->d_city_,
                                         record->d_state_, record->d_zip_);
    record->d_tax_ = TpccRandomGenerator::GenerateTax();
    record->d_ytd_ = INITIAL_D_YTD;
    record->d_next_o_id_ = d_next_o_id;
  }

  void GenerateCustomerRecord(const int &c_w_id, const int &c_d_id,
                              const int &c_id, bool bad_credit,
                              CustomerRecord* record) const {
    record->c_id_ = c_id;
    record->c_d_id_ = c_d_id;
    record->c_w_id_ = c_w_id;
    std::string first = TpccRandomGenerator::GenerateAString(MIN_FIRST,
                                                             MAX_FIRST);
    memcpy(record->c_first_, first.c_str(), first.size());
    std::string middle = MIDDLE;
    memcpy(record->c_middle_, middle.c_str(), middle.size());
    std::string last;
    if (c_id < 1000) {
      last = TpccRandomGenerator::GenerateLastName(c_id - 1);
    } else {
      last = TpccRandomGenerator::GenerateRandomLastName(
          CUSTOMERS_PER_DISTRICT);
    }
    memcpy(record->c_last_, last.c_str(), last.size());
    TpccRandomGenerator::GenerateAddress(record->c_street_1_,
                                         record->c_street_2_, record->c_city_,
                                         record->c_state_, record->c_zip_);
    std::string phone = TpccRandomGenerator::GenerateNString(PHONE, PHONE);
    memcpy(record->c_phone_, phone.c_str(), phone.size());
    record->c_since_ = TpccRandomGenerator::GenerateCurrentTime();
    std::string credit = GOOD_CREDIT;
    if (bad_credit) {
      credit = BAD_CREDIT;
    }
    memcpy(record->c_credit_, credit.c_str(), credit.size());
    record->c_credit_lim_ = INITIAL_CREDIT_LIM;
    record->c_discount_ = TpccRandomGenerator::GenerateFixedPoint(
        DISCOUNT_DECIMALS, MIN_DISCOUNT, MAX_DISCOUNT);
    record->c_balance_ = INITIAL_BALANCE;
    record->c_ytd_payment_ = INITIAL_YTD_PAYMENT;
    record->c_payment_cnt_ = INITIAL_PAYMENT_CNT;
    record->c_delivery_cnt_ = INITIAL_DELIVERY_CNT;
    std::string data = TpccRandomGenerator::GenerateAString(MIN_C_DATA,
                                                            MAX_C_DATA);
    memcpy(record->c_data_, data.c_str(), data.size());
  }

  void GenerateStockRecord(const int &s_w_id, const int &s_i_id, bool original,
                           StockRecord* record) const {
    record->s_i_id_ = s_i_id;
    record->s_w_id_ = s_w_id;
    record->s_quantity_ = TpccRandomGenerator::GenerateInteger(MIN_QUANTITY,
                                                               MAX_QUANTITY);
    for (int i = 0; i < DISTRICTS_PER_WAREHOUSE; ++i) {
      std::string dist = TpccRandomGenerator::GenerateAString(DIST, DIST);
      memcpy(record->s_dists_[i], dist.c_str(), dist.size());
    }
    record->s_ytd_ = 0;
    record->s_order_cnt_ = 0;
    record->s_remote_cnt_ = 0;
    std::string data = TpccRandomGenerator::GenerateAString(MIN_I_DATA,
                                                            MAX_I_DATA);
    // TODO: FillOriginal() needs rewrite.
    //if (original){
    //	data = FillOriginal(data);
    //}
    memcpy(record->s_data_, data.c_str(), data.size());
  }

  void GenerateOrderRecord(const int &o_w_id, const int &o_d_id,
                           const int &o_id, const int &o_c_id,
                           const int &o_ol_cnt, bool new_order,
                           OrderRecord* record) const {
    record->o_id_ = o_id;
    record->o_c_id_ = o_c_id;
    record->o_d_id_ = o_d_id;
    record->o_w_id_ = o_w_id;
    record->o_entry_d_ = TpccRandomGenerator::GenerateCurrentTime();
    if (new_order) {
      record->o_carrier_id_ = NULL_CARRIER_ID;
    } else {
      record->o_carrier_id_ = TpccRandomGenerator::GenerateInteger(
          MIN_CARRIER_ID, MAX_CARRIER_ID);
    }
    record->o_ol_cnt_ = o_ol_cnt;
    record->o_all_local_ = INITIAL_ALL_LOCAL;
  }

  void GenerateNewOrderRecord(const int &w_id, const int &d_id, const int &o_id,
                              NewOrderRecord* record) const {
    record->w_id_ = w_id;
    record->d_id_ = d_id;
    record->o_id_ = o_id;
  }

  void GenerateOrderLineRecord(const int &ol_w_id, const int &ol_d_id,
                               const int &ol_o_id, const int &ol_number,
                               const int &max_items, bool new_order,
                               OrderLineRecord* record) const {
    record->ol_o_id_ = ol_o_id;
    record->ol_d_id_ = ol_d_id;
    record->ol_w_id_ = ol_w_id;
    record->ol_number_ = ol_number;
    record->ol_i_id_ = TpccRandomGenerator::GenerateInteger(1, max_items);
    record->ol_supply_w_id_ = ol_w_id;
    record->ol_quantity_ = INITIAL_QUANTITY;
    if (new_order) {
      record->ol_delivery_d_ = -1;
      record->ol_amount_ = TpccRandomGenerator::GenerateFixedPoint(
          MONEY_DECIMALS, MIN_AMOUNT, MAX_PRICE * MAX_OL_QUANTITY);
    } else {
      record->ol_delivery_d_ = TpccRandomGenerator::GenerateCurrentTime();
      record->ol_amount_ = 0.0;
    }
    std::string ol_dist_info = TpccRandomGenerator::GenerateAString(DIST, DIST);
    memcpy(record->ol_dist_info_, ol_dist_info.c_str(), ol_dist_info.size());
  }

  void GenerateHistoryRecord(const int &h_c_w_id, const int &h_c_d_id,
                             const int &h_c_id, HistoryRecord* record) const {
    record->h_c_id_ = h_c_id;
    record->h_c_d_id_ = h_c_d_id;
    record->h_c_w_id_ = h_c_w_id;
    record->h_d_id_ = h_c_d_id;
    record->h_w_id_ = h_c_w_id;
    record->h_date_ = TpccRandomGenerator::GenerateCurrentTime();
    record->h_amount_ = INITIAL_AMOUNT;
    std::string data = TpccRandomGenerator::GenerateAString(MIN_DATA, MAX_DATA);
    memcpy(record->h_data_, data.c_str(), data.size());
  }

  DistrictNewOrderRecord* GenerateDistrictNewOrderRecord(
      const int &w_id, const int &d_id, const int &o_id,
      DistrictNewOrderRecord* record) const {
    record->w_id_ = w_id;
    record->d_id_ = d_id;
    record->o_id_ = o_id;
  }

  void InsertItemRecord(ItemRecord* record_ptr, Record *record_buf,
                        size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->i_id_);
    record_buf->SetColumn(1, &record_ptr->i_im_id_);
    record_buf->SetColumn(2, record_ptr->i_name_, 32);
    record_buf->SetColumn(3, &record_ptr->i_price_);
    record_buf->SetColumn(4, record_ptr->i_data_, 64);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetItemPrimaryKey(record_ptr->i_id_, record_ptr->w_id_);
    storage_manager_->tables_[ITEM_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertWarehouseRecord(WarehouseRecord* record_ptr, Record *record_buf,
                             size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->w_id_);
    record_buf->SetColumn(1, record_ptr->w_name_, 16);
    record_buf->SetColumn(2, record_ptr->w_street_1_, 32);
    record_buf->SetColumn(3, record_ptr->w_street_2_, 32);
    record_buf->SetColumn(4, record_ptr->w_city_, 32);
    record_buf->SetColumn(5, record_ptr->w_state_, 2);
    record_buf->SetColumn(6, record_ptr->w_zip_, 9);
    record_buf->SetColumn(7, &record_ptr->w_tax_);
    record_buf->SetColumn(8, &record_ptr->w_ytd_);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey k = GetWarehousePrimaryKey(record_ptr->w_id_);
    storage_manager_->tables_[WAREHOUSE_TABLE_ID]->InsertRecord(
        &k, 1, data_addr, gallocator, thread_id);
  }

  void InsertDistrictRecord(DistrictRecord* record_ptr, Record *record_buf,
                            size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->d_id_);
    record_buf->SetColumn(1, &record_ptr->d_w_id_);
    record_buf->SetColumn(2, record_ptr->d_name_, 16);
    record_buf->SetColumn(3, record_ptr->d_street_1_, 32);
    record_buf->SetColumn(4, record_ptr->d_street_2_, 32);
    record_buf->SetColumn(5, record_ptr->d_city_, 32);
    record_buf->SetColumn(6, record_ptr->d_state_, 2);
    record_buf->SetColumn(7, record_ptr->d_zip_, 9);
    record_buf->SetColumn(8, &record_ptr->d_tax_);
    record_buf->SetColumn(9, &record_ptr->d_ytd_);
    record_buf->SetColumn(10, &record_ptr->d_next_o_id_);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey k = GetDistrictPrimaryKey(record_ptr->d_id_, record_ptr->d_w_id_);
    storage_manager_->tables_[DISTRICT_TABLE_ID]->InsertRecord(
        &k, 1, data_addr, gallocator, thread_id);
  }

  void InsertCustomerRecord(CustomerRecord* record_ptr, Record *record_buf,
                            size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->c_id_);
    record_buf->SetColumn(1, &record_ptr->c_d_id_);
    record_buf->SetColumn(2, &record_ptr->c_w_id_);
    record_buf->SetColumn(3, record_ptr->c_first_, 32);
    record_buf->SetColumn(4, record_ptr->c_middle_, 2);
    record_buf->SetColumn(5, record_ptr->c_last_, 32);
    record_buf->SetColumn(6, record_ptr->c_street_1_, 32);
    record_buf->SetColumn(7, record_ptr->c_street_2_, 32);
    record_buf->SetColumn(8, record_ptr->c_city_, 32);
    record_buf->SetColumn(9, record_ptr->c_state_, 2);
    record_buf->SetColumn(10, record_ptr->c_zip_, 9);
    record_buf->SetColumn(11, record_ptr->c_phone_, 32);
    record_buf->SetColumn(12, &record_ptr->c_since_);
    record_buf->SetColumn(13, record_ptr->c_credit_, 2);
    record_buf->SetColumn(14, &record_ptr->c_credit_lim_);
    record_buf->SetColumn(15, &record_ptr->c_discount_);
    record_buf->SetColumn(16, &record_ptr->c_balance_);
    record_buf->SetColumn(17, &record_ptr->c_ytd_payment_);
    record_buf->SetColumn(18, &record_ptr->c_payment_cnt_);
    record_buf->SetColumn(19, &record_ptr->c_delivery_cnt_);
    record_buf->SetColumn(20, record_ptr->c_data_, 500);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetCustomerPrimaryKey(record_ptr->c_id_, record_ptr->c_d_id_,
                                         record_ptr->c_w_id_);
    storage_manager_->tables_[CUSTOMER_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertStockRecord(StockRecord* record_ptr, Record *record_buf,
                         size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->s_i_id_);
    record_buf->SetColumn(1, &record_ptr->s_w_id_);
    record_buf->SetColumn(2, &record_ptr->s_quantity_);
    for (int i = 3; i < 3 + 10; ++i) {
      record_buf->SetColumn(i, record_ptr->s_dists_[i - 3], 32);
    }
    record_buf->SetColumn(13, &record_ptr->s_ytd_);
    record_buf->SetColumn(14, &record_ptr->s_order_cnt_);
    record_buf->SetColumn(15, &record_ptr->s_remote_cnt_);
    record_buf->SetColumn(16, record_ptr->s_data_, 64);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetStockPrimaryKey(record_ptr->s_i_id_, record_ptr->s_w_id_);
    storage_manager_->tables_[STOCK_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertOrderRecord(OrderRecord* record_ptr, Record *record_buf,
                         size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->o_id_);
    record_buf->SetColumn(1, &record_ptr->o_c_id_);
    record_buf->SetColumn(2, &record_ptr->o_d_id_);
    record_buf->SetColumn(3, &record_ptr->o_w_id_);
    record_buf->SetColumn(4, &record_ptr->o_entry_d_);
    record_buf->SetColumn(5, &record_ptr->o_carrier_id_);
    record_buf->SetColumn(6, &record_ptr->o_ol_cnt_);
    record_buf->SetColumn(7, &record_ptr->o_all_local_);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetOrderPrimaryKey(record_ptr->o_id_, record_ptr->o_d_id_,
                                      record_ptr->o_w_id_);
    storage_manager_->tables_[ORDER_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertNewOrderRecord(NewOrderRecord* record_ptr, Record *record_buf,
                            size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->o_id_);
    record_buf->SetColumn(1, &record_ptr->d_id_);
    record_buf->SetColumn(2, &record_ptr->w_id_);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetNewOrderPrimaryKey(record_ptr->o_id_, record_ptr->d_id_,
                                         record_ptr->w_id_);
    storage_manager_->tables_[NEW_ORDER_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertOrderLineRecord(OrderLineRecord* record_ptr, Record *record_buf,
                             size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->ol_o_id_);
    record_buf->SetColumn(1, &record_ptr->ol_d_id_);
    record_buf->SetColumn(2, &record_ptr->ol_w_id_);
    record_buf->SetColumn(3, &record_ptr->ol_number_);
    record_buf->SetColumn(4, &record_ptr->ol_i_id_);
    record_buf->SetColumn(5, &record_ptr->ol_supply_w_id_);
    record_buf->SetColumn(6, &record_ptr->ol_delivery_d_);
    record_buf->SetColumn(7, &record_ptr->ol_quantity_);
    record_buf->SetColumn(8, &record_ptr->ol_amount_);
    record_buf->SetColumn(9, record_ptr->ol_dist_info_, 32);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetOrderLinePrimaryKey(record_ptr->ol_o_id_,
                                          record_ptr->ol_d_id_,
                                          record_ptr->ol_w_id_,
                                          record_ptr->ol_number_);
    //keys[1] = GetOrderLineSecondaryKey(record_ptr->ol_o_id_, record_ptr->ol_d_id_, record_ptr->ol_w_id_);
    storage_manager_->tables_[ORDER_LINE_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertHistoryRecord(HistoryRecord* record_ptr, Record *record_buf,
                           size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->h_c_id_);
    record_buf->SetColumn(1, &record_ptr->h_c_d_id_);
    record_buf->SetColumn(2, &record_ptr->h_c_w_id_);
    record_buf->SetColumn(3, &record_ptr->h_d_id_);
    record_buf->SetColumn(4, &record_ptr->h_w_id_);
    record_buf->SetColumn(5, &record_ptr->h_date_);
    record_buf->SetColumn(6, &record_ptr->h_amount_);
    record_buf->SetColumn(7, record_ptr->h_data_, 32);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetHistoryPrimaryKey(record_ptr->h_c_id_,
                                        record_ptr->h_d_id_,
                                        record_ptr->h_w_id_);
    storage_manager_->tables_[HISTORY_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }

  void InsertDistrictNewOrderRecord(DistrictNewOrderRecord* record_ptr,
                                    Record *record_buf, size_t thread_id) {
    GAlloc *gallocator = gallocators[thread_id];
    GAddr data_addr = gallocator->Malloc(record_buf->GetSchemaSize());
    record_buf->SetColumn(0, &record_ptr->d_id_);
    record_buf->SetColumn(1, &record_ptr->w_id_);
    record_buf->SetColumn(2, &record_ptr->o_id_);
    record_buf->SetVisible(true);
    record_buf->Serialize(data_addr, gallocator);
    IndexKey key = GetDistrictNewOrderPrimaryKey(record_ptr->d_id_,
                                                 record_ptr->w_id_);
    storage_manager_->tables_[DISTRICT_NEW_ORDER_TABLE_ID]->InsertRecord(
        &key, 1, data_addr, gallocator, thread_id);
  }
};

}
}
#endif
