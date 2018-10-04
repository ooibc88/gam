// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_SOURCE_H__
#define __DATABASE_TPCC_SOURCE_H__

#include "TpccRandomGenerator.h"
#include "BenchmarkSource.h"
#include "TpccTxnParams.h"
#include "TpccKeyGenerator.h"
#include <ctime>

namespace Database {
namespace TpccBenchmark {
class TpccSource : public BenchmarkSource {
 public:
  TpccSource(TpccScaleParams* scale_params, IORedirector* redirector,
             size_t num_txn, size_t source_type, size_t thread_count,
             size_t dist_ratio = 0, size_t node_id = 0)
      : BenchmarkSource(redirector, num_txn, source_type, thread_count,
                        dist_ratio),
        scale_params_(scale_params) {
    node_id_ = node_id;
    // force to access a specific warehouse always
    direct_warehouse_id_ = new int[scale_params_->num_warehouses_];
    for (int i = 0; i < scale_params_->num_warehouses_; ++i) {
      direct_warehouse_id_[i] = (i + 1) % scale_params_->num_warehouses_;
    }
    last_payment_param_.w_id_ = -1;
    total_read_access_count_ = 0;
    total_write_access_count_ = 0;
    total_insert_access_count_ = 0;
  }

 private:
  TpccScaleParams* scale_params_;
  size_t node_id_;
  int* direct_warehouse_id_;
  // for time locality
  bool is_first_;
  std::vector<std::pair<int, int>> new_order_access_items_;
  PaymentParam last_payment_param_;
  uint64_t total_read_access_count_;
  uint64_t total_write_access_count_;
  uint64_t total_insert_access_count_;

  virtual void StartGeneration() {
    //srand(time(nullptr));
    srand(node_id_ * time(nullptr));
    double frequency_weights[5];
    frequency_weights[0] = FREQUENCY_DELIVERY;
    frequency_weights[1] = FREQUENCY_PAYMENT;
    frequency_weights[2] = FREQUENCY_NEW_ORDER;
    frequency_weights[3] = FREQUENCY_ORDER_STATUS;
    frequency_weights[4] = FREQUENCY_STOCK_LEVEL;

    double total = 0;
    for (size_t i = 0; i < 5; ++i) {
      total += frequency_weights[i];
    }
    for (size_t i = 0; i < 5; ++i) {
      frequency_weights[i] = frequency_weights[i] * 1.0 / total * 100;
    }
    for (size_t i = 1; i < 5; ++i) {
      frequency_weights[i] += frequency_weights[i - 1];
    }

    if (source_type_ == RANDOM_SOURCE) {
      ParamBatch *tuples = new ParamBatch(gParamBatchSize);
      for (size_t i = 0; i < num_txn_; ++i) {
        int x = TpccRandomGenerator::GenerateInteger(1, 100);
        if (x <= frequency_weights[0]) {
          DeliveryParam *param = NULL;
          param = GenerateDeliveryParam();
          tuples->push_back(param);
        } else if (x <= frequency_weights[1]) {
          PaymentParam *param = NULL;
          param = GeneratePaymentParam();
          tuples->push_back(param);
        } else if (x <= frequency_weights[2]) {
          NewOrderParam *param = NULL;
          param = GenerateNewOrderParam();
          tuples->push_back(param);
        } else if (x <= frequency_weights[3]) {
          OrderStatusParam *param = NULL;
          param = GenerateOrderStatusParam();
          tuples->push_back(param);
        } else {
          StockLevelParam *param = NULL;
          param = GenerateStockLevelParam();
          tuples->push_back(param);
        }
        if ((i + 1) % gParamBatchSize == 0) {
          redirector_ptr_->PushParameterBatch(tuples);
          tuples = new ParamBatch(gParamBatchSize);
        }
      }
      if (tuples->size() != 0) {
        redirector_ptr_->PushParameterBatch(tuples);
      }
    } else if (source_type_ == PARTITION_SOURCE) {
      size_t thread_id = 0;
      ParamBatch *tuples = new ParamBatch(gParamBatchSize);
      size_t node_num_warehouse = scale_params_->ending_warehouse_
          - scale_params_->starting_warehouse_ + 1;
      for (size_t i = 0; i < num_txn_; ++i) {
        size_t warehouse_id;
        if (node_num_warehouse >= thread_count_) {
          while (1) {
            warehouse_id = TpccRandomGenerator::GenerateWarehouseId(
                scale_params_->starting_warehouse_,
                scale_params_->ending_warehouse_);
            if ((warehouse_id - 1) % thread_count_ == thread_id) {
              break;
            }
          }
        } else {
          warehouse_id = scale_params_->starting_warehouse_
              + (thread_id % node_num_warehouse);
        }
        int x = TpccRandomGenerator::GenerateInteger(1, 100);
        if (x <= frequency_weights[0]) {
          DeliveryParam *param = NULL;
          param = GenerateDeliveryParam(warehouse_id);
          tuples->push_back(param);
        } else if (x <= frequency_weights[1]) {
          PaymentParam *param = NULL;
          param = GeneratePaymentParam(warehouse_id);
          tuples->push_back(param);
        } else if (x <= frequency_weights[2]) {
          NewOrderParam *param = NULL;
          param = GenerateNewOrderParam(warehouse_id);
          tuples->push_back(param);
        } else if (x <= frequency_weights[3]) {
          OrderStatusParam *param = NULL;
          param = GenerateOrderStatusParam(warehouse_id);
          tuples->push_back(param);
        } else {
          StockLevelParam *param = NULL;
          param = GenerateStockLevelParam(warehouse_id);
          tuples->push_back(param);
        }
        if ((i + 1) % gParamBatchSize == 0) {
          redirector_ptr_->PushParameterBatch(tuples);
          tuples = new ParamBatch(gParamBatchSize);
          thread_id = (thread_id + 1) % thread_count_;
        }
      }
      if (tuples->size() != 0) {
        redirector_ptr_->PushParameterBatch(tuples);
      } else {
        delete tuples;
        tuples = NULL;
      }
    }

    std::cout
        << "read/write access count\ntotal_read_access_count\t"
        << total_read_access_count_
        << "\ntotal_write_access_count\t"
        << total_write_access_count_
        << "\ntotal_read_ratio\t"
        << total_read_access_count_ * 1.0
            / (total_write_access_count_ + total_read_access_count_
                + total_insert_access_count_)
        << std::endl;
  }

  void ProfileReadWriteAccess(size_t type) {
    if (type == READ_ONLY)
      total_read_access_count_++;
    else if (type == READ_WRITE)
      total_write_access_count_++;
    else {
      // INSERT
      total_write_access_count_++;
    }
  }

  DeliveryParam* GenerateDeliveryParam(const int &w_id = -1) const {
    DeliveryParam *param = new DeliveryParam();
    if (w_id == -1) {
      param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(
          1, scale_params_->num_warehouses_);
    } else {
      param->w_id_ = w_id;
    }
    param->o_carrier_id_ = TpccRandomGenerator::GenerateInteger(MIN_CARRIER_ID,
                                                                MAX_CARRIER_ID);
    param->ol_delivery_d_ = TpccRandomGenerator::GenerateCurrentTime();
    return param;
  }

  NewOrderParam* GenerateNewOrderParam(const int &w_id = -1) {
    NewOrderParam *param = new NewOrderParam();
    if (w_id == -1) {
      param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(
          1, scale_params_->num_warehouses_);
    } else {
      param->w_id_ = w_id;
    }
    param->d_id_ = TpccRandomGenerator::GenerateDistrictId(
        scale_params_->num_districts_per_warehouse_);
    param->c_id_ = TpccRandomGenerator::GenerateCustomerId(
        scale_params_->num_customers_per_district_);
    param->ol_cnt_ = static_cast<size_t>(TpccRandomGenerator::GenerateInteger(
        MIN_OL_CNT, MAX_OL_CNT));
    param->o_entry_d_ = TpccRandomGenerator::GenerateCurrentTime();
    // generate abort here!
    //bool rollback = (TpccRandomGenerator::GenerateInteger(1, 100) == 1);
    bool rollback = false;
    std::unordered_set<int> exist_items;
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      if (rollback && i == param->ol_cnt_ - 1) {
        param->i_ids_[i] = scale_params_->num_items_ + 1;
      } else {
        while (true) {
          int item_id = TpccRandomGenerator::GenerateItemId(
              scale_params_->num_items_);
          // guarantee the uniqueness of the item.
          if (exist_items.find(item_id) == exist_items.end()) {
            param->i_ids_[i] = item_id;
            exist_items.insert(item_id);
            break;
          }
        }
      }
      bool remote = (TpccRandomGenerator::GenerateInteger(1, 100)
          <= (int) dist_ratio_);
      if (scale_params_->num_warehouses_ > 1 && remote) {
        param->i_w_ids_[i] = TpccRandomGenerator::GenerateIntegerExcluding(
            1, scale_params_->num_warehouses_, param->w_id_);
      } else {
        param->i_w_ids_[i] = param->w_id_;
      }
      param->i_qtys_[i] = TpccRandomGenerator::GenerateInteger(1,
                                                               MAX_OL_QUANTITY);
    }

    // standard read/write
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      param->item_access_type_[i] = READ_ONLY;
      param->stock_access_type_[i] = READ_WRITE;
    }
    param->warehouse_access_type_ = READ_ONLY;
    param->district_access_type_ = READ_WRITE;
    param->customer_access_type_ = READ_ONLY;

    param->new_order_access_type_ = INSERT_ONLY;
    param->order_access_type_ = INSERT_ONLY;
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      param->order_line_access_type_[i] = INSERT_ONLY;
    }

    if (gStandard == false) {
      ModifyNewOrder(param);
    }

    // collect statistics
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      ProfileReadWriteAccess(param->item_access_type_[i]);
      ProfileReadWriteAccess(param->stock_access_type_[i]);
    }
    ProfileReadWriteAccess(param->warehouse_access_type_);
    ProfileReadWriteAccess(param->district_access_type_);
    ProfileReadWriteAccess(param->customer_access_type_);
    ProfileReadWriteAccess(param->new_order_access_type_);
    ProfileReadWriteAccess(param->order_access_type_);
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      ProfileReadWriteAccess(param->order_line_access_type_[i]);
    }
    return param;
  }

  void ModifyNewOrder(NewOrderParam *param) {
    if (gForceRandomAccess) {
      param->c_id_ = TpccRandomGenerator::GenerateCustomerIdUniform(
          scale_params_->num_customers_per_district_);
    }
    // time locality only apply to ITEM, STOCK tables
    std::unordered_map<int, int> exist_items;
    size_t use_last_txn_count = 0;
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      bool use_last_txn = (TpccRandomGenerator::GenerateInteger(1, 100)
          <= (int) gTimeLocality);
      if (use_last_txn && new_order_access_items_.size() > use_last_txn_count) {
        // if decide to use items from last txn and last txn still has unvisisted items, 
        // then randomly pick up one from last txn access set
        while (true) {
          int idx = TpccRandomGenerator::GenerateInteger(
              1, new_order_access_items_.size()) - 1;
          int item_id = new_order_access_items_.at(idx).first;
          int w_id = new_order_access_items_.at(idx).second;
          if (exist_items.find(item_id) == exist_items.end()) {
            param->i_ids_[i] = item_id;
            param->i_w_ids_[i] = w_id;
            break;
          }
        }
        ++use_last_txn_count;
      } else {
        if (gForceRandomAccess) {
          // if decide not to use items from last txn, but random access is required, still need to regenerate the item
          while (true) {
            int item_id = TpccRandomGenerator::GenerateItemIdUniform(
                scale_params_->num_items_);
            // guarantee the uniqueness of the item.
            if (exist_items.find(item_id) == exist_items.end()) {
              param->i_ids_[i] = item_id;
              break;
            }
          }
        }
        //still may possibly use item from last txn
        for (auto &entry : new_order_access_items_) {
          if (entry.first == param->i_ids_[i]) {
            ++use_last_txn_count;
            break;
          }
        }
      }
      while (exist_items.find(param->i_ids_[i]) != exist_items.end()) {
        int item_id = TpccRandomGenerator::GenerateItemId(
            scale_params_->num_items_);
        param->i_ids_[i] = item_id;
      }
      // in any case need to maintain the visited items set
      exist_items.insert(std::make_pair(param->i_ids_[i], param->i_w_ids_[i]));
    }

    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      for (size_t j = i + 1; j < param->ol_cnt_; ++j) {
        assert (!(param->i_ids_[i] == param->i_ids_[j]
            && param->i_w_ids_[i] == param->i_w_ids_[j]));
      }
    }

    // maintain item set of this txn
    new_order_access_items_.clear();
    for (auto &entry : exist_items) {
      new_order_access_items_.push_back(
          std::make_pair(entry.first, entry.second));
    }
    assert(new_order_access_items_.size() == exist_items.size());

    // read ratio only turns those writes in standard into read
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      param->stock_access_type_[i] =
          (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
              READ_ONLY : READ_WRITE;
    }
    param->district_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;

    param->new_order_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
    param->order_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
    for (size_t i = 0; i < param->ol_cnt_; ++i) {
      param->order_line_access_type_[i] =
          (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
              READ_ONLY : READ_WRITE;
    }
  }

  PaymentParam* GeneratePaymentParam(const int &w_id = -1) {
    PaymentParam *param = new PaymentParam();
    // Return parameters for PAYMENT
    int x = TpccRandomGenerator::GenerateInteger(1, 100);
    int y = TpccRandomGenerator::GenerateInteger(1, 100);
    if (w_id == -1) {
      param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(
          1, scale_params_->num_warehouses_);
    } else {
      param->w_id_ = w_id;
    }
    param->d_id_ = TpccRandomGenerator::GenerateDistrictId(
        scale_params_->num_districts_per_warehouse_);
    param->c_w_id_ = -1;
    param->c_d_id_ = -1;
    param->c_id_ = -1;
    //param->c_last_ = "";
    param->h_amount_ = TpccRandomGenerator::GenerateFixedPoint(2, MIN_PAYMENT,
                                                               MAX_PAYMENT);
    param->h_date_ = TpccRandomGenerator::GenerateCurrentTime();

    bool remote = (TpccRandomGenerator::GenerateInteger(1, 100)
        <= (int) dist_ratio_);
    // 85%(by standard): paying through own warehouse (or there is only 1 warehouse)
    if (scale_params_->num_warehouses_ == 1 || remote == false) {
      param->c_w_id_ = param->w_id_;
      param->c_d_id_ = param->d_id_;
    }
    // 15%(by standard): paying through another warehouse
    else {
      // select in range [1, num_warehouses] excluding w_id
      param->c_w_id_ = TpccRandomGenerator::GenerateIntegerExcluding(
          1, scale_params_->num_warehouses_, param->w_id_);
      param->c_d_id_ = TpccRandomGenerator::GenerateDistrictId(
          scale_params_->num_districts_per_warehouse_);
    }

    // currently, index cannot support lookup by name, forcely use lookup by customer id
    y = 100;
    // 60%: payment by last name
    if (y <= 60) {
      param->c_last_ = TpccRandomGenerator::GenerateRandomLastName(
          scale_params_->num_customers_per_district_);
    }
    // 40%: payment by id
    else {
      param->c_id_ = TpccRandomGenerator::GenerateCustomerId(
          scale_params_->num_customers_per_district_);
    }

    // standard read/write
    param->warehouse_access_type_ = READ_WRITE;
    param->district_access_type_ = READ_WRITE;
    param->customer_access_type_ = READ_WRITE;
    param->history_access_type_ = INSERT_ONLY;

    if (gStandard == false) {
      ModifyPayment(param);
    }

    ProfileReadWriteAccess(param->warehouse_access_type_);
    ProfileReadWriteAccess(param->district_access_type_);
    ProfileReadWriteAccess(param->customer_access_type_);
    ProfileReadWriteAccess(param->history_access_type_);

    return param;
  }

  void ModifyPayment(PaymentParam *param) {
    if (last_payment_param_.w_id_ >= 0) {
      // not the first payment
      bool use_last_txn = (TpccRandomGenerator::GenerateInteger(1, 100)
          <= (int) gTimeLocality);
      if (use_last_txn) {
        // time_locality will entirely use last txn
        memcpy(param, &last_payment_param_, sizeof(PaymentParam));
      } else if (gForceRandomAccess) {
        param->c_id_ = TpccRandomGenerator::GenerateCustomerIdUniform(
            scale_params_->num_customers_per_district_);
      }
    }
    memcpy(&last_payment_param_, param, sizeof(PaymentParam));

    param->warehouse_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
    param->district_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
    param->customer_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
    param->history_access_type_ =
        (TpccRandomGenerator::GenerateInteger(1, 100) <= gReadRatio) ?
            READ_ONLY : READ_WRITE;
  }

  OrderStatusParam* GenerateOrderStatusParam(const int &w_id = -1) const {
    OrderStatusParam *param = new OrderStatusParam();
    int y = TpccRandomGenerator::GenerateInteger(1, 100);
    if (w_id == -1) {
      param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(
          1, scale_params_->num_warehouses_);
    } else {
      param->w_id_ = w_id;
    }
    param->d_id_ = TpccRandomGenerator::GenerateDistrictId(
        scale_params_->num_districts_per_warehouse_);

    // currently, index cannot support lookup by name, forcely use lookup by customer id
    y = 100;
    if (y <= 60) {
      // check by last name
      param->c_last_ = TpccRandomGenerator::GenerateRandomLastName(
          scale_params_->num_customers_per_district_);
    } else {
      // check order status by id
      param->c_id_ = TpccRandomGenerator::GenerateCustomerId(
          scale_params_->num_customers_per_district_);
    }
    return param;
  }

  StockLevelParam* GenerateStockLevelParam(const int &w_id = -1) const {
    StockLevelParam *param = new StockLevelParam();
    if (w_id == -1) {
      param->w_id_ = TpccRandomGenerator::GenerateWarehouseId(
          1, scale_params_->num_warehouses_);
    } else {
      param->w_id_ = w_id;
    }
    param->d_id_ = TpccRandomGenerator::GenerateDistrictId(
        scale_params_->num_districts_per_warehouse_);
    return param;
  }

};

}
}

#endif
