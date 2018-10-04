#include "TpccPopulator.h"
#include "TpccInitiator.h"
#include "TpccParams.h"
#include "TpccKeyGenerator.h"
#include "TpccConstants.h"
#include "Meta.h"
#include "BenchmarkArguments.h"
#include "ClusterHelper.h"
#include "ClusterSync.h"
#include <iostream>

using namespace Database::TpccBenchmark;
using namespace Database;

void Check(GAddr storage_addr);

int main(int argc, char* argv[]) {
  ArgumentsParser(argc, argv);

  std::string my_host_name = ClusterHelper::GetLocalHostName();
  ClusterConfig config(my_host_name, port, config_filename);
  ClusterSync synchronizer(&config);
  FillScaleParams(config);
  PrintScaleParams();

  TpccInitiator initiator(gThreadCount, &config);
  // initialize GAM storage layer
  initiator.InitGAllocator();
  synchronizer.Fence();
  // initialize benchmark data
  GAddr storage_addr = initiator.InitStorage();
  synchronizer.MasterBroadcast<GAddr>(&storage_addr); 
  std::cout << "storage_addr=" << storage_addr << std::endl;
  StorageManager storage_manager;
  storage_manager.Deserialize(storage_addr, default_gallocator);

  // populate database
  INIT_PROFILE_TIME(gThreadCount);
  TpccPopulator populator(&storage_manager, &tpcc_scale_params);
  populator.Start();
  REPORT_PROFILE_TIME
  (gThreadCount);

  // check semantic consistency of database
  synchronizer.Fence();
  Check(storage_addr);
  synchronizer.Fence();

  std::cout << "prepare to exit..." << std::endl;
  synchronizer.Fence();
  std::cout << "over.." << std::endl;
  return 0;
}

void CheckSchema(RecordSchema *schema) {
  RecordSchema *expect = nullptr;
  switch(schema->GetTableId()) {
    case ITEM_TABLE_ID:
      TpccInitiator::InitItemSchema(expect, default_gallocator); 
      break;
    case WAREHOUSE_TABLE_ID:
      TpccInitiator::InitWarehouseSchema(expect, default_gallocator);
      break;
    case DISTRICT_TABLE_ID:
      TpccInitiator::InitDistrictSchema(expect, default_gallocator);
      break;
    case CUSTOMER_TABLE_ID:
      TpccInitiator::InitCustomerSchema(expect, default_gallocator);
      break;
    case ORDER_TABLE_ID:
      TpccInitiator::InitOrderSchema(expect, default_gallocator);
      break;
    case DISTRICT_NEW_ORDER_TABLE_ID:
      TpccInitiator::InitDistrictNewOrderSchema(expect, default_gallocator);
      break;
    case NEW_ORDER_TABLE_ID:
      TpccInitiator::InitNewOrderSchema(expect, default_gallocator);
      break;
    case ORDER_LINE_TABLE_ID:
      TpccInitiator::InitOrderLineSchema(expect, default_gallocator);
      break;
    case HISTORY_TABLE_ID:
      TpccInitiator::InitHistorySchema(expect, default_gallocator);
      break;
    case STOCK_TABLE_ID:
      TpccInitiator::InitStockSchema(expect, default_gallocator);
      break;
    default:
      epicAssert(false);
      break;
  }
  epicAssert(schema->GetColumnCount() == expect->GetColumnCount());
  epicAssert(schema->GetSchemaSize() == expect->GetSchemaSize());
  for (size_t col_id = 0; col_id < schema->GetColumnCount(); ++col_id) {
    epicAssert(schema->GetColumnType(col_id) == 
        expect->GetColumnType(col_id));
    epicAssert(schema->GetColumnOffset(col_id) ==
        expect->GetColumnOffset(col_id));
    epicAssert(schema->GetColumnSize(col_id) ==
        expect->GetColumnSize(col_id));
  }
  delete expect;
  expect = nullptr;
}

void Check(GAddr storage_addr) {
  std::cout << "start check...." << std::endl;
  StorageManager storage_manager;
  storage_manager.Deserialize(storage_addr, default_gallocator);
  
  // Check RecordSchema
  epicAssert(storage_manager.GetTableCount() == kTableCount);
  for (size_t table_id = 0; table_id < kTableCount; ++table_id) {
    Table *table = storage_manager.tables_[table_id];
    epicAssert(table->GetTableId() == table_id);
    epicAssert(table->GetSecondaryCount() == 0);
    CheckSchema(table->GetSchema());
  }

  // Check populate
  for (int i_id = 1; i_id <= tpcc_scale_params.num_items_; ++i_id) {
    for (int w_id = tpcc_scale_params.starting_warehouse_;
        w_id <= tpcc_scale_params.ending_warehouse_; ++w_id) {
      GAddr ret = storage_manager.tables_[ITEM_TABLE_ID]->SearchRecord(
          GetItemPrimaryKey(i_id, w_id), gallocators[0], 0);
      assert(ret);
      Record *item_record = new Record(
          storage_manager.tables_[ITEM_TABLE_ID]->GetSchema());
      item_record->Deserialize(ret, gallocators[0]);
      int actual_i_id = -1;
      item_record->GetColumn(0, &actual_i_id);
      assert(actual_i_id == i_id);
      int i_im_id = -1;
      item_record->GetColumn(1, &i_im_id);
      assert(MIN_IM <= i_im_id && i_im_id <= MAX_IM);
      delete item_record;
      item_record = nullptr;
    }
  }
  for (int w_id = 1; w_id <= tpcc_scale_params.num_warehouses_; ++w_id) {
    GAddr ret = storage_manager.tables_[WAREHOUSE_TABLE_ID]->SearchRecord(
        GetWarehousePrimaryKey(w_id), gallocators[0], 0);
    assert(ret);
    Record *warehouse_record = new Record(
        storage_manager.tables_[WAREHOUSE_TABLE_ID]->GetSchema()
        );
    warehouse_record->Deserialize(ret, gallocators[0]);
    int act_w_id = -1;
    warehouse_record->GetColumn(0, &act_w_id);
    assert(act_w_id == w_id);
    delete warehouse_record;
    warehouse_record = nullptr;

    for (int d_id = 1; d_id <= tpcc_scale_params.num_districts_per_warehouse_;
        ++d_id) {
      ret = storage_manager.tables_[DISTRICT_TABLE_ID]->SearchRecord(
          GetDistrictPrimaryKey(d_id, w_id), gallocators[0], 0);
      assert(ret);
      std::cout << "d_id=" << d_id << "w_id=" << w_id << std::endl;
      Record *district_record = new Record(
          storage_manager.tables_[DISTRICT_TABLE_ID]->GetSchema()
          );
      district_record->Deserialize(ret, gallocators[0]);
      int act_d_id = -1;
      district_record->GetColumn(0, &act_d_id);
      assert(act_d_id == d_id);
      int act_w_id = -1;
      district_record->GetColumn(1, &act_w_id);
      assert(act_w_id == w_id);
      int act_d_next_o_id = -1;
      district_record->GetColumn(10, &act_d_next_o_id);
      assert(act_d_next_o_id == tpcc_scale_params.num_customers_per_district_ + 1);
      delete district_record;
      district_record = nullptr;

      for (int c_id = 1; c_id <= tpcc_scale_params.num_customers_per_district_;
          ++c_id) {
        ret = storage_manager.tables_[CUSTOMER_TABLE_ID]->SearchRecord(
            GetCustomerPrimaryKey(c_id, d_id, w_id), gallocators[0], 0);
        assert(ret);
        Record *customer_record = new Record(
            storage_manager.tables_[CUSTOMER_TABLE_ID]->GetSchema()
            );
        customer_record->Deserialize(ret, gallocators[0]);
        int act_c_id = -1;
        customer_record->GetColumn(0, &act_c_id);
        assert(act_c_id == c_id);
        int act_d_id = -1;
        customer_record->GetColumn(1, &act_d_id);
        assert(act_d_id == d_id);
        int act_w_id = -1;
        customer_record->GetColumn(2, &act_w_id);
        assert(act_w_id == w_id);
        delete customer_record;
        customer_record = nullptr;
        // skip HISTORY
      }

      ret = storage_manager.tables_[DISTRICT_NEW_ORDER_TABLE_ID]->SearchRecord(
          GetDistrictNewOrderPrimaryKey(d_id, w_id), gallocators[0], 0);
      assert(ret);
      Record *district_new_order_record = new Record(
          storage_manager.tables_[DISTRICT_NEW_ORDER_TABLE_ID]->GetSchema()
          );
      district_new_order_record->Deserialize(ret, gallocators[0]);
      act_d_id = -1;
      district_new_order_record->GetColumn(0, &act_d_id);
      assert(act_d_id == d_id);
      act_w_id = -1;
      district_new_order_record->GetColumn(1, &act_w_id);
      assert(act_w_id == w_id);
      int act_o_id = -1;
      district_new_order_record->GetColumn(2, &act_o_id);
      assert(act_o_id == tpcc_scale_params.num_customers_per_district_ 
          - tpcc_scale_params.num_new_orders_per_district_ + 1);
      delete district_new_order_record;
      district_new_order_record = nullptr;

      int initial_new_order_id = tpcc_scale_params.num_customers_per_district_
          - tpcc_scale_params.num_new_orders_per_district_ + 1;
      for (int o_id = 1; o_id <= tpcc_scale_params.num_customers_per_district_;
          ++o_id) {
        ret = storage_manager.tables_[ORDER_TABLE_ID]->SearchRecord(
            GetOrderPrimaryKey(o_id, d_id, w_id), gallocators[0], 0);
        assert(ret);
        Record *order_record = new Record(
            storage_manager.tables_[ORDER_TABLE_ID]->GetSchema()
            );
        order_record->Deserialize(ret, gallocators[0]);
        int act_o_id = -1;
        order_record->GetColumn(0, &act_o_id);
        assert(act_o_id == o_id);
        int act_c_id = -1;
        order_record->GetColumn(1, &act_c_id);
        assert(act_c_id == o_id);
        int act_d_id = -1;
        order_record->GetColumn(2, &act_d_id);
        assert(act_d_id == d_id);
        int act_w_id = -1;
        order_record->GetColumn(3, &act_w_id);
        assert(act_w_id == w_id);
        int act_carrier_id = -1;
        order_record->GetColumn(5, &act_carrier_id);
        assert(act_carrier_id == NULL_CARRIER_ID || 
            (MIN_CARRIER_ID <= act_carrier_id && act_carrier_id <= MAX_CARRIER_ID));
        int act_all_data = -1;
        order_record->GetColumn(7, &act_all_data);
        assert(act_all_data == INITIAL_ALL_LOCAL);
        delete order_record;
        order_record = nullptr;

        // skip ORDER_LINE
        if (o_id >= initial_new_order_id) {
          ret = storage_manager.tables_[NEW_ORDER_TABLE_ID]->SearchRecord(
              GetNewOrderPrimaryKey(o_id, d_id, w_id), gallocators[0], 0);
          assert(ret);
          Record *new_order_record = new Record(
              storage_manager.tables_[NEW_ORDER_TABLE_ID]->GetSchema()
              );
          new_order_record->Deserialize(ret, gallocators[0]);
          int act_o_id = -1;
          new_order_record->GetColumn(0, &act_o_id);
          assert(act_o_id == o_id);
          int act_d_id = -1;
          new_order_record->GetColumn(1, &act_d_id);
          assert(act_d_id == d_id);
          int act_w_id = -1;
          new_order_record->GetColumn(2, &act_w_id);
          assert(act_w_id == w_id);
          delete new_order_record;
          new_order_record = nullptr;
        }
      }
    }
    std::cout << "w_id=" << w_id << ",start check stock" << std::endl;
    for (int i_id = 1; i_id <= tpcc_scale_params.num_items_; ++i_id) {
      ret = storage_manager.tables_[STOCK_TABLE_ID]->SearchRecord(
          GetStockPrimaryKey(i_id, w_id), gallocators[0], 0);
      assert(ret);
      Record *stock_record = new Record(
          storage_manager.tables_[STOCK_TABLE_ID]->GetSchema()
          );
      stock_record->Deserialize(ret, gallocators[0]);
      int act_i_id = -1;
      stock_record->GetColumn(0, &act_i_id);
      assert(act_i_id == i_id);
      int act_w_id = -1;
      stock_record->GetColumn(1, &act_w_id);
      assert(act_w_id == w_id);
      int act_quantity = -1;
      stock_record->GetColumn(2, &act_quantity);
      assert(MIN_QUANTITY <= act_quantity && 
          act_quantity <= MAX_QUANTITY);
      int act_ytd = -1;
      stock_record->GetColumn(13, &act_ytd);
      assert(act_ytd == 0);
      int act_order_cnt = -1;
      stock_record->GetColumn(14, &act_order_cnt);
      assert(act_order_cnt == 0);
      int act_remote_cnt = -1;
      stock_record->GetColumn(15, &act_remote_cnt);
      assert(act_remote_cnt == 0);
      delete stock_record;
      stock_record = nullptr;
    }
  }
}
