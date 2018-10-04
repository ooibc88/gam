#ifndef __DATABASE_TPCC_PARAMS_H__
#define __DATABASE_TPCC_PARAMS_H__

#include "Meta.h"
#include "TpccConstants.h"
#include "ClusterConfig.h"
#include "BenchmarkArguments.h"

#include <cassert>

namespace Database {
namespace TpccBenchmark {

struct TpccScaleParams {
  int num_warehouses_;
  int starting_warehouse_;
  int ending_warehouse_;
  int partition_id_;
  double scale_factor_;
  int num_items_;
  int num_districts_per_warehouse_;
  int num_customers_per_district_;
  int num_new_orders_per_district_;
};
// Global
TpccScaleParams tpcc_scale_params;

static void FillScaleParams(ClusterConfig& config) {
  assert(factor_count == 2);
  tpcc_scale_params.num_warehouses_ = (int) scale_factors[0];
  tpcc_scale_params.partition_id_ = config.GetMyPartitionId();
  int partition_num = config.GetPartitionNum();
  int num_wh_per_par = (tpcc_scale_params.num_warehouses_ + partition_num - 1)
      / partition_num;
  tpcc_scale_params.starting_warehouse_ = 1
      + num_wh_per_par * tpcc_scale_params.partition_id_;
  tpcc_scale_params.ending_warehouse_ = tpcc_scale_params.starting_warehouse_
      + num_wh_per_par - 1;
  if (tpcc_scale_params.ending_warehouse_ > tpcc_scale_params.num_warehouses_) {
    tpcc_scale_params.ending_warehouse_ = tpcc_scale_params.num_warehouses_;
  }
  assert(
      tpcc_scale_params.starting_warehouse_
          <= tpcc_scale_params.ending_warehouse_);
  tpcc_scale_params.scale_factor_ = scale_factors[1];
  tpcc_scale_params.num_items_ = static_cast<int>(NUM_ITEMS
      / tpcc_scale_params.scale_factor_);
  tpcc_scale_params.num_districts_per_warehouse_ = DISTRICTS_PER_WAREHOUSE;
  tpcc_scale_params.num_customers_per_district_ =
      static_cast<int>(CUSTOMERS_PER_DISTRICT / tpcc_scale_params.scale_factor_);
  tpcc_scale_params.num_new_orders_per_district_ =
      static_cast<int>(INITIAL_NEW_ORDERS_PER_DISTRICT
          / tpcc_scale_params.scale_factor_);
}

static void PrintScaleParams() {
  std::cout << "============= tpcc_scale_params ===========" << std::endl;
  std::cout << "num_warehouses=" << tpcc_scale_params.num_warehouses_
            << ",starting_warehouse_=" << tpcc_scale_params.starting_warehouse_
            << ",ending_warehouse_=" << tpcc_scale_params.ending_warehouse_
            << std::endl;
  std::cout << "partition_id_" << tpcc_scale_params.partition_id_
            << "\nscale_factor_=" << tpcc_scale_params.scale_factor_
            << "\nnum_items_=" << tpcc_scale_params.num_items_
            << "\nnum_districts_per_warehouse_="
            << tpcc_scale_params.num_districts_per_warehouse_
            << "\nnum_customers_per_district_="
            << tpcc_scale_params.num_customers_per_district_
            << "\nnum_new_orders_per_district_="
            << tpcc_scale_params.num_new_orders_per_district_ << std::endl;
  std::cout << "dist_ratio=" << dist_ratio << ", gStandard=" << gStandard
            << ", gForceRandomAccess=" << gForceRandomAccess
            << ", gTimeLocality=" << gTimeLocality << ", gReadRatio="
            << gReadRatio << std::endl;
  std::cout << "============= end ===========" << std::endl;
}

}
}

#endif
