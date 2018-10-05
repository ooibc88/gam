// NOTICE: this file is adapted from Cavalia
#ifndef __DATABASE_TPCC_CONSTANTS_H__
#define __DATABASE_TPCC_CONSTANTS_H__

#include <string>

namespace Database {
namespace TpccBenchmark {
/********************procedure **********************/
enum TupleType
  : size_t {
    DELIVERY,
  NEW_ORDER,
  PAYMENT,
  ORDER_STATUS,
  STOCK_LEVEL,
  kProcedureCount
};

/******************** table **********************/
enum TableType
  : size_t {
    ITEM_TABLE_ID,
  STOCK_TABLE_ID,
  WAREHOUSE_TABLE_ID,
  DISTRICT_TABLE_ID,
  DISTRICT_NEW_ORDER_TABLE_ID,
  NEW_ORDER_TABLE_ID,
  ORDER_TABLE_ID,
  ORDER_LINE_TABLE_ID,
  CUSTOMER_TABLE_ID,
  HISTORY_TABLE_ID,
  kTableCount
};
const int FREQUENCY_DELIVERY = 0;  //4;
const int FREQUENCY_PAYMENT = 43;
const int FREQUENCY_NEW_ORDER = 45;
const int FREQUENCY_ORDER_STATUS = 0;  //4;
const int FREQUENCY_STOCK_LEVEL = 0;  //4;

/********** generate HashKey ************************/
const size_t kWarehouseBits = 58;
const size_t kTableIdLowBits = 54;
const size_t kTableIdHighBits = 6;
const size_t kDistrictLowBits = 32;
const size_t kOrderIdLowBits = 4;

/******************** source *************************/
const int MONEY_DECIMALS = 2;

//Item constants
//const int NUM_ITEMS = 1000; // origin : 100000
const int NUM_ITEMS = 100000;
const int MIN_IM = 1;
const int MAX_IM = 10000;
const double MIN_PRICE = 1.00;
const double MAX_PRICE = 100.00;
const int MIN_I_NAME = 14;
const int MAX_I_NAME = 24;
const int MIN_I_DATA = 26;
const int MAX_I_DATA = 50;

//Warehouse constants
const double MIN_TAX = 0;
const double MAX_TAX = 0.2000;
const int TAX_DECIMALS = 4;
const double INITIAL_W_YTD = 300000.00;
const int MIN_NAME = 6;
const int MAX_NAME = 10;
const int MIN_STREET = 10;
const int MAX_STREET = 20;
const int MIN_CITY = 10;
const int MAX_CITY = 20;
const int STATE = 2;
const int ZIP_LENGTH = 9;
const std::string ZIP_SUFFIX = "11111";

//Stock constants
const int MIN_QUANTITY = 10;
const int MAX_QUANTITY = 100;
const int DIST = 24;
//const int STOCK_PER_WAREHOUSE = 1000; //origin : 100000
const int STOCK_PER_WAREHOUSE = 100000;

//District constants
const int DISTRICTS_PER_WAREHOUSE = 10;
const double INITIAL_D_YTD = 30000.00;  //different from Warehouse
//const int INITIAL_NEXT_O_ID = 31; // origin : 3001
//const int INITIAL_NEXT_O_ID = 3001;

//Customer constants
//const int CUSTOMERS_PER_DISTRICT = 30; // origin : 3000
const int CUSTOMERS_PER_DISTRICT = 3000;
const double INITIAL_CREDIT_LIM = 50000.00;
const double MIN_DISCOUNT = 0.0000;
const double MAX_DISCOUNT = 0.5000;
const int DISCOUNT_DECIMALS = 4;
const double INITIAL_BALANCE = -10.00;
const double INITIAL_YTD_PAYMENT = 10.00;
const int INITIAL_PAYMENT_CNT = 1;
const int INITIAL_DELIVERY_CNT = 0;
const int MIN_FIRST = 6;
const int MAX_FIRST = 10;
const std::string MIDDLE = "OE";
const int PHONE = 16;
const int MIN_C_DATA = 300;
const int MAX_C_DATA = 500;
const std::string GOOD_CREDIT = "GC";
const std::string BAD_CREDIT = "BC";

//Order constants
const int MIN_CARRIER_ID = 1;
const int MAX_CARRIER_ID = 10;
//HACK: This is not strictly correct, but it works
const int NULL_CARRIER_ID = 0;
//o_id < than this value, carrier != null, >= -> carrier == null
const int NULL_CARRIER_LOWER_BOUND = 2101;
const int MIN_OL_CNT = 5;
const int MAX_OL_CNT = 15;
const bool INITIAL_ALL_LOCAL = true;
//const int INITIAL_ORDERS_PER_DISTRICT = 30; // origin : 3000
const int INITIAL_ORDERS_PER_DISTRICT = 3000;

//Used to generate new order transactions
const int MAX_OL_QUANTITY = 10;

//Order line constants
const int INITIAL_QUANTITY = 5;
const double MIN_AMOUNT = 0.01;

//History constants
const int MIN_DATA = 12;
const int MAX_DATA = 24;
const double INITIAL_AMOUNT = 10.00;

//New order constants
//const int INITIAL_NEW_ORDERS_PER_DISTRICT = 9; // origin : 900
const int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

//TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
const std::string INVALID_ITEM_MESSAGE = "Item number is not valid";

//Used to generate stock level transactions
const int MIN_STOCK_LEVEL_THRESHOLD = 10;
const int MAX_STOCK_LEVEL_THRESHOLD = 20;

//Used to generate payment transactions
const double MIN_PAYMENT = 1.0;
const double MAX_PAYMENT = 5000.0;

//Indicates "brand" items and stock in i_data and s_data.
const std::string ORIGINAL_STRING = "ORIGINAL";

}
}

#endif
