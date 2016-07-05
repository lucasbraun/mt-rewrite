module TPCHSchema (
    generateTPCHSchema
) where

import MtLib

generateCustomerTable :: MtSpecificTable
generateCustomerTable = mtSpecificTableFromList
    [("C_custkey"   , MtSpecific)
    ,("C_name"      , MtComparable)
    ,("C_address"   , MtComparable)
    ,("C_nationkey" , MtComparable)
    ,("C_phone"     , MtConvertible "phoneToUniversal" "phoneFromUniversal")
    ,("C_acctbal"   , MtConvertible "currencyToUniversal" "currencyFromUniversal")
    ,("C_mktsegment", MtComparable)
    ,("C_comment"   , MtComparable)
    ]

generateOrdersTable :: MtSpecificTable
generateOrdersTable = mtSpecificTableFromList
    [("O_orderkey"      , MtSpecific)
    ,("O_custkey"       , MtSpecific)
    ,("O_orderstatus"   , MtComparable)
    ,("O_totalprice"    , MtConvertible "currencyToUniversal" "currencyFromUniversal")
    ,("O_orderdate"     , MtComparable)
    ,("O_orderpriority" , MtComparable)
    ,("O_clerk"         , MtComparable)
    ,("O_shippriority"  , MtComparable)
    ,("O_comment"       , MtComparable)
    ]

generateLineitemTable :: MtSpecificTable
generateLineitemTable = mtSpecificTableFromList
  [("L_orderkey"        , MtSpecific)
  ,("L_partkey"         , MtComparable)
  ,("L_suppkey"         , MtComparable)
  ,("L_linenumber"      , MtComparable)
  ,("L_quantity"        , MtComparable)
  ,("L_extendedprice"   , MtConvertible "currencyToUniversal" "currencyFromUniversal")
  ,("L_discount"        , MtComparable)
  ,("L_tax"             , MtComparable)
  ,("L_returnflag"      , MtComparable)
  ,("L_linestatus"      , MtComparable)
  ,("L_shipdate"        , MtComparable)
  ,("L_commitdate"      , MtComparable)
  ,("L_receiptdate"     , MtComparable)
  ,("L_shipinstruct"    , MtComparable)
  ,("L_shipmode"        , MtComparable)
  ,("L_comment"         , MtComparable)
  ]

generateTPCHSchema :: MtSchemaSpec
generateTPCHSchema = mtSchemaSpecFromList
    [("Region", MtGlobalTable)
    ,("Nation", MtGlobalTable)
    ,("Part", MtGlobalTable)
    ,("Supplier", MtGlobalTable)
    ,("Partsupp", MtGlobalTable)
    ,("Customer", FromMtSpecificTable generateCustomerTable)
    ,("Orders", FromMtSpecificTable generateOrdersTable)
    ,("Lineitem", FromMtSpecificTable generateLineitemTable)
    ]

