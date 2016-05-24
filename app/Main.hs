module Main where

import MtLib

generateSupplierTable :: MtSpecificTable
generateSupplierTable = mtSpecificTableFromList
    [("S_SUPPKEY", MtSpecific)
    ,("S_NAME", MtComparable)
    ,("S_ADDRESS", MtComparable)
    ,("S_NATIONKEY", MtComparable)
    ,("S_PHONE", MtTransformable "phoneToUniversal" "phoneFromUniversal")
    ,("S_ACCTBAL", MtTransformable "currencyToUniversal" "currencyFromUniversal")
    ,("S_COMMENT", MtComparable)
    ]

generateLineitemTable :: MtSpecificTable
generateLineitemTable = mtSpecificTableFromList
  [("L_ORDERKEY", MtSpecific)
  ,("L_SUPPKEY", MtSpecific)
  ,("L_LINENUMBER", MtComparable)
  ,("L_QUANTITY", MtComparable)
  ,("L_EXTENDEDPRICE", MtTransformable "currencyToUniversal" "currencyFromUniversal")
  ,("L_DISCOUNT", MtComparable)
  ,("L_TAX", MtComparable)
  ,("L_RETURNFLAG", MtComparable)
  ,("L_LINESTATUS", MtComparable)
  ,("L_SHIPDATE", MtComparable)
  ,("L_COMMITDATE", MtComparable)
  ,("L_RECEIPTDATE", MtComparable)
  ,("L_SHIPINSTRUCT", MtComparable)
  ,("L_SHIPMODE", MtComparable)
  ,("L_COMMENT", MtComparable)
  ]

generateTestSchema :: MtSchemaSpec
generateTestSchema = mtSchemaSpecFromList
    [("REGION", MtGlobalTable)
    ,("NATION", MtGlobalTable)
    ,("SUPPLIER", FromMtSpecificTable generateSupplierTable)
    ,("LINEITEM", FromMtSpecificTable generateLineitemTable)
    ]

main :: IO ()
main = do
    let queries = ["SELECT * FROM SUPPLIER;"
                   ,"SELECT S_NAME FROM SUPPLIER;"
                   ,"SELECT SUPPLIER.S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 42;"
                   ,"SELECT S.S_NAME as SUP_NAME, N.N_NAME as NAT_NAME FROM SUPPLIER S, NATION N WHERE S.S_NATIONKEY = N.N_NATIONKEY"
                   ,"SELECT max(S_ACCT) as NATIONMAX FROM (SELECT avg(S_ACCTBAL) as S_ACCT FROM SUPPLIER GROUP BY S_NATIONKEY)"
                   ,"SELECT sum(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE FROM LINEITEM WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as date)) AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24" -- Q6
                   ]
    let schemaSpec = generateTestSchema
    let client = 1
    let dataset = [1,42]
    
    -- test pretty print
    putStrLn "\nPretty Print with error looks like this:"
    putStrLn $ mtPrettyPrint $ mtParse "SELECT;" -- should print an error message
    putStrLn "\nPretty Print for normal query looks like this:"
    putStrLn $ mtPrettyPrint $ mtParse $ head queries -- should print something correct
    
    -- test parsing and rewrite
    mapM_ (\query -> do
        putStrLn $ "\n" ++ query ++ " parses to:"
        let parsedQuery = mtParse query
        print parsedQuery
        let rewrittenQuery = mtRewrite schemaSpec (client, dataset) query
        putStrLn $ "\n Its rewritten form is:\n  " ++ mtPrettyPrint rewrittenQuery
        )
        queries

    putStrLn "\n\n"
