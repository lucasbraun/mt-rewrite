module Main where

import MtLib

generateCustomerTable :: MtSpecificTable
generateCustomerTable = mtSpecificTableFromList
    [("C_custkey"   , MtSpecific)
    ,("C_name"      , MtComparable)
    ,("C_address"   , MtComparable)
    ,("C_nationkey" , MtComparable)
    ,("C_phone"     , MtTransformable "phoneToUniversal" "phoneFromUniversal")
    ,("C_acctbal"   , MtTransformable "currencyToUniversal" "currencyFromUniversal")
    ,("C_mktsegment", MtComparable)
    ,("C_comment"   , MtComparable)
    ]

generateOrdersTable :: MtSpecificTable
generateOrdersTable = mtSpecificTableFromList
    [("O_orderkey"      , MtSpecific)
    ,("O_custkey"       , MtSpecific)
    ,("O_orderstatus"   , MtComparable)
    ,("O_totalprice"    , MtTransformable "currencyToUniversal" "currencyFromUniversal")
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
  ,("L_extendedprice"   , MtTransformable "currencyToUniversal" "currencyFromUniversal")
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

generateTestSchema :: MtSchemaSpec
generateTestSchema = mtSchemaSpecFromList
    [("Region", MtGlobalTable)
    ,("Nation", MtGlobalTable)
    ,("Part", MtGlobalTable)
    ,("Supplier", MtGlobalTable)
    ,("Partsupp", MtGlobalTable)
    ,("Customer", FromMtSpecificTable generateCustomerTable)
    ,("Orders", FromMtSpecificTable generateOrdersTable)
    ,("Lineitem", FromMtSpecificTable generateLineitemTable)
    ]


runTestQueries :: MtSchemaSpec -> MtSetting -> IO ()
runTestQueries spec setting = do
    let queries = ["SELECT * FROM Supplier;"
                    ,"SELECT * FROM Supplier WHERE S_name = ?;"
                    ,"SELECT DISTINCT S_name FROM Supplier WHERE S_acctbal > 42;"
                    ,"SELECT S.S_name as SUP_NAME, N.n_name as NAT_NAME FROM Supplier S, Nation N WHERE S.s_nationkey = N.n_nationkey"
                    ,"SELECT max(S_acct) as NATIONMAX FROM (SELECT avg(S_acctbal) as S_acct FROM Supplier GROUP BY S_nationkey ORDER BY S_nationkey) tmp"
                    ,"SELECT SUM(L_extendedprice*L_discount) AS REVENUE FROM Lineitem WHERE L_shipdate >= '1994-01-01' AND L_shipdate < cast('1995-01-01' as date) AND L_discount between .06 - 0.01 AND .06 + 0.01 AND L_quantity < 24" -- Q6
                    ,"SELECT C_custkey, C_name, SUM(L_extendedprice*(1-L_discount)) AS REVENUE, C_acctbal, N_name, C_address, C_phone, C_comment FROM Customer, Orders, Lineitem, Nation WHERE C_custkey = O_custkey AND L_orderkey = O_orderkey AND O_orderdate>= '1993-10-01' AND O_orderdate < cast('1994-01-01' as date) AND L_returnflag = 'R' AND C_nationkey = N_nationkey GROUP BY C_custkey, C_name, C_acctbal, C_phone, N_name, C_address, C_comment ORDER BY REVENUE DESC LIMIT 20" -- Q10
                    ,"SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_acctbal) AS TOTACCTBAL FROM (SELECT SUBSTRING(C_phone,1,2) AS CNTRYCODE, C_acctbal FROM Customer WHERE SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND C_acctbal > (SELECT AVG(C_acctbal) FROM Customer WHERE C_acctbal > 0.00 AND SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS ( SELECT * FROM Orders WHERE O_custkey = C_custkey)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE" -- Q22
                    ,"SELECT C.c_custkey, C.c_name, SUM(L.l_extendedprice*(1-L.l_discount)) AS REVENUE, C.c_acctbal, N.n_name, C.c_address, C.c_phone, C.c_comment FROM Customer C, Orders O, Lineitem L, Nation N WHERE C.c_custkey = O.o_custkey AND L.l_orderkey = O.o_orderkey AND O.o_orderdate>= '1993-10-01' AND O.o_orderdate < cast('1994-01-01' as date) AND L.l_returnflag = 'R' AND C.c_nationkey = N.n_nationkey GROUP BY C.c_custkey, C.c_name, C.c_acctbal, C.c_phone, N.n_name, C.c_address, C.c_comment ORDER BY REVENUE DESC LIMIT 20" -- Q10, with tables renamed
                    ,"SELECT C_name, N_nationkey FROM Customer, Nation WHERE C_custkey = N_nationkey"
                    ,"SELECT C_name, O_orderkey FROM Customer, Orders WHERE O_totalprice = C_custkey "
                   ]

    -- test pretty print
    putStrLn "\nPretty Print with error looks like this:"
    putStrLn $ mtPrettyPrint $ mtParse "SELECT;" -- should print an error message
    putStrLn "\nPretty Print for normal query looks like this:"
    putStrLn $ mtPrettyPrint $ mtParse $ head queries -- should print something correct
    
    -- test parsing and rewrite
    mapM_ (\query -> do
        putStrLn "\n====================================================\n"
        putStrLn $ query ++ " parses to:\n"
        let parsedQuery = mtParse query
        print parsedQuery
        let rewrittenQuery = mtRewrite spec setting query
        putStrLn $ "\nIts rewritten form is:\n  " ++ mtPrettyPrint rewrittenQuery
        putStrLn "and has the following syntax tree:\n"
        print rewrittenQuery 
        )
        queries

    putStrLn "\n\n"
    return ()

runTPCHQueries :: MtSchemaSpec -> MtSetting -> IO ()
runTPCHQueries spec setting = do
    let queries = ["SELECT L_returnflag, L_linestatus, SUM(L_quantity) AS SUM_QTY, SUM(L_extendedprice) AS SUM_BASE_PRICE, SUM(L_extendedprice*(1-L_discount)) AS SUM_DISC_PRICE, SUM(L_extendedprice*(1-L_discount)*(1+L_tax)) AS SUM_CHARGE, AVG(L_quantity) AS AVG_QTY, AVG(L_extendedprice) AS AVG_PRICE, AVG(L_discount) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM Lineitem WHERE L_shipdate <= cast('1998-09-02' as date) GROUP BY L_returnflag, L_linestatus ORDER BY L_returnflag,L_linestatus" -- Q01
                    ,"SELECT SUM(L_extendedprice*L_discount) AS REVENUE FROM Lineitem WHERE L_shipdate >= '1994-01-01' AND L_shipdate < cast('1995-01-01' as date) AND L_discount between .06 - 0.01 AND .06 + 0.01 AND L_quantity < 24" -- Q6
                    ,"SELECT C_custkey, C_name, SUM(L_extendedprice*(1-L_discount)) AS REVENUE, C_acctbal, N_name, C_address, C_phone, C_comment FROM Customer, Orders, Lineitem, Nation WHERE C_custkey = O_custkey AND L_orderkey = O_orderkey AND O_orderdate>= '1993-10-01' AND O_orderdate < cast('1994-01-01' as date) AND L_returnflag = 'R' AND C_nationkey = N_nationkey GROUP BY C_custkey, C_name, C_acctbal, C_phone, N_name, C_address, C_comment ORDER BY REVENUE DESC LIMIT 20" -- Q10
                    ,"SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_acctbal) AS TOTACCTBAL FROM (SELECT SUBSTRING(C_phone,1,2) AS CNTRYCODE, C_acctbal FROM Customer WHERE SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND C_acctbal > (SELECT AVG(C_acctbal) FROM Customer WHERE C_acctbal > 0.00 AND SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS ( SELECT * FROM Orders WHERE O_custkey = C_custkey)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE" -- Q22
                   ]

    -- rewrite
    mapM_ (\query -> do
        putStrLn "\n====================================================\n"
        putStrLn $ query ++ " rewrites to:\n"
        let rewrittenQuery = mtRewrite spec setting query
        putStrLn (mtPrettyPrint rewrittenQuery)
        )
        queries

    putStrLn "\n\n"
    return ()

sessionLoop :: MtSchemaSpec -> MtSetting -> IO ()
sessionLoop spec setting = do
    putStrLn ">> Please write a query you want to rewrite or type 'logout'/'l' to logout or 'test' to run a set of test queries."
    line <- getLine
    if line == "logout" || line == "l"
        then return ()
        else do
            if line == "test"
                then do
                    runTestQueries spec setting
                    sessionLoop spec setting
                else do
                    if line == "tpch"
                        then do
                            runTPCHQueries spec setting
                            sessionLoop spec setting
                        else do
                            putStrLn $ "The query parses to:\n" ++ mtPrettyPrint (mtParse line)
                            putStrLn $ "\nIts rewritten form is:\n  " ++ (mtPrettyPrint (mtRewrite spec setting line))
                            sessionLoop spec setting

mainLoop :: MtSchemaSpec -> IO ()
mainLoop spec = do
    putStrLn ">> Please write 'login'/'l' to login or any other word to quit"
    line <- getLine
    if line /= "login" && line /= "l"
        then return ()
        else do
            putStrLn "Please enter your client id:"
            line1 <- getLine
            let c = read line1 :: Int
            putStrLn "Please enter your dataset as space-separated tenant ids:"
            line2 <- getLine
            let d = map read $ words line2 :: [Int]
            putStrLn ("###############################################")
            putStrLn ("Successfully logged in with C=" ++ show c ++ " and D=" ++ show d)
            putStrLn ("###############################################")
            sessionLoop spec (c,d)
            mainLoop spec

main :: IO ()
main = do
    let schemaSpec = generateTestSchema
    mainLoop schemaSpec
    return ()
    -- let client = 7
    -- let dataset = [3,7]
    -- runTestQueries schemaSpec client dataset
    
