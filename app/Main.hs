module Main where

import Control.Monad
import System.IO
import qualified Data.Set as S

import MtLib
import TPCHSchema

-- TODO: something hangs here... the problem was introduced with commit 47edd65, but so far problem not identified...
runTestQueries :: MtSchemaSpec -> MtSetting -> Dialect -> IO ()
runTestQueries spec setting dialect = do
    let queries = ["SELECT * FROM Supplier;"
                    ,"SELECT * FROM Supplier WHERE S_name = ?;"
                    ,"SELECT DISTINCT S_name FROM Supplier WHERE S_acctbal > 42;"
                    ,"SELECT S.S_name as SUP_NAME, N.n_name as NAT_NAME FROM Supplier S, Nation N WHERE S.s_nationkey = N.n_nationkey"
                    ,"SELECT max(S_acct) as NATIONMAX FROM (SELECT avg(S_acctbal) as S_acct FROM Supplier GROUP BY S_nationkey ORDER BY S_nationkey) tmp"
                    ,"SELECT SUM(L_extendedprice*L_discount) AS REVENUE FROM Lineitem WHERE L_shipdate >= '1994-01-01' AND L_shipdate < cast('1995-01-01' as date) AND L_discount between .06 - 0.01 AND .06 + 0.01 AND L_quantity < 24" -- Q6
                    ,"SELECT C_custkey, C_name, SUM(L_extendedprice*(1-L_discount)) AS REVENUE, C_acctbal, N_name, C_address, C_phone, C_comment FROM Customer, Orders, Lineitem, Nation WHERE C_custkey = O_custkey AND L_orderkey = O_orderkey AND O_orderdate>= '1993-10-01' AND O_orderdate < cast('1994-01-01' as date) AND L_returnflag = 'R' AND C_nationkey = N_nationkey GROUP BY C_custkey, C_name, C_acctbal, C_phone, N_name, C_address, C_comment ORDER BY REVENUE DESC LIMIT 20" -- Q10
                    ,"SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_acctbal) AS TOTACCTBAL FROM (SELECT SUBSTRING(C_phone,1,2) AS CNTRYCODE, C_acctbal FROM Customer WHERE SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND C_acctbal > (SELECT AVG(C_acctbal) FROM Customer WHERE C_acctbal > 0.00 AND SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS ( SELECT * FROM Orders WHERE O_custkey = C_custkey)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE" -- Q22
                    ,"SELECT C.c_custkey, C.c_name, SUM(L.l_extendedprice*(1-L.l_discount)) AS REVENUE, C.c_acctbal, N.n_name, C.c_address, C.c_phone, C.c_comment FROM Customer C, Orders O, Lineitem L, Nation N WHERE C.c_custkey = O.o_custkey AND L.l_orderkey = O.o_orderkey AND O.o_orderdate>= '1993-10-01' AND O.o_orderdate < cast('1994-01-01' as date) AND L.l_returnflag = 'R' AND C.c_nationkey = N.n_nationkey GROUP BY C.c_custkey, C.c_name, C.c_acctbal, C.c_phone, N.n_name, C.c_address, C.c_comment ORDER BY REVENUE DESC LIMIT 20" -- Q10, with tables renamed
                    ,"SELECT C_name, N_nationkey FROM Customer, Nation WHERE C_custkey = N_nationkey" -- test MT error handling
                    ,"SELECT C_name, O_orderkey FROM Customer, Orders WHERE O_totalprice = C_custkey " -- test MT error handling
                    ,"SELECT S_name, COUNT(*) AS NUMWAIT FROM Supplier, Lineitem L1, Orders, Nation WHERE S_suppkey = L1.L_suppkey AND O_orderkey = L1.L_orderkey AND O_orderstatus = 'F' AND L1.L_receiptdate> L1.L_commitdate AND EXISTS (SELECT * FROM Lineitem L2 WHERE L2.L_orderkey = L1.L_orderkey AND L2.L_suppkey <> L1.L_suppkey) AND NOT EXISTS (SELECT * FROM Lineitem L3 WHERE L3.L_orderkey = L1.L_orderkey AND L3.L_suppkey <> L1.L_suppkey AND L3.L_receiptdate > L3.L_commitdate) AND S_nationkey = N_nationkey AND N_name = 'SAUDI ARABIA' GROUP BY S_name ORDER BY NUMWAIT DESC, S_name LIMIT 100;" -- Q21
                   ]

    -- test pretty print
    putStrLn "\nPretty Print with error looks like this:"
    putStrLn $ mtPrettyPrint (mtParse "SELECT;" dialect) dialect        -- should print an error message
    putStrLn "\nPretty Print for normal query looks like this:"
    putStrLn $ mtPrettyPrint (mtParse (head queries) dialect) dialect   -- should print something correct
    
    -- test parsing and rewrite
    mapM_ (\query -> do
        putStrLn "\n====================================================\n"
        putStrLn $ query ++ " parses to:\n"
        let parsedQuery = mtParse query dialect
        print parsedQuery
        -- TODO: problem is somewhere here...
        let rewrittenQuery = mtRewrite spec setting query dialect
        putStrLn $ "\nIts rewritten form is:\n  " ++ mtPrettyPrint rewrittenQuery dialect
        putStrLn "and has the following syntax tree:\n"
        print rewrittenQuery 
        ) queries

    putStrLn "\n\n"
    return ()

runTPCHQueries :: MtSchemaSpec -> MtSetting -> Dialect -> IO ()
runTPCHQueries spec setting dialect = do
    let queries = ["SELECT L_returnflag, L_linestatus, SUM(L_quantity) AS SUM_QTY, SUM(L_extendedprice) AS SUM_BASE_PRICE, SUM(L_extendedprice*(1-L_discount)) AS SUM_DISC_PRICE, SUM(L_extendedprice*(1-L_discount)*(1+L_tax)) AS SUM_CHARGE, AVG(L_quantity) AS AVG_QTY, AVG(L_extendedprice) AS AVG_PRICE, AVG(L_discount) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM Lineitem WHERE L_shipdate <= '1998-09-02' GROUP BY L_returnflag, L_linestatus ORDER BY L_returnflag,L_linestatus;" -- Q01
                    ,"SELECT S_acctbal, S_name, N_name, P_partkey, P_mfgr, S_address, S_phone, S_comment FROM Part, Supplier, Partsupp, Nation, Region WHERE P_partkey = PS_partkey AND S_suppkey = PS_suppkey AND P_size = 15 AND P_type LIKE '%%BRASS' AND S_nationkey = N_nationkey AND N_regionkey = R_regionkey AND R_name = 'EUROPE' AND PS_supplycost = (SELECT MIN(PS_supplycost) FROM Partsupp, Supplier, Nation, Region WHERE P_partkey = PS_partkey AND S_suppkey = PS_suppkey AND S_nationkey = N_nationkey AND N_regionkey = R_regionkey AND R_name = 'EUROPE') ORDER BY S_acctbal DESC, N_name, S_name, P_partkey LIMIT 100;" --Q02
                    ,"SELECT L_orderkey, SUM(L_extendedprice*(1-L_discount)) AS REVENUE, O_orderdate, O_shippriority FROM Customer, Orders, Lineitem WHERE C_mktsegment = 'BUILDING' AND C_custkey = O_custkey AND L_orderkey = O_orderkey AND O_orderdate < '1995-03-15' AND L_shipdate > '1995-03-15' GROUP BY L_orderkey, O_orderdate, O_shippriority ORDER BY REVENUE DESC, O_orderdate LIMIT 10;" --Q03
                    ,"SELECT O_orderpriority, COUNT(*) AS ORDER_COUNT FROM Orders WHERE O_orderdate >= '1993-07-01' AND O_orderdate < '1993-10-01' AND EXISTS (SELECT * FROM Lineitem WHERE L_orderkey = O_orderkey AND L_commitdate < L_receiptdate) GROUP BY O_orderpriority ORDER BY O_orderpriority;" --Q04
                    ," SELECT N_name, SUM(L_extendedprice*(1-L_discount)) AS REVENUE FROM Customer, Orders, Lineitem, Supplier, Nation, Region WHERE C_custkey = O_custkey AND L_orderkey = O_orderkey AND L_suppkey = S_suppkey AND C_nationkey = S_nationkey AND S_nationkey = N_nationkey AND N_regionkey = R_regionkey AND R_name = 'ASIA' AND O_orderdate >= '1994-01-01' AND O_orderdate < '1995-01-01' GROUP BY N_name ORDER BY REVENUE DESC;" --Q05
                    ,"SELECT SUM(L_extendedprice*L_discount) AS REVENUE FROM Lineitem WHERE L_shipdate >= '1994-01-01' AND L_shipdate < '1995-01-01' AND L_discount between .06 - 0.01 AND .06 + 0.01 AND L_quantity < 24;" -- Q06
                    ,"SELECT SUPP_Nation, CUST_Nation, L_YEAR, SUM(VOLUME) AS REVENUE FROM ( SELECT N1.N_name AS SUPP_Nation, N2.N_name AS CUST_Nation, datepart(yy, L_shipdate) AS L_YEAR,L_extendedprice*(1-L_discount) AS VOLUME FROM Supplier, Lineitem, Orders, Customer, Nation N1, Nation N2 WHERE S_suppkey = L_suppkey AND O_orderkey = L_orderkey AND C_custkey = O_custkey AND S_nationkey = N1.N_nationkey AND C_nationkey = N2.N_nationkey AND ((N1.N_name = 'FRANCE' AND N2.N_name = 'GERMANY') OR (N1.N_name = 'GERMANY' AND N2.N_name = 'FRANCE')) AND L_shipdate BETWEEN '1995-01-01' AND '1996-12-31' ) AS SHIPPING GROUP BY SUPP_Nation, CUST_Nation, L_YEAR ORDER BY SUPP_Nation, CUST_Nation, L_YEAR;" --Q07
                    ,"SELECT O_YEAR, SUM(CASE WHEN Nation = 'BRAZIL' THEN VOLUME ELSE 0 END)/SUM(VOLUME) AS MKT_SHARE FROM (SELECT datepart(yy,O_orderdate) AS O_YEAR, L_extendedprice*(1-L_discount) AS VOLUME, N2.N_name AS Nation FROM Part, Supplier, Lineitem, Orders, Customer, Nation N1, Nation N2, Region WHERE P_partkey = L_partkey AND S_suppkey = L_suppkey AND L_orderkey = O_orderkey AND O_custkey = C_custkey AND C_nationkey = N1.N_nationkey AND N1.N_regionkey = R_regionkey AND R_name = 'AMERICA' AND S_nationkey = N2.N_nationkey AND O_orderdate BETWEEN '1995-01-01' AND '1996-12-31' AND P_type= 'ECONOMY ANODIZED STEEL') AS ALL_NATIONS GROUP BY O_YEAR ORDER BY O_YEAR;" --Q08
                    ,"SELECT Nation, O_YEAR, SUM(AMOUNT) AS SUM_PROFIT FROM (SELECT N_name AS Nation, datepart(yy, O_orderdate) AS O_YEAR,L_extendedprice*(1-L_discount) - PS_supplycost*L_quantity AS AMOUNT FROM Part, Supplier, Lineitem, Partsupp, Orders, Nation WHERE S_suppkey = L_suppkey AND PS_suppkey= L_suppkey AND PS_partkey = L_partkey AND P_partkey= L_partkey AND O_orderkey = L_orderkey AND S_nationkey = N_nationkey AND P_name LIKE '%%green%%') AS PROFIT GROUP BY Nation, O_YEAR ORDER BY Nation, O_YEAR DESC;" --Q09
                    ,"SELECT C_custkey, C_name, SUM(L_extendedprice*(1-L_discount)) AS REVENUE, C_acctbal, N_name, C_address, C_phone, C_comment FROM Customer, Orders, Lineitem, Nation WHERE C_custkey = O_custkey AND L_orderkey = O_orderkey AND O_orderdate>= '1993-10-01' AND O_orderdate < '1994-01-01' AND L_returnflag = 'R' AND C_nationkey = N_nationkey GROUP BY C_custkey, C_name, C_acctbal, C_phone, N_name, C_address, C_comment ORDER BY REVENUE DESC LIMIT 20;" -- Q10
                    ,"SELECT PS_partkey, SUM(PS_supplycost*PS_availqty) AS VALUE FROM Partsupp, Supplier, Nation WHERE PS_suppkey = S_suppkey AND S_nationkey = N_nationkey AND N_name = 'GERMANY' GROUP BY PS_partkey HAVING SUM(PS_supplycost*PS_availqty) > (SELECT SUM(PS_supplycost*PS_availqty) * 0.0001000000FROM Partsupp, Supplier, Nation WHERE PS_suppkey = S_suppkey AND S_nationkey = N_nationkey AND N_name = 'GERMANY') ORDER BY VALUE DESC;" --Q11
                    ,"SELECT L_shipmode, SUM(CASE WHEN O_orderpriority = '1-URGENT' OR O_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT, SUM(CASE WHEN O_orderpriority <> '1-URGENT' AND O_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END ) AS LOW_LINE_COUNT FROM Orders, Lineitem WHERE O_orderkey = L_orderkey AND L_shipmode IN ('MAIL','SHIP') AND L_commitdate < L_receiptdate AND L_shipdate < L_commitdate AND L_receiptdate >= '1994-01-01' AND L_receiptdate < '1995-10-01' GROUP BY L_shipmode ORDER BY L_shipmode;" --Q12
                    ," SELECT C_COUNT, COUNT(*) AS CUSTDIST FROM (SELECT C_custkey, COUNT(O_orderkey)FROM Customer left outer join Orders on C_custkey = O_custkey AND O_comment not like '%%special%%requests%%'GROUP BY C_custkey) AS C_Orders (C_custkey, C_COUNT) GROUP BY C_COUNT ORDER BY CUSTDIST DESC, C_COUNT DESC;" --Q13
                    ," SELECT 100.00* SUM(CASE WHEN P_type LIKE 'PROMO%%' THEN L_extendedprice*(1-L_discount) ELSE 0 END) / SUM(L_extendedprice*(1-L_discount)) AS PROMO_REVENUE FROM Lineitem, Part WHERE L_partkey = P_partkey AND L_shipdate >= '1995-09-01' AND L_shipdate < '1995-10-01';" --Q14
                    ,"CREATE VIEW REVENUE0 (Supplier_NO, TOTAL_REVENUE) AS SELECT L_suppkey, SUM(L_extendedprice*(1-L_discount)) FROM Lineitem WHERE L_shipdate >= '1996-01-01' AND L_shipdate < '1996-04-01' GROUP BY L_suppkey;" --Q15 - Create View
                    ,"SELECT S_suppkey, S_name, S_address, S_phone, TOTAL_REVENUE FROM Supplier, REVENUE0 WHERE S_suppkey = Supplier_NO AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE0) ORDER BY S_suppkey;" --Q15 - Query
                    ,"DROP VIEW REVENUE0;" --Q15 - Drop View
                    ,"SELECT P_brand, P_type, P_size, COUNT(DISTINCT PS_suppkey) AS Supplier_CNT FROM Partsupp, Part WHERE P_partkey = PS_partkey AND P_brand <> 'Brand#45' AND P_type NOT LIKE 'MEDIUM POLISHED%%' AND P_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND PS_suppkey NOT IN (SELECT S_suppkey FROM Supplier WHERE S_comment LIKE '%%Customer% Complaints%%') GROUP BY P_brand, P_type, P_size ORDER BY Supplier_CNT DESC, P_brand, P_type, P_size;" --Q16
                    ,"SELECT SUM(L_extendedprice)/7.0 AS AVG_YEARLY FROM Lineitem, Part WHERE P_partkey = L_partkey AND P_brand = 'Brand#23' AND P_container = 'MED BOX' AND L_quantity < (SELECT 0.2*AVG(L_quantity) FROM Lineitem WHERE L_partkey = P_partkey);" --Q17
                    ,"SELECT C_name, C_custkey, O_orderkey, O_orderdate, O_totalprice, SUM(L_quantity) FROM Customer, Orders, Lineitem WHERE O_orderkey IN (SELECT L_orderkey FROM Lineitem GROUP BY L_orderkey HAVING SUM(L_quantity) > 300) AND C_custkey = O_custkey AND O_orderkey = L_orderkey GROUP BY C_name, C_custkey, O_orderkey, O_orderdate, O_totalprice ORDER BY O_totalprice DESC, O_orderdate LIMIT 100;" --Q18
                    ,"SELECT SUM(L_extendedprice* (1 - L_discount)) AS REVENUE FROM Lineitem, Part WHERE (P_partkey = L_partkey AND P_brand = 'Brand#12' AND P_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND L_quantity >= 1 AND L_quantity <= 1 + 10 AND P_size BETWEEN 1 AND 5 AND L_shipmode IN ('AIR', 'AIR REG') AND L_shipinstruct = 'DELIVER IN PERSON') OR (P_partkey = L_partkey AND P_brand ='Brand#23' AND P_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND L_quantity >=10 AND L_quantity <=10 + 10 AND P_size BETWEEN 1 AND 10 AND L_shipmode IN ('AIR', 'AIR REG') AND L_shipinstruct = 'DELIVER IN PERSON') OR (P_partkey = L_partkey AND P_brand = 'Brand#34' AND P_container IN ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND L_quantity >=20 AND L_quantity <= 20 + 10 AND P_size BETWEEN 1 AND 15 AND L_shipmode IN ('AIR', 'AIR REG') AND L_shipinstruct = 'DELIVER IN PERSON');" --Q19
                    ,"SELECT S_name, S_address FROM Supplier, Nation WHERE S_suppkey IN (SELECT PS_suppkey FROM Partsupp WHERE PS_partkey in (SELECT P_partkey FROM Part WHERE P_name like 'forest%%') AND PS_availqty > (SELECT 0.5*sum(L_quantity) FROM Lineitem WHERE L_partkey = PS_partkey AND L_suppkey = PS_suppkey AND L_shipdate >= '1994-01-01' AND L_shipdate < '1995-01-01')) AND S_nationkey = N_nationkey AND N_name = 'CANADA' ORDER BY S_name;" --Q20
                    ,"SELECT S_name, COUNT(*) AS NUMWAIT FROM Supplier, Lineitem L1, Orders, Nation WHERE S_suppkey = L1.L_suppkey AND O_orderkey = L1.L_orderkey AND O_orderstatus = 'F' AND L1.L_receiptdate> L1.L_commitdate AND EXISTS (SELECT * FROM Lineitem L2 WHERE L2.L_orderkey = L1.L_orderkey AND L2.L_suppkey <> L1.L_suppkey) AND NOT EXISTS (SELECT * FROM Lineitem L3 WHERE L3.L_orderkey = L1.L_orderkey AND L3.L_suppkey <> L1.L_suppkey AND L3.L_receiptdate > L3.L_commitdate) AND S_nationkey = N_nationkey AND N_name = 'SAUDI ARABIA' GROUP BY S_name ORDER BY NUMWAIT DESC, S_name LIMIT 100;" --Q21
                    ,"SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_acctbal) AS TOTACCTBAL FROM (SELECT SUBSTRING(C_phone,1,2) AS CNTRYCODE, C_acctbal FROM Customer WHERE SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND C_acctbal > (SELECT AVG(C_acctbal) FROM Customer WHERE C_acctbal > 0.00 AND SUBSTRING(C_phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND NOT EXISTS ( SELECT * FROM Orders WHERE O_custkey = C_custkey)) AS CUSTSALE GROUP BY CNTRYCODE ORDER BY CNTRYCODE;" -- Q22
                   ]

    -- rewrite
    mapM_ (\(idx, query) -> do
        putStrLn "\n====================================================\n"
        putStrLn $ "Query " ++ show idx ++ "\n"
        -- DEBUG
        -- putStrLn $ query ++ " parses to:\n"
        -- putStrLn (show (mtParse query dialect))
        -- putStrLn ("\n")
        -- END DEBUG
        putStrLn $ query ++ " rewrites to:\n"
        let rewrittenQuery = mtRewrite spec setting query dialect
        putStrLn (mtCompactPrint rewrittenQuery dialect)
        ) (zip [(1::Integer)..] queries)

    putStrLn "\n\n"
    return ()

runQueryFile :: MtSchemaSpec -> MtSetting -> Dialect -> (String, String) -> IO ()
runQueryFile spec setting dialect (infile, outfile)= do
    putStr $ "Reading from " ++ infile ++ " and storing results in " ++ outfile ++ "..."

    inputHandle <- openFile infile ReadMode
    outputHandle <- openFile outfile WriteMode
    contents <- hGetContents inputHandle
    let queries = lines contents

    mapM_ (\query -> do
        let ws = words query
        if head (head ws) == '-'
            then do
                -- it is just a comment --> leave as is
                hPutStrLn outputHandle query
                return ()
            else do
                let rewrittenQuery = mtRewrite spec setting query dialect
                hPutStrLn outputHandle (mtCompactPrint rewrittenQuery dialect)
                return ()
        ) queries

    hClose inputHandle
    hFlush outputHandle
    hClose outputHandle
    putStrLn "[DONE]\n"
    return ()

awaitInput :: IO ()
awaitInput = do
    putStr ">> "
    return ()

sessionLoop :: MtSchemaSpec -> MtSetting -> Dialect -> IO ()
sessionLoop spec setting dialect = do
    putStrLn "Please write a query you want to rewrite or type 'logout'/'l' to logout or 'test' to run a set of test queries. Other options include 'tpch' to run the tpch queries and 'file <infile> <outfile>' to convert queries read from a file."
    awaitInput
    line <- getLine
    Control.Monad.unless (line == "logout" || line == "l") $
        if line == "test"
            then do
                runTestQueries spec setting dialect
                sessionLoop spec setting dialect
            else 
                if line == "tpch"
                    then do
                        runTPCHQueries spec setting dialect
                        sessionLoop spec setting dialect
                    else do
                        let ws = words line
                        if head ws == "file"
                            then do
                                runQueryFile spec setting dialect (ws !! 1, ws !! 2)
                                sessionLoop spec setting dialect
                            else do
-- TODO: something hangs here... the problem was introduced with commit 47edd65, but so far problem not identified...
                                putStrLn $ "The query parses to:\n" ++ mtPrettyPrint (mtParse line dialect) dialect
                                -- TODO: the problem is somewhere here
                                putStrLn $ "\nIts rewritten form is:\n  " ++ mtPrettyPrint (mtRewrite spec setting line dialect) dialect
                                sessionLoop spec setting dialect

getDialect :: String -> Dialect
getDialect s
    | s == "mssql"      = sqlServerDialect
    | s == "postgres"   = postgresDialect
    | s == "oracle"     = oracleDialect
    | otherwise         = ansiDialect

getOptimizations :: String -> S.Set MtOptimization
getOptimizations s =
    let ws      = words s
        f "t"   = MtTrivialOptimization
        f "p"   = MtClientPresentationPushUp
        f "c"   = MtConversionPushUp
        f "d"   = MtConversionDistribution
        f _     = MtUnknownOptimization
    in mtOptimizationsFromList $ map f ws

mainLoop :: MtSchemaSpec -> IO ()
mainLoop spec = do
    putStrLn "Please write 'login'/'l'to login, 'test' to run a test for C=1 and D=[1,42], or any other word to quit "
    awaitInput
    line <- getLine
    Control.Monad.unless (line /= "login" && line /= "l" && line /= "test") $
        if line == "test"
            then do
                runTestQueries spec (1, [1,42],
                    (mtOptimizationsFromList [MtTrivialOptimization, MtClientPresentationPushUp, MtConversionPushUp, MtConversionDistribution]))
                    ansiDialect
                mainLoop spec
            else do
                putStrLn "Please enter your client id:"
                awaitInput
                line1 <- getLine
                let c = read line1 :: Int
                putStrLn "Please enter your dataset as space-separated tenant ids:"
                awaitInput
                line2 <- getLine
                let d = map read $ words line2 :: [Int]
                putStrLn "Please enter 'mssql', 'postgres', 'oracle' for a specific dialect or nothing for ANSI SQL."
                line3 <- getLine
                let dialect = getDialect line3
                putStrLn "Please enter a number of optimization passes as characters, separated by spaces. Options include:"
                putStrLn ("'t' (trivial optimizations), 'p' (client presentation push-up),"
                    ++ "'c' (client conversion push-up), 'd' (conversion distribution)")
                line4 <- getLine
                let o = getOptimizations line4
                putStrLn "###############################################"
                putStrLn ("Successfully logged in with C=" ++ show c ++ " and D=" ++ show d ++ ".")
                putStrLn ("Dialect: " ++ diName dialect ++ ", Optimizations: " ++ show o)
                putStrLn "###############################################"
                sessionLoop spec (c,d,o) dialect
                mainLoop spec

main :: IO ()
main = do
    let schemaSpec = generateTPCHSchema
    mainLoop schemaSpec
    return ()
 
