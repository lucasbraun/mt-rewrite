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

generateTestSchema :: MtSchemaSpec
generateTestSchema = mtSchemaSpecFromList
    [("REGION", MtGlobalTable)
    ,("NATION", MtGlobalTable)
    ,("SUPPLIER", FromMtSpecificTable generateSupplierTable)
    ]

main :: IO ()
main = do
    let queries = ["SELECT * FROM SUPPLIER;",
                   "SELECT SUPPLIER.S_NAME FROM SUPPLIER;",
                   "SELECT SUPPLIER.S_NAME FROM SUPPLIER WHERE S_ACCTBAL > 42;"]
    let schemaSpec = generateTestSchema
    
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
        putStrLn $ "\n Its rewritten form is:\n  " ++ mtPrettyPrintRewrittenQuery (mtRewrite schemaSpec parsedQuery)
        )
        queries

    putStrLn "\n\n"
