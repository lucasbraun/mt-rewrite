module Main where

import MtLib

generateTestSchema :: MtSchemaSpec
generateTestSchema = mtSchemaSpecFromList
    [("region", MtGlobalTable)
    ,("nation", MtGlobalTable)
    ]

main :: IO ()
main = do
    putStrLn $ mtPrettyPrint (mtParse "SELECT a From A;") -- should print something correct
    putStrLn $ mtPrettyPrint (mtParse "SELECT;") -- should print an error message
