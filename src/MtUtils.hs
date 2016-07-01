module MtUtils
(
    TableAttributePair
    ,getTenantAttributeName
    ,getTenantIdentifier
    ,isGlobalTable
    ,getOldTableName
    ,getTableAndAttName
    ,lookupAttributeComparability
    ,printName
    ,containsString
    ,removeDuplicates
) where

import MtTypes
import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A
import qualified Data.Map as M

type TableAttributePair = (Maybe MtTableName, Maybe MtAttributeName)

getTenantAttributeName :: MtTableName -> MtAttributeName
getTenantAttributeName s = s ++ "_TENANT_KEY"

-- takes a (alias of a) table name and an mt table name and constructs the corresponding identifier
getTenantIdentifier :: String -> MtTableName -> Pa.ScalarExpr
getTenantIdentifier tName mtName = Pa.Identifier A.emptyAnnotation $ Pa.Name A.emptyAnnotation [Pa.Nmc tName, Pa.Nmc $ getTenantAttributeName mtName]

-- checks whehter a table is global. Assumes anything not in the schema spec is also global
isGlobalTable :: MtSchemaSpec -> Maybe MtTableName -> Bool
isGlobalTable spec (Just tName) =
    let tableSpec = M.lookup tName spec
        analyse (Just (FromMtSpecificTable _))  = False
        analyse _                               = True
    in analyse tableSpec
isGlobalTable _ Nothing = True

-- returns old name (name before renaming) for a specific table name given the tref list
getOldTableName :: Maybe MtTableName -> Pa.TableRefList -> Maybe MtTableName
getOldTableName (Just tableName) (Pa.TableAlias _ (Pa.Nmc aliasName) (Pa.Tref _ (Pa.Name _ [Pa.Nmc tName])):trefs)
    | tableName == aliasName    = Just tName
    | otherwise                 = getOldTableName (Just tableName) trefs
getOldTableName (Just tableName) (Pa.Tref _ (Pa.Name _ [Pa.Nmc tName]):trefs)
    | tableName == tName    = Just tName
    | otherwise             = getOldTableName (Just tableName) trefs
getOldTableName (Just tableName) (Pa.JoinTref _ tref0 _ _ _ tref1 _ : trefs) = getOldTableName (Just tableName) (tref0:tref1:trefs)
getOldTableName (Just tableName) (_:trefs) = getOldTableName (Just tableName) trefs
getOldTableName _ [] = Nothing
getOldTableName Nothing _ = Nothing

-- returns a pair (table-name, attribute-name) where both can be nothing
getTableAndAttName :: Pa.Name -> TableAttributePair
getTableAndAttName (Pa.Name _ nameList) =
    let (Pa.Nmc attName) = last nameList
        (Pa.Nmc tName)
            | length nameList > 1   = last $ init nameList
            | otherwise             = Pa.Nmc ""  
        tableName
            | not (null tName)  = Just tName
            | otherwise         = Nothing 
    in  (tableName, Just attName)
getTableAndAttName (Pa.AntiName _) = (Nothing, Nothing)

-- returns the attribute comparability for a specific attribute if it is part of a tenant-specific table
lookupAttributeComparability :: MtSchemaSpec -> TableAttributePair -> Pa.TableRefList -> Maybe MtAttributeComparability
lookupAttributeComparability spec (tableName, attributeName) trefs = do
    attName <- attributeName
    let oldTName = getOldTableName tableName trefs
    tName <- oldTName
    (FromMtSpecificTable tableSpec) <- M.lookup tName spec
    tSpec <- Just tableSpec
    M.lookup attName tSpec

printName :: Pa.Name -> String
printName (Pa.Name _ (Pa.Nmc name:names)) = foldl (\w (Pa.Nmc n) -> w ++ "." ++ n) name names
printName _ = ""

-- checks whether a string contains a certain substring
containsString :: String -> String ->Bool
containsString l s = containsString' l s True where
    containsString' _ [] _          = True
    containsString' [] _ _          = False
    containsString' (x:xs) (y:ys) h = (y == x && containsString' xs ys False) || (h && containsString' xs (y:ys) h)

-- returns all duplicates from a list
removeDuplicates :: Eq a => [a] -> [a]
removeDuplicates = foldr (\x seen ->
    if x `elem` seen
    then seen
    else x : seen) []

