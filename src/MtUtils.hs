module MtUtils
(
    MtRewriteError(..)
    ,ProvenanceItem(..)
    ,Provenance
    ,addToProvenance
    ,addIdentifierToProvenance
    ,getProvenanceItem
    ,replaceProvenanceItem
    ,flattenProvenance
    ,pruneProvenance
    ,keepRecommendations
    ,emptyProvenance
    ,RewriteQueryFun
    ,CasesType
    ,TableAttributePair
    ,getTenantAttributeName
    ,getTenantIdentifier
    ,getIntermediateTenantIdentifier
    ,isGlobalTable
    ,getOldTableName
    ,getTableAndAttName
    ,lookupAttributeComparability
    ,createConvFunctionApplication
    ,ConversionFunctionsTriple
    ,getConversionFunctions
    ,isComparisonOp
    ,isSqlAggOp
    ,printName
    ,containsString
    ,removeDuplicates
) where

import MtTypes
import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A

import qualified Data.Map as M
import qualified Data.MultiMap as MM
-- import qualified Data.Set as S --> already imported somewhere else
import qualified Data.Text as T

-- error handling
data MtRewriteError = FromParseError Pa.ParseErrorExtra | FromMtRewriteError String
instance Show MtRewriteError where
    show (FromParseError err)     = show err
    show (FromMtRewriteError err) = "MTSQL-ERROR: " ++ err

-- conversion provenance
data ProvenanceItem = ProvenanceItem    { fieldName :: Pa.Name        
                                        , toUniversal ::  MtToUniversalFunc
                                        , fromUniversal :: MtFromUniversalFunc
                                        , tenantField :: Pa.ScalarExpr  -- Identifier
                                        , converted :: Bool
                                        , shouldConvert :: Bool
                                        } deriving (Show, Eq)

-- the key used in the provenance is the attribute name 
type Provenance = MM.MultiMap MtAttributeName ProvenanceItem
emptyProvenance :: Provenance
emptyProvenance = MM.empty

-- helper functions for provenance 

addToProvenance :: MtAttributeName -> ProvenanceItem -> Provenance -> Provenance
addToProvenance n i p   = MM.insert n i p

addIdentifierToProvenance :: Provenance -> ConversionFunctionsTriple -> Pa.ScalarExpr -> Pa.ScalarExpr -> Bool -> Bool -> Provenance
addIdentifierToProvenance p (to,from,(_, Just attName)) (Pa.Identifier _ i) tenant convert should =
    let pItem = ProvenanceItem {fieldName=i, toUniversal=to, fromUniversal=from,
        tenantField=tenant, converted = convert, shouldConvert = should}
    in  MM.insert attName pItem p
-- default... check if that is correct
addIdentifierToProvenance p _ _ _ _ _ = p

getProvenanceItemFromName :: MtAttributeName -> Provenance -> [ProvenanceItem]
getProvenanceItemFromName attName = MM.lookup attName

type ProvItemAttPair = ([ProvenanceItem], MtAttributeName)
combineProvItemPairs :: ProvItemAttPair ->  ProvItemAttPair -> ProvItemAttPair
combineProvItemPairs ([], "") p = p
combineProvItemPairs p ([], "") = p
combineProvItemPairs p _        = p     -- if both are non-empty, they SHOULD have the same conversion function, attname does not matter that much

-- returns prov item - attributename pair from a (nested) scalar expr
-- prov item is a list which can be empty, typically, when called after pruning, it returns exactly one or zero items
getProvenanceItem :: Pa.ScalarExpr -> Provenance -> ([ProvenanceItem], MtAttributeName)
getProvenanceItem (Pa.App _ _ l) prov   = foldl1 combineProvItemPairs (map (\x -> getProvenanceItem x prov) l)
getProvenanceItem (Pa.BinaryOp _ _ e1 e2) prov =
    let n1  = getProvenanceItem e1 prov
        n2  = getProvenanceItem e2 prov
    in combineProvItemPairs n1 n2
getProvenanceItem (Pa.Identifier _ (Pa.Name _ nameComps)) prov =
    let (Pa.Nmc attName)    = last nameComps
    in  (getProvenanceItemFromName attName prov, attName)
-- default for literals and other non-relevant things
getProvenanceItem _ _ = ([], "")

-- replaces the provenance item identified with attribute name by a new version
replaceProvenanceItem :: Provenance -> MtAttributeName -> ProvenanceItem -> Provenance
replaceProvenanceItem prov attName item = MM.insert attName item (MM.delete attName prov)

-- simply collapses multi-map to map by giving priority to select items (which are typically the last item added per key)
flattenProvenance :: Provenance -> Provenance
flattenProvenance prov = MM.fromList (map (\(k,as) -> (k, last as)) (MM.assocs prov))

-- decides what to do with provenance hints (Provenance items with ShouldConvert true, but converted false): for the moment we just sort them out
pruneProvenance :: Provenance -> Provenance
pruneProvenance prov =
    let pList = MM.toList prov
        prune ((k,p):ps)= 
            let c   = converted(p)
                sc  = shouldConvert(p)
                pin False True  = prune ps
                pin _ _         = (k,p):(prune ps)
            in pin c sc
        prune []    = []
    in  MM.fromList (prune pList)

-- only keeps the hints and prunes the rest (used in scalar sub queries)
-- basically the invert of pruneProvenance
keepRecommendations :: Provenance -> Provenance
keepRecommendations prov =
    let pList = MM.toList prov
        prune ((k,p):ps)= 
            let c   = converted(p)
                sc  = shouldConvert(p)
                pin False True  = (k,p):(prune ps)
                pin _ _         = prune ps
            in pin c sc
        prune []    = []
    in  MM.fromList (prune pList)

-- allows MtRewriteSelect and MtRewriteWhere to call recurively back into MtLib using a Function rather than an import
type RewriteQueryFun = MtSchemaSpec -> MtSetting -> Provenance -> Pa.QueryExpr -> Pa.TableRefList -> Either MtRewriteError (Provenance, Pa.QueryExpr)
type CasesType = Pa.CaseScalarExprListScalarExprPairList

-- used for looking up table and attribute names of an Identifier
type TableAttributePair = (Maybe MtTableName, Maybe MtAttributeName)

getTenantAttributeName :: MtTableName -> MtAttributeName
getTenantAttributeName s = s ++ "_tenant_key"

-- takes a (alias of a) table name and an mt table name and constructs the corresponding identifier
getTenantIdentifier :: String -> MtTableName -> Pa.ScalarExpr
getTenantIdentifier tName mtName = Pa.Identifier A.emptyAnnotation $ Pa.Name A.emptyAnnotation [Pa.Nmc tName, Pa.Nmc $ getTenantAttributeName mtName]

-- creates a unique, reasonable name for intermediate tenant keys
getIntermediateTenantIdentifier :: Pa.ScalarExpr -> String
getIntermediateTenantIdentifier (Pa.Identifier _ (Pa.Name _ nameComps)) =
    foldl (\s1 (Pa.Nmc s2) -> (s1 ++ "_" ++ s2)) "tk_" nameComps

-- checks whether a table is global. Assumes anything not in the schema spec is also global
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

createConvFunctionApplication :: String -> Pa.ScalarExpr -> Pa.ScalarExpr -> Pa.ScalarExpr
createConvFunctionApplication funcName exp1 exp2 =
    Pa.App A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc funcName])[exp1, exp2]

type ConversionFunctionsTriple = (MtToUniversalFunc, MtFromUniversalFunc, TableAttributePair)
-- gets the conversion functions and the tablename/attname pair for a specific name of an identifier
getConversionFunctions :: MtSchemaSpec -> Pa.TableRefList -> Pa.Name -> Maybe ConversionFunctionsTriple
getConversionFunctions s t i  =
    let pair = getTableAndAttName i
        comparability = lookupAttributeComparability s pair t
        result (Just (MtConvertible to from))   = Just (to, from, pair)
        result _                                = Nothing
    in result comparability

isComparisonOp :: Pa.ScalarExpr -> Bool
isComparisonOp (Pa.BinaryOp _ (Pa.Name _ [Pa.Nmc opName]) _ _) = opName `elem` ["=", "<>", "<", ">", ">=", "<="]
isComparisonOp (Pa.InPredicate _ _ _ (Pa.InList _ _)) = True
isComparisonOp _ = False

isSqlAggOp :: Pa.Name -> Bool
isSqlAggOp (Pa.Name _ [Pa.Nmc n]) = (T.toLower (T.pack n)) `elem` ["sum", "count", "avg", "min", "max"]

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

