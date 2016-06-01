module MtLib
(
    MtFromUniversalFunc
    ,MtToUniversalFunc
    ,MtAttributeComparability(..)
    ,MtAttributeName
    ,MtSpecificTable
    ,MtTableSpec(..)
    ,MtTableName
    ,MtSchemaSpec
    ,MtClient
    ,MtDataSet
    ,MtSetting
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
    ,MtSqlTree
    ,mtParse
    ,mtPrettyPrint
    ,mtRewrite
) where

import qualified Data.Map as M
-- import qualified Data.Set as S

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A
import qualified Data.Text.Lazy as L
import qualified Database.HsSqlPpp.Pretty as Pr

-- ##################################
-- MT types and type classes
-- ##################################

type MtFromUniversalFunc        = String
type MtToUniversalFunc          = String

data MtAttributeComparability   =
    MtComparable
    | MtTransformable MtToUniversalFunc MtFromUniversalFunc
    | MtSpecific

type MtAttributeName            = String
type MtSpecificTable            = M.Map MtAttributeName MtAttributeComparability
data MtTableSpec                = MtGlobalTable | FromMtSpecificTable MtSpecificTable

type MtTableName                = String
type MtSchemaSpec               = M.Map MtTableName MtTableSpec

type MtClient                   = Int
type MtDataSet                  = [MtClient]
type MtSetting                  = (MtClient, MtDataSet)

-- ##################################
-- Type Construction helper functions
-- ##################################

mtSchemaSpecFromList :: [(MtTableName, MtTableSpec)] -> MtSchemaSpec
mtSchemaSpecFromList = M.fromList

mtSpecificTableFromList :: [(MtAttributeName, MtAttributeComparability)] -> MtSpecificTable
mtSpecificTableFromList = M.fromList

-- ##################################
-- Parsing and Printing
-- ##################################

type MtSqlTree = Either Pa.ParseErrorExtra Pa.QueryExpr

mtParse :: String -> MtSqlTree
mtParse query = Pa.parseQueryExpr
                    Pa.defaultParseFlags
                    "source"
                    (Just (0,0))
                    (L.pack query)

mtPrettyPrint :: MtSqlTree -> String
mtPrettyPrint (Left err) = show err
mtPrettyPrint (Right query) = L.unpack $ Pr.prettyQueryExpr Pr.defaultPrettyFlags query

-- ##################################
-- MT Rewrite
-- ##################################

-- general concepts and assmptions:
-- push Dataset filtering down to base relations -> every subquery is aleady filtered
-- the result of every subquery is presented already in client format
-- predicates in where clauses are also presented in client format
-- 
-- Focuses on simplicity and correctness (including tenant keys in the write places and alike...)
-- This design has the advantage that it is simple and that we do not have to track any intermediary state
-- This means it is context-free and somehow self-conained... or in other words every (sub-query) is in MT format
-- All the other things should be deferred to optimizations

mtRewrite :: MtSchemaSpec -> MtSetting -> String -> MtSqlTree
mtRewrite spec setting query = do
    parsedQuery <- mtParse query
    let annotatedQuery = mtAnnotate spec parsedQuery
    rewrittenQuery <- mtRewriteQuery spec setting annotatedQuery
    Right rewrittenQuery

-- helper types and functions
type TableAttributePair = (Maybe MtTableName, Maybe MtAttributeName)

getTenantAttributeName :: MtTableName -> MtAttributeName
getTenantAttributeName s = s ++ "_TENANT_KEY"

-- takes a (alias of a) table name and an mt table name and constructs the corresponding identifier
getTenantIdentifier :: String -> MtTableName -> Pa.ScalarExpr
getTenantIdentifier tName mtName = Pa.Identifier A.emptyAnnotation $ Pa.Name A.emptyAnnotation [Pa.Nmc tName, Pa.Nmc $ getTenantAttributeName mtName]

isGlobalTable :: MtSchemaSpec -> MtTableName -> Bool
isGlobalTable spec tName =
    let tableSpec = M.lookup tName spec
        analyse (Just MtGlobalTable)    = True
        analyse _                       = False
    in analyse tableSpec

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
lookupAttributeComparability :: MtSchemaSpec -> TableAttributePair -> Maybe MtAttributeComparability
lookupAttributeComparability spec (tableName, attributeName) = do
    attName <- attributeName
    tName <- tableName
    (FromMtSpecificTable tableSpec) <- M.lookup tName spec
    tSpec <- Just tableSpec
    M.lookup attName tSpec

-- checks whether a string contains a certain substring
containsString :: String -> String ->Bool
containsString l s = containsString' l s True where
    containsString' _ [] _          = True
    containsString' [] _ _          = False
    containsString' (x:xs) (y:ys) h = (y == x && containsString' xs ys False) || (h && containsString' xs (y:ys) h)

-- the main rewrite functions
mtRewriteQuery :: MtSchemaSpec -> MtSetting -> Pa.QueryExpr -> MtSqlTree
mtRewriteQuery spec (c,ds) (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) = do
        newTrefs         <- mtRewriteTrefList spec (c,ds) selTref
        newSelectList    <- mtRewriteSelectList spec (c,ds) selSelectList
        newWhere         <- mtRewriteMaybeScalarExpr spec (c,ds) selWhere
        adjustedWhere    <- mtAdjustWhereClause spec newWhere
        let filteredWhere = mtFilterWhereClause spec ds selTref adjustedWhere
        let newGroupBy    = mtRewriteGroupByClause spec selGroupBy
        newHaving        <- mtRewriteMaybeScalarExpr spec (c,ds) selHaving
        let newOrderBy    = mtRewriteOrderByClause spec selOrderBy
        Right $ Pa.Select ann selDistinct newSelectList newTrefs filteredWhere
            newGroupBy newHaving newOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtRewriteQuery _ _ query = Right query

mtRewriteTrefList :: MtSchemaSpec -> MtSetting -> Pa.TableRefList -> Either Pa.ParseErrorExtra Pa.TableRefList
mtRewriteTrefList spec setting (Pa.SubTref ann sel:trefs) = do
    h <- mtRewriteQuery spec setting sel
    t <- mtRewriteTrefList spec setting trefs
    Right $ Pa.SubTref ann h : t
mtRewriteTrefList spec setting (Pa.TableAlias ann tb tref:trefs) = do
    h <- mtRewriteTrefList spec setting [tref]
    t <- mtRewriteTrefList spec setting trefs
    Right $ Pa.TableAlias ann tb (head h) : t
mtRewriteTrefList spec setting (tref:trefs) = do
    t <- mtRewriteTrefList spec setting trefs
    Right $ tref : t
mtRewriteTrefList _ _ [] = Right []

mtRewriteSelectList :: MtSchemaSpec -> MtSetting -> Pa.SelectList -> Either Pa.ParseErrorExtra Pa.SelectList
mtRewriteSelectList spec setting (Pa.SelectList ann items) = do
    t <- mtRewriteSelectItems spec setting items
    Right $ Pa.SelectList ann t

mtRewriteSelectItems :: MtSchemaSpec -> MtSetting -> [Pa.SelectItem] -> Either Pa.ParseErrorExtra [Pa.SelectItem]
-- default case, recursively call rewrite on single item
mtRewriteSelectItems spec setting (item:items) = do
    h <- mtRewriteSelectItem spec setting item
    t <- mtRewriteSelectItems spec setting items
    Right $ h : t
mtRewriteSelectItems _ _ [] = Right []

mtRewriteSelectItem :: MtSchemaSpec -> MtSetting -> Pa.SelectItem -> Either Pa.ParseErrorExtra Pa.SelectItem
-- todo: if outer-most scalar expr of SelExp is an transformationApp, then we have to rename it properly using a select item...
mtRewriteSelectItem spec setting (Pa.SelExp ann scalExp)             = do
    newExp <- mtRewriteScalarExpr spec setting scalExp
    let create (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                    [Pa.Identifier a4 i, arg0]
                ,Pa.NumberLit a5 arg1]) 
                    | containsString to "ToUniversal" && containsString from "FromUniversal"   = let (_ , Just attName) = getTableAndAttName i
                        in  Pa.SelectItem ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1]) (Pa.Nmc attName)
                    | otherwise                                                         =
                        Pa.SelExp ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1])
        create expr                                                                     = Pa.SelExp ann expr
    Right $ create newExp
mtRewriteSelectItem spec setting (Pa.SelectItem ann scalExp newName) = do
    h <- mtRewriteScalarExpr spec setting scalExp
    Right $ Pa.SelectItem ann h newName

-- adds a dataset filter for a specific table to an existing where predicate
-- only adds filter for tables that are tenant-specific
addDFilter :: MtSchemaSpec -> MtDataSet -> Pa.TableRef -> Maybe Pa.ScalarExpr -> Maybe MtTableName -> Maybe Pa.ScalarExpr
addDFilter spec ds (Pa.Tref _ (Pa.Name nAnn nameList)) whereClause priorName =
    let (Pa.Nmc tName)      = last nameList
        isGlobal            = isGlobalTable spec tName
        newName (Just n)    = n
        newName Nothing     = tName
        inPred
            | isGlobal                  = Nothing
            | otherwise                 = Just $ Pa.InPredicate nAnn (getTenantIdentifier (newName priorName) tName) True
                                            (Pa.InList A.emptyAnnotation (map (Pa.NumberLit A.emptyAnnotation . show) ds))
        addTo (Just expr) (Just pr)     = Just $ Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "and"]) expr pr
        addTo Nothing (Just pr)         = Just pr
        addTo (Just expr) Nothing       = Just expr
        addTo Nothing Nothing           = Nothing
    in addTo whereClause inPred
addDFilter spec ds (Pa.TableAlias _ (Pa.Nmc newName) tref) whereClause priorName =
    let finalName (Just n)  = n
        finalName Nothing   = newName
    in  addDFilter spec ds tref whereClause (Just $ finalName priorName)
addDFilter _ _ _ whereClause _ = whereClause

-- checks the join predicates and adds necessary constraints as necessary
-- right now only checks simple (non-nested and non-complex) join predicates
mtAdjustWhereClause :: MtSchemaSpec -> Maybe Pa.ScalarExpr -> Either Pa.ParseErrorExtra (Maybe Pa.ScalarExpr)
mtAdjustWhereClause spec (Just whereClause) = do
    adjustedExp <- mtAdjustScalarExpr spec whereClause
    Right $ Just adjustedExp
mtAdjustWhereClause _ whereClause = Right whereClause

-- adds tenant identifier for mt-specific predicates
mtAdjustJoinPredicate :: MtTableName -> MtTableName -> String -> Pa.ScalarExpr -> Pa.ScalarExpr
mtAdjustJoinPredicate t0 t1 opName expr
    | opName == "<>"    = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "or"])
                            (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "<>"])
                                (getTenantIdentifier t0 t0) (getTenantIdentifier t1 t1)) expr
    | otherwise         = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "and"])
                            (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "="])
                                (getTenantIdentifier t0 t0) (getTenantIdentifier t1 t1)) expr


mtAdjustScalarExpr :: MtSchemaSpec -> Pa.ScalarExpr -> Either Pa.ParseErrorExtra Pa.ScalarExpr
mtAdjustScalarExpr spec (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)) =
    let checkNecessary  = opName `elem` ["=", "<>", "<", ">", ">=", "<="]
        (t0, a0)        = getTableAndAttName n0
        comp0           = lookupAttributeComparability spec (t0, a0)
        (t1, a1)        = getTableAndAttName n1
        comp1           = lookupAttributeComparability spec (t1, a1)
        -- if both are specific, check if they are from same table
        adjust True (Just tn0) (Just MtSpecific) (Just tn1) (Just MtSpecific)
            | tn0 == tn1    = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)
            | otherwise     = Right $ mtAdjustJoinPredicate tn0 tn1 opName
                                (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1))
        adjust True _ (Just MtSpecific) _ (Just MtSpecific) = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1) -- add parse error here later

        adjust True _ (Just MtSpecific) _ _ = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1) -- add parse error here later
        adjust True _ _ _ (Just MtSpecific) = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1) -- add parse error here later
        adjust _ _ _ _ _ = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1) -- add parse error here later
    in adjust checkNecessary t0 comp0 t1 comp1
mtAdjustScalarExpr spec (Pa.BinaryOp ann i arg0 arg1) = do
    h <- mtAdjustScalarExpr spec arg0
    t <- mtAdjustScalarExpr spec arg1
    Right $ Pa.BinaryOp ann i h t
mtAdjustScalarExpr spec (Pa.PrefixOp ann opName arg) = do
    h <- mtAdjustScalarExpr spec arg
    Right $ Pa.PrefixOp ann opName h
mtAdjustScalarExpr _ expr = Right expr

-- adds the necessary dataset filter to a where clause
mtFilterWhereClause :: MtSchemaSpec -> MtDataSet -> Pa.TableRefList -> Maybe Pa.ScalarExpr  -> Maybe Pa.ScalarExpr
mtFilterWhereClause spec ds (tref:trefs) whereClause = mtFilterWhereClause spec ds trefs (addDFilter spec ds tref whereClause Nothing)
mtFilterWhereClause _ _ [] whereClause = whereClause

-- ommmits the table name of an attribute if necessary, this is actually the case for transformable attributre in group- and order-by clauses
omitIfNecessary :: MtSchemaSpec -> Pa.ScalarExpr -> Pa.ScalarExpr
omitIfNecessary spec (Pa.Identifier iAnn name) =
    let (tName, attName) = getTableAndAttName name
        comp = lookupAttributeComparability spec (tName, attName)
        (Just aName) = attName
        applyIf (Just (MtTransformable _ _)) = Pa.Name A.emptyAnnotation [Pa.Nmc aName]
        applyIf _ = name
    in Pa.Identifier iAnn (applyIf comp)
omitIfNecessary _ expr = expr

mtRewriteGroupByClause :: MtSchemaSpec -> [Pa.ScalarExpr] -> [Pa.ScalarExpr]
mtRewriteGroupByClause spec = map (omitIfNecessary spec)

mtRewriteOrderByClause :: MtSchemaSpec -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
mtRewriteOrderByClause spec ((expr, dir, no):list) = (omitIfNecessary spec expr, dir, no) : mtRewriteOrderByClause spec list
mtRewriteOrderByClause _ [] = []

mtRewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Either Pa.ParseErrorExtra (Maybe Pa.ScalarExpr)
mtRewriteMaybeScalarExpr spec setting (Just expr) = do
    h <- mtRewriteScalarExpr spec setting expr
    Right $ Just h
mtRewriteMaybeScalarExpr _ _ Nothing = Right Nothing

mtRewriteScalarExprList :: MtSchemaSpec -> MtSetting -> [Pa.ScalarExpr] -> Either Pa.ParseErrorExtra [Pa.ScalarExpr]
mtRewriteScalarExprList spec setting (arg:args) = do
    newArg <- mtRewriteScalarExpr spec setting arg
    newArgs <- mtRewriteScalarExprList spec setting args
    Right (newArg : newArgs)
mtRewriteScalarExprList _ _ [] = Right []

mtRewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Pa.ScalarExpr -> Either Pa.ParseErrorExtra Pa.ScalarExpr
mtRewriteScalarExpr spec setting (Pa.PrefixOp ann opName arg) = do
    h <- mtRewriteScalarExpr spec setting arg
    Right $ Pa.PrefixOp ann opName h
mtRewriteScalarExpr spec setting (Pa.PostfixOp ann opName arg) = do
    h <- mtRewriteScalarExpr spec setting arg
    Right $ Pa.PostfixOp ann opName h
mtRewriteScalarExpr spec setting (Pa.BinaryOp ann opName arg0 arg1) = do
    b1 <- mtRewriteScalarExpr spec setting arg0
    b2 <- mtRewriteScalarExpr spec setting arg1
    Right $ Pa.BinaryOp ann opName b1 b2
mtRewriteScalarExpr spec setting (Pa.SpecialOp ann opName args) = do
    l <- mtRewriteScalarExprList spec setting args
    Right $ Pa.SpecialOp ann opName l
mtRewriteScalarExpr spec setting (Pa.App ann funName args) = do
    l <- mtRewriteScalarExprList spec setting args
    Right $ Pa.App ann funName l
mtRewriteScalarExpr spec setting (Pa.Parens ann expr) = do
    h <- mtRewriteScalarExpr spec setting expr
    Right $ Pa.Parens ann h
mtRewriteScalarExpr spec setting (Pa.InPredicate ann expr i list) = do
    h <- mtRewriteScalarExpr spec setting expr
    Right $ Pa.InPredicate ann h i list
mtRewriteScalarExpr spec setting (Pa.Exists ann sel) = do
    h <- mtRewriteQuery spec setting sel
    Right $ Pa.Exists ann h
mtRewriteScalarExpr spec setting (Pa.ScalarSubQuery ann sel) = do
    h <- mtRewriteQuery spec setting sel
    Right $ Pa.ScalarSubQuery ann h
mtRewriteScalarExpr spec (c,_) (Pa.Identifier iAnn i) =
    let (tableName, attName) = getTableAndAttName i
        comparability = lookupAttributeComparability spec (tableName, attName)
        rewrite (Just (MtTransformable to from)) (Just tName) =
            Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from])
                [Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to])
                    [Pa.Identifier iAnn i, getTenantIdentifier tName tName]
                ,Pa.NumberLit iAnn (show c)]
        rewrite _ _ = Pa.Identifier iAnn i
    in Right $ rewrite comparability tableName 
mtRewriteScalarExpr _ _ expr = Right expr

-- ##################################
-- MT Annotate
-- ##################################

-- make sure that every attribute of a specific table appears with its table name
-- assumes that attributes not found within the schema are from global tables
mtAnnotate :: MtSchemaSpec -> Pa.QueryExpr -> Pa.QueryExpr
mtAnnotate spec (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
        let newSelectList   = mtAnnotateSelectList spec selTref selSelectList
            newTrefs        = mtAnnotateTrefList spec selTref
            newWhere        = mtAnnotateMaybeScalarExpr spec selTref selWhere
            newGroupBy      = map (mtAnnotateScalarExpr spec selTref) selGroupBy
            newHaving       = mtAnnotateMaybeScalarExpr spec selTref selHaving
            newOrderBy      = mtAnnotateDirectionList spec selTref selOrderBy
        in  Pa.Select ann selDistinct newSelectList newTrefs newWhere
            newGroupBy newHaving newOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtAnnotate _ query = query

mtAnnotateTrefList :: MtSchemaSpec -> Pa.TableRefList -> Pa.TableRefList
mtAnnotateTrefList spec (Pa.SubTref ann sel:trefs) = Pa.SubTref ann (mtAnnotate spec sel) : mtAnnotateTrefList spec trefs
mtAnnotateTrefList spec (Pa.TableAlias ann tb tref:trefs) = Pa.TableAlias ann tb (head (mtAnnotateTrefList spec [tref])) : mtAnnotateTrefList spec trefs
-- default case, recursively call annotation call on single item
mtAnnotateTrefList spec (tref:trefs) = tref:mtAnnotateTrefList spec trefs
mtAnnotateTrefList _ [] = []

mtAnnotateSelectList :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtAnnotateSelectList spec trefs (Pa.SelectList ann items) = Pa.SelectList ann (mtAnnotateSelectItems spec trefs items)

mtAnnotateSelectItems :: MtSchemaSpec -> Pa.TableRefList -> [Pa.SelectItem] -> [Pa.SelectItem]
-- replaces Star Expressions with an enumeration of all attributes instead (* would also display tenant key, which is something we do not want)
mtAnnotateSelectItems spec [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] [Pa.SelExp selAnn (Pa.Star starAnn)] =
    let tableSpec = M.lookup tname spec
        generate (Just (FromMtSpecificTable tab)) = map (\key -> Pa.SelExp selAnn (Pa.Identifier starAnn (Pa.Name starAnn [Pa.Nmc key]))) (M.keys tab)
        generate _ = [Pa.SelExp selAnn (Pa.Star starAnn)]
    in mtAnnotateSelectItems spec [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] (generate tableSpec)
-- default case, recursively call annotation call on single item
mtAnnotateSelectItems spec trefs (item:items) = mtAnnotateSelectItem spec trefs item : mtAnnotateSelectItems spec trefs items
mtAnnotateSelectItems _ _ [] = []

mtAnnotateSelectItem :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectItem -> Pa.SelectItem
mtAnnotateSelectItem spec trefs (Pa.SelExp ann scalExp)             = Pa.SelExp ann (mtAnnotateScalarExpr spec trefs scalExp)
mtAnnotateSelectItem spec trefs (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (mtAnnotateScalarExpr spec trefs scalExp) newName

mtAnnotateMaybeScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
mtAnnotateMaybeScalarExpr spec trefs (Just expr) = Just (mtAnnotateScalarExpr spec trefs expr)
mtAnnotateMaybeScalarExpr _ _ Nothing = Nothing

mtAnnotateDirectionList :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
mtAnnotateDirectionList spec trefs ((expr, dir, no):list) = (mtAnnotateScalarExpr spec trefs expr, dir, no) : mtAnnotateDirectionList spec trefs list
mtAnnotateDirectionList _ _ [] = []

mtAnnotateScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExpr -> Pa.ScalarExpr
mtAnnotateScalarExpr spec treflist (Pa.PrefixOp ann opName arg) = Pa.PrefixOp ann opName (mtAnnotateScalarExpr spec treflist arg)
mtAnnotateScalarExpr spec treflist (Pa.PostfixOp ann opName arg) = Pa.PostfixOp ann opName (mtAnnotateScalarExpr spec treflist arg)
mtAnnotateScalarExpr spec treflist (Pa.BinaryOp ann opName arg0 arg1) = Pa.BinaryOp ann opName
        (mtAnnotateScalarExpr spec treflist arg0) (mtAnnotateScalarExpr spec treflist arg1)
mtAnnotateScalarExpr spec treflist (Pa.SpecialOp ann opName args) = Pa.SpecialOp ann opName (map (mtAnnotateScalarExpr spec treflist) args)
mtAnnotateScalarExpr spec treflist (Pa.App ann funName args) = Pa.App ann funName (map (mtAnnotateScalarExpr spec treflist) args)
mtAnnotateScalarExpr spec treflist (Pa.Parens ann expr) = Pa.Parens ann (mtAnnotateScalarExpr spec treflist expr)
mtAnnotateScalarExpr spec treflist (Pa.InPredicate ann expr i list) = Pa.InPredicate ann (mtAnnotateScalarExpr spec treflist expr) i list
mtAnnotateScalarExpr spec _ (Pa.Exists ann sel) = Pa.Exists ann (mtAnnotate spec sel)
mtAnnotateScalarExpr spec _ (Pa.ScalarSubQuery ann sel) = Pa.ScalarSubQuery ann (mtAnnotate spec sel)
mtAnnotateScalarExpr spec (Pa.Tref _ (Pa.Name _ [Pa.Nmc tname]):trefs) (Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName:nameComps)))
    | not (null nameComps)  = Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName : nameComps))
    | otherwise             =
        let tableSpec = M.lookup tname spec
            containsAttribute (Just (FromMtSpecificTable attrMap)) aName = M.member aName attrMap 
            containsAttribute _ _ = False
            contains = containsAttribute tableSpec attName
            propagate False = mtAnnotateScalarExpr spec trefs (Pa.Identifier iAnn (Pa.Name a [Pa.Nmc attName]))
            propagate True  = Pa.Identifier iAnn (Pa.Name a [Pa.Nmc tname,Pa.Nmc attName])
        in  propagate contains
-- default case handles anything we do not handle so far
mtAnnotateScalarExpr _ _ expr = expr

-- ##################################
-- MT Optimizer
-- ##################################
--
-- Some preliminary ideas for optimizer steps
-- Observations:
--      - some optimizations should happen in conjunction with rewrite (i) (iv)
--      - some optimizations need transformation provenance (iv) to (vi)
--
-- (i)      D=C optimization ==> only filter at the bottom level, rest of the query looks the same, no transformation needed
-- (ii)     |D| = 1: similar to i), but additionally requires a final presentation to client view at the uppermost select
-- (iii)    Client Presentation push-up: push-up transformation to client format to the uppermost select
--          --> comparisons are always done in universal format, constants need to be brought into universal format as well
-- (iv)     Transformation push-up: do comparisons in owner's format --> needs to transform constant twice --> only transform on joins or aggregations
-- (v)      Aggregation distribution: if possible, compute partial aggregations on different client formats and then only transfrom these partial results
-- (vi)     Statistical aggregation optimization: if (v) not possible: figure out in (intermediary) formats to transform values (essentially equivalent to join ordering and site selection in distributed query processing

