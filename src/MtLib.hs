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
    ,MtOptimization(..)
    ,MtSetting
    ,MtRewriteResult
    ,mtOptimizationsFromList
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
    ,mtParse
    ,mtPrettyPrint
    ,mtCompactPrint
    ,mtRewrite
    ,D.Dialect(..)
    ,D.ansiDialect
    ,D.postgresDialect
    ,D.sqlServerDialect
    ,D.oracleDialect
) where

-- import qualified Data.Map as M --> already imported from somewhere else
-- import qualified Data.Set as S --> already imported from somewhere else
import qualified Data.Text.Lazy as L

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A
import qualified Database.HsSqlPpp.Pretty as Pr
import qualified Database.HsSqlPpp.Dialect as D

import MtTypes
import MtUtils
import MtAnnotate

-- ##################################
-- Parsing and Printing
-- ##################################

data MtRewriteError = FromParseError Pa.ParseErrorExtra | FromMtRewriteError String
instance Show MtRewriteError where
    show (FromParseError err)     = show err
    show (FromMtRewriteError err) = "MTSQL-ERROR: " ++ err

type MtRewriteResult = Either MtRewriteError Pa.Statement

toMtRewriteResult :: Either Pa.ParseErrorExtra [Pa.Statement] -> MtRewriteResult
toMtRewriteResult (Left err) = Left $ FromParseError err
toMtRewriteResult (Right res)= Right $ head res

mtParse :: String -> D.Dialect -> MtRewriteResult
mtParse query dialect = toMtRewriteResult $ Pa.parseStatements
                    (Pa.ParseFlags dialect)
                    "source"
                    (Just (0,0))
                    (L.pack query)

mtPrettyPrint :: MtRewriteResult -> D.Dialect -> String
mtPrettyPrint (Left err) _ = show err
mtPrettyPrint (Right statement) dialect = L.unpack $ Pr.prettyStatements (Pr.PrettyFlags dialect) [statement]

mtCompactPrint :: MtRewriteResult -> D.Dialect -> String
mtCompactPrint result dialect =
    let prettyResult = mtPrettyPrint result dialect
        clearSpaces ('\n':word) = clearSpaces (' ':word)
        clearSpaces (' ':' ':word) = clearSpaces (' ':word)
        clearSpaces (c:word) = c:clearSpaces word
        clearSpaces _ = []
    in  clearSpaces (map (\c -> if c=='\n' then ' '; else c) prettyResult)

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

-- parses a single SQL statement terminated by ;
mtRewrite :: MtSchemaSpec -> MtSetting -> String -> D.Dialect -> MtRewriteResult
mtRewrite spec setting statement dialect = do
    parsedStatement <- mtParse statement dialect
    let annotatedStatement = mtAnnotateStatement spec parsedStatement
    -- Right annotatedStatement -- DEBUG
    rewrittenStatement <- mtRewriteStatement spec setting annotatedStatement
    Right rewrittenStatement

-- helper types and functions

mtRewriteStatement :: MtSchemaSpec -> MtSetting -> Pa.Statement -> MtRewriteResult
mtRewriteStatement spec setting (Pa.QueryStatement a q) = do
    rewrittenQuery <- mtRewriteQuery spec setting q []
    Right $ Pa.QueryStatement a rewrittenQuery
mtRewriteStatement spec setting (Pa.CreateView a n c q) = do
    rewrittenQuery <- mtRewriteQuery spec setting q []
    Right $ Pa.CreateView a n c rewrittenQuery
mtRewriteStatement _ _ statement = Right statement

-- the main rewrite function, for the case it is used in subqueries, it takes also the table refs from the outer queries into account
mtRewriteQuery :: MtSchemaSpec -> MtSetting -> Pa.QueryExpr -> Pa.TableRefList -> Either MtRewriteError Pa.QueryExpr
mtRewriteQuery spec (c,d,o) (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) trefs = do
        let allTrefs      = selTref ++ trefs    -- ordering matters here as the first value that fits is taken
        newTrefs         <- mtRewriteTrefList spec (c,d,o) selTref
        newSelectList    <- mtRewriteSelectList spec (c,d,o) selSelectList selTref
        adjustedWhere    <- mtAdjustWhereClause spec selWhere allTrefs -- adds predicates on tenant keys
        convertedWhere   <- mtRewriteMaybeScalarExpr spec (c,d,o) adjustedWhere allTrefs -- adds conversion functions
        let filteredWhere = mtFilterWhereClause spec d selTref convertedWhere -- adds D-filters
        let newGroupBy    = mtRewriteGroupByClause spec selTref selGroupBy
        newHaving        <- mtRewriteMaybeScalarExpr spec (c,d,o) selHaving selTref
        let newOrderBy    = mtRewriteOrderByClause spec selTref selOrderBy
        Right $ Pa.Select ann selDistinct newSelectList newTrefs filteredWhere
            newGroupBy
            newHaving
            newOrderBy
            selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtRewriteQuery _ _ query _ = Right query

mtRewriteTrefList :: MtSchemaSpec -> MtSetting -> Pa.TableRefList -> Either MtRewriteError Pa.TableRefList
mtRewriteTrefList spec setting (Pa.SubTref ann sel:trefs) = do
    h <- mtRewriteQuery spec setting sel []
    t <- mtRewriteTrefList spec setting trefs
    Right $ Pa.SubTref ann h : t
mtRewriteTrefList spec setting (Pa.TableAlias ann tb tref:trefs) = do
    h <- mtRewriteTrefList spec setting [tref]
    t <- mtRewriteTrefList spec setting trefs
    Right $ Pa.TableAlias ann tb (head h) : t
mtRewriteTrefList spec (c,d,o) (Pa.JoinTref ann tref0 n t h tref1 (Just (Pa.JoinOn a expr)):trefs) = do
    l <- mtRewriteTrefList spec (c,d,o) [tref0]
    r <- mtRewriteTrefList spec (c,d,o) [tref1]
    let allTrefs = [tref0, tref1]
    (Just adjusted) <- mtAdjustWhereClause spec (Just expr) allTrefs
    converted <- mtRewriteScalarExpr spec (c,d,o) adjusted allTrefs
    let (Just filtered) = mtFilterWhereClause spec d allTrefs (Just converted)
    rest <- mtRewriteTrefList spec (c,d,o) trefs
    Right $ Pa.JoinTref ann (head l) n t h (head r) (Just (Pa.JoinOn a filtered)) : rest
mtRewriteTrefList spec setting (Pa.FullAlias ann tb cols tref:trefs) = do
    h <- mtRewriteTrefList spec setting [tref]
    l <- mtRewriteTrefList spec setting trefs
    Right $ Pa.FullAlias ann tb cols (head h) : l
mtRewriteTrefList spec setting (tref:trefs) = do
    t <- mtRewriteTrefList spec setting trefs
    Right $ tref : t
mtRewriteTrefList _ _ [] = Right []

-- adds a dataset filter for a specific table to an existing where predicate
-- only adds filter for tables that are tenant-specific
addDFilter :: MtSchemaSpec -> MtDataSet -> Pa.TableRef -> Maybe Pa.ScalarExpr -> Maybe MtTableName -> Maybe Pa.ScalarExpr
addDFilter spec ds (Pa.Tref _ (Pa.Name nAnn nameList)) whereClause priorName =
    let (Pa.Nmc tName)      = last nameList
        isGlobal            = isGlobalTable spec (Just tName)
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

-- checks the join predicates and adds necessary constraints
-- right now only checks simple (non-nested and non-complex) join predicates
mtAdjustWhereClause :: MtSchemaSpec -> Maybe Pa.ScalarExpr -> Pa.TableRefList-> Either MtRewriteError (Maybe Pa.ScalarExpr)
mtAdjustWhereClause spec (Just whereClause) trefs = do
    adjustedExp <- mtAdjustScalarExpr spec whereClause trefs
    Right $ Just adjustedExp
mtAdjustWhereClause _ whereClause _ = Right whereClause

-- adds tenant identifier for mt-specific predicates
mtAdjustJoinPredicate :: MtTableName -> MtTableName -> String -> Pa.ScalarExpr -> Pa.TableRefList -> Pa.ScalarExpr
mtAdjustJoinPredicate t0 t1 opName expr trefs =
    let (Just old0) = getOldTableName (Just t0) trefs
        (Just old1) = getOldTableName (Just t1) trefs
        exec
            | opName == "<>"    = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "or"])
                                    (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "<>"])
                                        (getTenantIdentifier t0 old0) (getTenantIdentifier t1 old1)) expr
            | otherwise         = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "and"])
                                    (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "="])
                                        (getTenantIdentifier t0 old0) (getTenantIdentifier t1 old1)) expr
    in exec


mtAdjustScalarExpr :: MtSchemaSpec -> Pa.ScalarExpr -> Pa.TableRefList -> Either MtRewriteError Pa.ScalarExpr
mtAdjustScalarExpr spec (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)) trefs =
    let checkNecessary  = opName `elem` ["=", "<>", "<", ">", ">=", "<="]
        (t0, a0)        = getTableAndAttName n0
        comp0           = lookupAttributeComparability spec (t0, a0) trefs
        (t1, a1)        = getTableAndAttName n1
        comp1           = lookupAttributeComparability spec (t1, a1) trefs
        -- if both are specific, check if they are from same table
        adjust True (Just tn0) (Just MtSpecific) (Just tn1) (Just MtSpecific)
            | tn0 == tn1    = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)
            | otherwise     = Right $ mtAdjustJoinPredicate tn0 tn1 opName
                                (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)) trefs
        adjust True _ (Just MtSpecific) _ _ = Left $ FromMtRewriteError ("cannot compare tenant-specific attribute "
            ++ printName n0 ++ " with non-specific attribute " ++ printName n1 ++ "!")
        adjust True _ _ _ (Just MtSpecific) = Left $ FromMtRewriteError ("cannot compare tenant-specific attribute "
            ++ printName n1 ++ " with non-specific attribute " ++ printName n0 ++ "!")
        adjust _ _ _ _ _ = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)
    in adjust checkNecessary t0 comp0 t1 comp1
mtAdjustScalarExpr spec (Pa.BinaryOp ann i arg0 arg1) trefs = do
    h <- mtAdjustScalarExpr spec arg0 trefs
    t <- mtAdjustScalarExpr spec arg1 trefs
    Right $ Pa.BinaryOp ann i h t
mtAdjustScalarExpr spec (Pa.PrefixOp ann opName arg) trefs= do
    h <- mtAdjustScalarExpr spec arg trefs
    Right $ Pa.PrefixOp ann opName h
mtAdjustScalarExpr _ expr _ = Right expr

-- adds the necessary dataset filter to a where clause
mtFilterWhereClause :: MtSchemaSpec -> MtDataSet -> Pa.TableRefList -> Maybe Pa.ScalarExpr  -> Maybe Pa.ScalarExpr
mtFilterWhereClause spec ds (tref:trefs) whereClause = mtFilterWhereClause spec ds trefs (addDFilter spec ds tref whereClause Nothing)
mtFilterWhereClause _ _ [] whereClause = whereClause

-- ommmits the table name of an attribute if necessary, this is actually the case for convertible attributre in group- and order-by clauses
omitIfNecessary :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExpr -> Pa.ScalarExpr
omitIfNecessary spec trefs (Pa.Identifier iAnn name) =
    let (tName, attName) = getTableAndAttName name
        comp = lookupAttributeComparability spec (tName, attName) trefs
        (Just aName) = attName
        applyIf (Just (MtConvertible _ _)) = Pa.Name A.emptyAnnotation [Pa.Nmc aName]
        applyIf _ = name
    in Pa.Identifier iAnn (applyIf comp)
omitIfNecessary _ _ expr = expr

-- extends the group-by clause with references to tenant keys wehre necessary (e.g. if there are tenant-specific key-attributes in group-by)
-- returns only the additional keys which then need to be added
extendTenantKeys :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprList -> Pa.ScalarExprList
extendTenantKeys spec trefs (Pa.Identifier _ name:exprs) =
    let (tName, a)  = getTableAndAttName name
        comp        = lookupAttributeComparability spec (tName, a) trefs
        oldName     = getOldTableName tName trefs
        others      = extendTenantKeys spec trefs exprs
        addIf (Just newTName) (Just oldTName) (Just MtSpecific) = others ++ [getTenantIdentifier newTName oldTName]
        addIf _ _ _ = others
    in addIf tName oldName comp
extendTenantKeys _ _ _ = []

mtRewriteGroupByClause :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprList -> Pa.ScalarExprList
mtRewriteGroupByClause spec trefs = map (omitIfNecessary spec trefs) . (\l -> removeDuplicates (extendTenantKeys spec trefs l) ++ l)

mtRewriteOrderByClause :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
mtRewriteOrderByClause spec trefs ((expr, dir, no):list) = (omitIfNecessary spec trefs expr, dir, no) : mtRewriteOrderByClause spec trefs list
mtRewriteOrderByClause _ _ [] = []

mtRewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> Either MtRewriteError (Maybe Pa.ScalarExpr)
mtRewriteMaybeScalarExpr spec setting (Just expr) trefs = do
    h <- mtRewriteScalarExpr spec setting expr trefs
    Right $ Just h
mtRewriteMaybeScalarExpr _ _ Nothing _ = Right Nothing

mtRewriteSelectList :: MtSchemaSpec -> MtSetting -> Pa.SelectList -> Pa.TableRefList -> Either MtRewriteError Pa.SelectList
mtRewriteSelectList spec setting (Pa.SelectList ann items) trefs = do
    t <- mtRewriteSelectItems spec setting items trefs
    Right $ Pa.SelectList ann t

mtRewriteSelectItems :: MtSchemaSpec -> MtSetting -> [Pa.SelectItem] -> Pa.TableRefList -> Either MtRewriteError [Pa.SelectItem]
-- default case, recursively call rewrite on single item
mtRewriteSelectItems spec setting (item:items) trefs = do
    h <- mtRewriteSelectItem spec setting item trefs
    t <- mtRewriteSelectItems spec setting items trefs
    Right $ h : t
mtRewriteSelectItems _ _ [] _ = Right []

mtRewriteSelectItem :: MtSchemaSpec -> MtSetting -> Pa.SelectItem -> Pa.TableRefList -> Either MtRewriteError Pa.SelectItem
mtRewriteSelectItem spec setting (Pa.SelExp ann scalExp) trefs = do
    newExp <- mtRewriteScalarExpr spec setting scalExp trefs
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
mtRewriteSelectItem spec setting (Pa.SelectItem ann scalExp newName) trefs = do
    h <- mtRewriteScalarExpr spec setting scalExp trefs
    Right $ Pa.SelectItem ann h newName

mtRewriteScalarExprList :: MtSchemaSpec -> MtSetting -> Pa.ScalarExprList -> Pa.TableRefList -> Either MtRewriteError Pa.ScalarExprList
mtRewriteScalarExprList spec setting (arg:args) trefs = do
    newArg <- mtRewriteScalarExpr spec setting arg trefs
    newArgs <- mtRewriteScalarExprList spec setting args trefs
    Right (newArg : newArgs)
mtRewriteScalarExprList _ _ [] _ = Right []

mtRewriteInList :: MtSchemaSpec -> MtSetting -> Pa.InList -> Pa.TableRefList -> Either MtRewriteError Pa.InList
mtRewriteInList spec setting (Pa.InList a elist) trefs = do
    l <- mtRewriteScalarExprList spec setting elist trefs
    Right $ Pa.InList a l
mtRewriteInList spec setting (Pa.InQueryExpr a sel) trefs = do
    h <- mtRewriteQuery spec setting sel trefs
    Right $ Pa.InQueryExpr a h

type CasesType = Pa.CaseScalarExprListScalarExprPairList
mtRewriteCases :: MtSchemaSpec -> MtSetting -> CasesType -> Pa.TableRefList -> Either MtRewriteError CasesType
mtRewriteCases spec setting ((elist, expr):rest) trefs = do
    h <- mtRewriteScalarExprList spec setting elist trefs
    e <- mtRewriteScalarExpr spec setting expr trefs
    l <- mtRewriteCases spec setting rest trefs
    Right $ ((h, e):l)
mtRewriteCases _ _ [] _ = Right []

mtRewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Pa.ScalarExpr -> Pa.TableRefList -> Either MtRewriteError Pa.ScalarExpr
mtRewriteScalarExpr spec setting (Pa.PrefixOp ann opName arg) trefs = do
    h <- mtRewriteScalarExpr spec setting arg trefs
    Right $ Pa.PrefixOp ann opName h
mtRewriteScalarExpr spec setting (Pa.PostfixOp ann opName arg) trefs = do
    h <- mtRewriteScalarExpr spec setting arg trefs
    Right $ Pa.PostfixOp ann opName h
mtRewriteScalarExpr spec setting (Pa.BinaryOp ann opName arg0 arg1) trefs = do
    b1 <- mtRewriteScalarExpr spec setting arg0 trefs
    b2 <- mtRewriteScalarExpr spec setting arg1 trefs
    Right $ Pa.BinaryOp ann opName b1 b2
mtRewriteScalarExpr spec setting (Pa.SpecialOp ann opName args) trefs = do
    l <- mtRewriteScalarExprList spec setting args trefs
    Right $ Pa.SpecialOp ann opName l
mtRewriteScalarExpr spec setting (Pa.App ann funName args) trefs = do
    l <- mtRewriteScalarExprList spec setting args trefs
    Right $ Pa.App ann funName l
mtRewriteScalarExpr spec setting (Pa.Parens ann expr) trefs = do
    h <- mtRewriteScalarExpr spec setting expr trefs
    Right $ Pa.Parens ann h
mtRewriteScalarExpr spec setting (Pa.InPredicate ann expr i list) trefs = do
    h <- mtRewriteScalarExpr spec setting expr trefs
    l <- mtRewriteInList spec setting list trefs
    Right $ Pa.InPredicate ann h i l
mtRewriteScalarExpr spec setting (Pa.Exists ann sel) trefs = do
    h <- mtRewriteQuery spec setting sel trefs
    Right $ Pa.Exists ann h
mtRewriteScalarExpr spec setting (Pa.ScalarSubQuery ann sel) trefs = do
    h <- mtRewriteQuery spec setting sel trefs
    Right $ Pa.ScalarSubQuery ann h
mtRewriteScalarExpr spec setting (Pa.Case ann cases els) trefs = do
    c <- mtRewriteCases spec setting cases trefs
    e <- mtRewriteMaybeScalarExpr spec setting els trefs
    Right $ Pa.Case ann c e
mtRewriteScalarExpr spec (c,_,_) (Pa.Identifier iAnn i) trefs  =
    let (tableName, attName) = getTableAndAttName i
        comparability = lookupAttributeComparability spec (tableName, attName) trefs
        rewrite (Just (MtConvertible to from)) (Just tName) =
            let (Just oldTName) = getOldTableName (Just tName) trefs
            in  Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from])
                    [Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to])
                        [Pa.Identifier iAnn i, getTenantIdentifier tName oldTName]
                    ,Pa.NumberLit iAnn (show c)]
        rewrite _ _ = Pa.Identifier iAnn i
    in Right $ rewrite comparability tableName 
mtRewriteScalarExpr _ _ expr _ = Right expr


-- ##################################
-- MT Optimizer
-- ##################################
--
-- Some preliminary ideas for optimizer steps
-- Observations:
--      - some optimizations should happen in conjunction with rewrite (i) (iv)
--      - some optimizations need conversion provenance (iv) to (vi)
--
-- (i)      D=C optimization ==> only filter at the bottom level, rest of the query looks the same, no conversion needed
-- (ii)     |D| = 1: similar to i), but additionally requires a final presentation to client view at the uppermost select
-- (iii)    Client Presentation push-up: push-up conversion to client format to the uppermost select
--          --> comparisons are always done in universal format, constants need to be brought into universal format as well
-- (iv)     Conversion push-up: do comparisons in owner's format --> needs to convert constant twice --> only convert on joins or aggregations
-- (v)      Aggregation distribution: if possible, compute partial aggregations on different client formats and then only transfrom these partial results
-- (vi)     Statistical aggregation optimization: if (v) not possible: figure out in (intermediary) formats to convert values (essentially equivalent to join ordering and site selection in distributed query processing

