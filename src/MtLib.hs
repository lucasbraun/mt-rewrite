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
    ,MtOptimizationSet
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

import qualified Data.Text.Lazy as L

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A
import qualified Database.HsSqlPpp.Pretty as Pr
import qualified Database.HsSqlPpp.Dialect as D

import MtTypes
import MtUtils
import MtAnnotate
import MtRewriteWhere
import MtRewriteSelect
import MtOptimizer

-- ##################################
-- Parsing and Printing
-- ##################################

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
-- mtPrettyPrint (Right statement) dialect = show statement -- DEBUG
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
--
-- For optimizations:
-- - for now we assume that conversion functions for numeric values are scalar and conversion functions for
--   text do not have any special properties. For tpch, this is enough, for other benchmark, the definition of
--   MtConvertible has to be extended with a definition whether a conversion function is scalar, linear, quadratic
--   or none of these.

-- parses and rewrites a single SQL statement terminated by ;
mtRewrite :: MtSchemaSpec -> MtSetting -> String -> D.Dialect -> MtRewriteResult
mtRewrite spec setting statement dialect = do
    parsedStatement <- mtParse statement dialect
    let annotatedStatement = mtAnnotateStatement spec parsedStatement
    -- Right annotatedStatement -- DEBUG
    rewrittenStatement <- rewriteStatement spec setting annotatedStatement
    Right rewrittenStatement

-- rewrites an annotated and parsed SQL statement
rewriteStatement :: MtSchemaSpec -> MtSetting -> Pa.Statement -> MtRewriteResult
rewriteStatement spec setting (Pa.QueryStatement a q) = do
    -- (p, rewrittenQuery) <- rewriteQuery spec setting emptyProvenance q []   -- DEBUG
    -- Left $ FromMtRewriteError $ show (MM.toMap p)                           -- DEBUG
    (_, rewrittenQuery) <- rewriteQuery spec setting emptyProvenance q []
    Right $ Pa.QueryStatement a rewrittenQuery
rewriteStatement spec setting (Pa.CreateView a n c q) = do
    (_, rewrittenQuery) <- rewriteQuery spec setting emptyProvenance q []
    Right $ Pa.CreateView a n c rewrittenQuery
rewriteStatement _ _ (Pa.Set ann s vals) = Right $ Pa.Set ann s vals
rewriteStatement _ _ (Pa.DropSomething a d i n c) = Right $ Pa.DropSomething a d i n c
rewriteStatement _ _ statement = Left $ FromMtRewriteError $ "Rewrite function not yet implementd for statement " ++ show statement

-- the main rewrite function, for the case it is used in subqueries, it takes also the table refs from the outer queries into account
rewriteQuery :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.QueryExpr -> Pa.TableRefList -> Either MtRewriteError (Provenance, Pa.QueryExpr)
rewriteQuery spec setting p0 (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) trefs = do
        let allTrefs        = selTref ++ trefs    -- ordering matters here as the first value that fits is taken
        (p1,newTrefs)      <- rewriteTrefList spec setting p0 selTref allTrefs
        (p2,filteredWhere) <- rewriteWhereClause spec setting p1 selWhere selTref allTrefs rewriteQuery
        (p3,newHaving)     <- rewriteHavingClause spec setting p2 selHaving allTrefs rewriteQuery
        (p4,newSelectList) <- rewriteSelectList spec setting p3 selSelectList selTref rewriteQuery (null trefs)
        let newGroupBy      = rewriteGroupByClause spec selTref selGroupBy
        let newOrderBy      = rewriteOrderByClause spec selTref selOrderBy
        Right (p4, mtOptimize setting (
            Pa.Select ann selDistinct newSelectList newTrefs filteredWhere
            newGroupBy newHaving newOrderBy selLimit selOffset selOption) (null trefs))
rewriteQuery _ _ _ query _ = Left $ FromMtRewriteError $ "Rewrite function not yet implemented for query expression " ++ show query

-- ## REWRITE FUNCTIONS ##
-- for small things... for SELECT and WHERE clause, there are separate files
-- ########################

-- the first list is the one that needs to be rewritten, the second one keeps all trefs of that FROM clause
rewriteTrefList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.TableRefList ->Pa.TableRefList -> Either MtRewriteError (Provenance, Pa.TableRefList)
rewriteTrefList spec setting p0 (Pa.SubTref ann sel:trefs) allTrefs = do
    (p1,h) <- rewriteQuery spec setting p0 sel allTrefs
    (p2,t) <- rewriteTrefList spec setting p1 trefs allTrefs
    Right (p2, Pa.SubTref ann h : t)
rewriteTrefList spec setting p0 (Pa.TableAlias ann tb tref:trefs) allTrefs = do
    (p1,h) <- rewriteTrefList spec setting p0 [tref] allTrefs
    (p2,t) <- rewriteTrefList spec setting p1 trefs allTrefs
    Right (p2, Pa.TableAlias ann tb (head h) : t)
rewriteTrefList spec setting p0 (Pa.JoinTref ann tref0 n t h tref1 (Just (Pa.JoinOn a expr)):trefs) allTrefs = do
    (p1,l) <- rewriteTrefList spec setting p0 [tref0] allTrefs
    (p2,r) <- rewriteTrefList spec setting p1 [tref1] allTrefs
    (p3,Just filtered) <- rewriteWhereClause spec setting p2 (Just expr) [tref0, tref1] allTrefs rewriteQuery
    (p4,rest) <- rewriteTrefList spec setting p3 trefs allTrefs
    Right (p4, Pa.JoinTref ann (head l) n t h (head r) (Just (Pa.JoinOn a filtered)) : rest)
rewriteTrefList spec setting p0 (Pa.FullAlias ann tb cols tref:trefs) allTrefs = do
    (p1,h) <- rewriteTrefList spec setting p0 [tref] allTrefs
    (p2,l) <- rewriteTrefList spec setting p1 trefs allTrefs
    Right (p2, Pa.FullAlias ann tb cols (head h) : l)
rewriteTrefList spec setting p0 (Pa.Tref ann tbl:trefs) allTrefs = do
    (p1,t) <- rewriteTrefList spec setting p0 trefs allTrefs
    Right (p1, Pa.Tref ann tbl : t)
rewriteTrefList _ _ _ (tref:_) _ =  Left $ FromMtRewriteError $ "Rewrite function not yet implemented for table-ref " ++ show tref
rewriteTrefList _ _ prov [] _ = Right (prov, [])

-- ommmits the table name of an attribute if necessary, this is actually the case for convertible attributes in group- and order-by clauses
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

rewriteGroupByClause :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprList -> Pa.ScalarExprList
rewriteGroupByClause spec trefs = map (omitIfNecessary spec trefs) . (\l -> removeDuplicates (extendTenantKeys spec trefs l) ++ l)

rewriteOrderByClause :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
rewriteOrderByClause spec trefs ((expr, dir, no):list) = (omitIfNecessary spec trefs expr, dir, no) : rewriteOrderByClause spec trefs list
rewriteOrderByClause _ _ [] = []

