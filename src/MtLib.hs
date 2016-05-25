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
-- This design has the advantage that it is simple and that we do not have to track any intermediary state

mtRewrite :: MtSchemaSpec -> MtSetting -> String -> MtSqlTree
mtRewrite spec setting query = do
    parsedQuery <- mtParse query
    let annotatedQuery = mtAnnotate spec parsedQuery
    let rewrittenQuery = mtRewriteQuery spec setting annotatedQuery
    Right rewrittenQuery

mtRewriteQuery :: MtSchemaSpec -> MtSetting -> Pa.QueryExpr -> Pa.QueryExpr
mtRewriteQuery spec setting (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
        let newTrefs      = mtRewriteTrefList spec setting selTref
            newSelectList = mtRewriteSelectList spec setting selSelectList
            newWhere      = mtRewriteMaybeScalarExpr spec setting selWhere
            newHaving     = mtRewriteMaybeScalarExpr spec setting selHaving
        in  Pa.Select ann selDistinct newSelectList newTrefs newWhere
            selGroupBy newHaving selOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtRewriteQuery _ _ query = query

getTenantIdentifier :: MtTableName -> String
getTenantIdentifier s = head s : "_TENANT_KEY"

mtRewriteTrefList :: MtSchemaSpec -> MtSetting -> Pa.TableRefList -> Pa.TableRefList
mtRewriteTrefList spec setting (Pa.SubTref ann sel:trefs) = Pa.SubTref ann (mtRewriteQuery spec setting sel) : mtRewriteTrefList spec setting trefs
mtRewriteTrefList spec setting (Pa.TableAlias ann tb tref:trefs) = Pa.TableAlias ann tb (head (mtRewriteTrefList spec setting [tref]))
        : mtRewriteTrefList spec setting trefs
mtRewriteTrefList spec setting (tref:trefs) = tref:mtRewriteTrefList spec setting trefs
mtRewriteTrefList _ _ [] = []

mtRewriteSelectList :: MtSchemaSpec -> MtSetting -> Pa.SelectList -> Pa.SelectList
mtRewriteSelectList spec setting (Pa.SelectList ann items) = Pa.SelectList ann (mtRewriteSelectItems spec setting items)

mtRewriteSelectItems :: MtSchemaSpec -> MtSetting -> [Pa.SelectItem] -> [Pa.SelectItem]
-- default case, recursively call rewrite on single item
mtRewriteSelectItems spec setting (item:items) = mtRewriteSelectItem spec setting item : mtRewriteSelectItems spec setting items
mtRewriteSelectItems _ _ [] = []

mtRewriteSelectItem :: MtSchemaSpec -> MtSetting -> Pa.SelectItem -> Pa.SelectItem
mtRewriteSelectItem spec setting (Pa.SelExp ann scalExp)             = Pa.SelExp ann (mtRewriteScalarExpr spec setting scalExp)
mtRewriteSelectItem spec setting (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (mtRewriteScalarExpr spec setting scalExp) newName

mtRewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
mtRewriteMaybeScalarExpr spec setting (Just expr) = Just (mtRewriteScalarExpr spec setting expr)
mtRewriteMaybeScalarExpr _ _ Nothing = Nothing

lookupAttributeComparability :: MtSchemaSpec -> Pa.Name -> (Maybe MtAttributeComparability, MtTableName)
lookupAttributeComparability spec (Pa.Name _ nameList) =
    let (Pa.Nmc attName) = last nameList
        (Pa.Nmc tName) = last $ init nameList
        innerLookup (Just (FromMtSpecificTable tab)) = M.lookup attName tab
        innerLookup _ = Nothing
    in  (innerLookup $ M.lookup tName spec, tName)
lookupAttributeComparability _ (Pa.AntiName _) = (Nothing, "NOTABLE")

mtRewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Pa.ScalarExpr -> Pa.ScalarExpr
mtRewriteScalarExpr spec setting (Pa.PrefixOp ann opName arg) = Pa.PrefixOp ann opName (mtRewriteScalarExpr spec setting arg)
mtRewriteScalarExpr spec setting (Pa.PostfixOp ann opName arg) = Pa.PostfixOp ann opName (mtRewriteScalarExpr spec setting arg)
mtRewriteScalarExpr spec setting (Pa.BinaryOp ann opName arg0 arg1) = Pa.BinaryOp ann opName
        (mtRewriteScalarExpr spec setting arg0) (mtRewriteScalarExpr spec setting arg1)
mtRewriteScalarExpr spec setting (Pa.SpecialOp ann opName args) = Pa.SpecialOp ann opName (map (mtRewriteScalarExpr spec setting) args)
mtRewriteScalarExpr spec setting (Pa.App ann funName args) = Pa.App ann funName (map (mtRewriteScalarExpr spec setting) args)
mtRewriteScalarExpr spec setting (Pa.Parens ann expr) = Pa.Parens ann (mtRewriteScalarExpr spec setting expr)
mtRewriteScalarExpr spec setting (Pa.InPredicate ann expr i list) = Pa.InPredicate ann (mtRewriteScalarExpr spec setting expr) i list
mtRewriteScalarExpr spec setting (Pa.Exists ann sel) = Pa.Exists ann (mtRewriteQuery spec setting sel)
mtRewriteScalarExpr spec setting (Pa.ScalarSubQuery ann sel) = Pa.ScalarSubQuery ann (mtRewriteQuery spec setting sel)
mtRewriteScalarExpr spec (c,_) (Pa.Identifier iAnn i) =
    let rewrite (Just (MtTransformable to from), tName) = Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from]) [Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to]) [Pa.Identifier iAnn i, Pa.Identifier iAnn (Pa.Name iAnn [Pa.Nmc tName, Pa.Nmc (getTenantIdentifier tName)])], Pa.NumberLit iAnn (show c)]
        rewrite _ = Pa.Identifier iAnn i
    in  rewrite $ lookupAttributeComparability spec i
mtRewriteScalarExpr _ _ expr = expr

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
mtAnnotateDirectionList spec trefs ((expr, dir, no):list) = ((mtAnnotateScalarExpr spec trefs expr), dir, no) : mtAnnotateDirectionList spec trefs list
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

