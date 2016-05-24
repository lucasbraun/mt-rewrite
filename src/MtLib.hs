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
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
    ,MtSqlTree
    ,mtParse
    ,mtPrettyPrint
    ,mtRewrite
) where

import qualified Data.Map as M

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

mtRewrite :: MtSchemaSpec -> (MtClient, MtDataSet) -> String -> MtSqlTree
mtRewrite spec config query = do
    parsedQuery <- mtParse query
    let annotatedQuery = mtAnnotate spec parsedQuery
    let rewrittenQuery = mtRewriteQuery spec config annotatedQuery
    Right rewrittenQuery

mtRewriteQuery :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.QueryExpr -> Pa.QueryExpr
mtRewriteQuery spec config (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
        let newSelectList   = mtRewriteSelectList spec config selTref selSelectList
            newTrefs        = mtRewriteTrefList spec config selTref
            newWhere        = mtRewriteMaybeScalarExpr spec config selTref selWhere
            newGroupBy      = map (mtRewriteScalarExpr spec config selTref) selGroupBy
            newHaving       = mtRewriteMaybeScalarExpr spec config selTref selHaving
            newOrderBy      = mtRewriteDirectionList spec config selTref selOrderBy
        in  Pa.Select ann selDistinct newSelectList newTrefs newWhere
            newGroupBy newHaving newOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtRewriteQuery _ _ query = query

mtGetTenantIdentifier :: MtTableName -> String
mtGetTenantIdentifier s = head s : "_TENANT_KEY"

mtRewriteTrefList :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.TableRefList
mtRewriteTrefList spec config (Pa.SubTref ann sel:trefs) = Pa.SubTref ann (mtRewriteQuery spec config sel) : mtRewriteTrefList spec config trefs
mtRewriteTrefList spec config (Pa.TableAlias ann tb tref:trefs) = Pa.TableAlias ann tb (head (mtRewriteTrefList spec config [tref]))
        : mtRewriteTrefList spec config trefs
mtRewriteTrefList spec config (tref:trefs) = tref:mtRewriteTrefList spec config trefs
mtRewriteTrefList _ _ [] = []

mtRewriteSelectList :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtRewriteSelectList spec config tref (Pa.SelectList ann items) = Pa.SelectList ann (mtRewriteSelectItems spec config tref items)

mtRewriteSelectItems :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> [Pa.SelectItem] -> [Pa.SelectItem]
-- default case, recursively call rewrite on single item
mtRewriteSelectItems spec config trefs (item:items) = mtRewriteSelectItem spec config trefs item : mtRewriteSelectItems spec config trefs items
mtRewriteSelectItems _ _ _ [] = []

mtRewriteSelectItem :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectItem -> Pa.SelectItem
mtRewriteSelectItem spec config trefs (Pa.SelExp ann scalExp)             = Pa.SelExp ann (mtRewriteScalarExpr spec config trefs scalExp)
mtRewriteSelectItem spec config trefs (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (mtRewriteScalarExpr spec config trefs scalExp) newName

mtRewriteMaybeScalarExpr :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
mtRewriteMaybeScalarExpr spec config trefs (Just expr) = Just (mtRewriteScalarExpr spec config trefs expr)
mtRewriteMaybeScalarExpr _ _ _ Nothing = Nothing

mtRewriteDirectionList :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
mtRewriteDirectionList spec config trefs ((expr, dir, no):list) = ((mtRewriteScalarExpr spec config trefs expr), dir, no)
        : mtRewriteDirectionList spec config trefs list
mtRewriteDirectionList _ _ _ [] = []

mtRewriteScalarExpr :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.ScalarExpr -> Pa.ScalarExpr
--mtRewriteScalarExpr spec config treflist (Pa.PrefixOp ann opName arg) = Pa.PrefixOp ann opName (mtRewriteScalarExpr spec config treflist arg)
--mtRewriteScalarExpr spec config treflist (Pa.PostfixOp ann opName arg) = Pa.PostfixOp ann opName (mtRewriteScalarExpr spec config treflist arg)
--mtRewriteScalarExpr spec config treflist (Pa.BinaryOp ann opName arg0 arg1) = Pa.BinaryOp ann opName
--        (mtRewriteScalarExpr spec config treflist arg0) (mtRewriteScalarExpr spec config treflist arg1)
--mtRewriteScalarExpr spec config treflist (Pa.SpecialOp ann opName args) = Pa.SpecialOp ann opName (map (mtRewriteScalarExpr spec config treflist) args)
--mtRewriteScalarExpr spec config treflist (Pa.App ann funName args) = Pa.App ann funName (map (mtRewriteScalarExpr spec config treflist) args)
--mtRewriteScalarExpr spec config treflist (Pa.Parens ann expr) = Pa.Parens ann (mtRewriteScalarExpr spec config treflist expr)
--mtRewriteScalarExpr spec config treflist (Pa.InPredicate ann expr i list) = Pa.InPredicate ann (mtRewriteScalarExpr spec config treflist expr) i list
--mtRewriteScalarExpr spec config _ (Pa.Exists ann sel) = Pa.Exists ann (mtRewriteQuery spec config sel)
--mtRewriteScalarExpr spec config _ (Pa.ScalarSubQuery ann sel) = Pa.ScalarSubQuery ann (mtRewriteQuery spec config sel)
--mtRewriteScalarExpr spec config (Pa.Tref _ (Pa.Name _ [Pa.Nmc tname]):trefs) (Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName:nameComps)))
--    | not (null nameComps)  = Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName : nameComps))
--    | otherwise             =
--        let tableSpec = M.lookup tname spec
--            containsAttribute (Just (FromMtSpecificTable attrMap)) aName = M.member aName attrMap 
--            containsAttribute _ _ = False
--            contains = containsAttribute tableSpec attName
--            propagate False = mtRewriteScalarExpr spec trefs (Pa.Identifier iAnn (Pa.Name a [Pa.Nmc attName]))
--            propagate True  = Pa.Identifier iAnn (Pa.Name a [Pa.Nmc tname,Pa.Nmc attName])
--        in  propagate contains
-- default case handles anything we do not handle so far
mtRewriteScalarExpr _ _ _ expr = expr

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

