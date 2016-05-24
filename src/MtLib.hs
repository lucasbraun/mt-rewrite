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
    Right $
        (\(Pa.Select
            ann selDistinct selSelectList selTref selWhere
            selGroupBy selHaving selOrderBy selLimit selOffset selOption) 
                -> (Pa.Select ann selDistinct
                    (mtRewriteSelectList spec config selTref selSelectList)
                    selTref selWhere selGroupBy selHaving selOrderBy selLimit selOffset selOption)
        )
        annotatedQuery

mtGetTenantIdentifier :: MtTableName -> String
mtGetTenantIdentifier s = head s : "_TENANT_KEY"

mtRewriteSelectList :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtRewriteSelectList spec config tref (Pa.SelectList ann items) = Pa.SelectList ann (concatMap (mtRewriteSelectItem spec config tref) items)

mtRewriteSelectItem :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectItem -> [Pa.SelectItem]
-- replace transformable function
-- TODO: continue here
-- default case
mtRewriteSelectItem spec config tref item = [item]

-- ##################################
-- MT Annotate
-- ##################################

-- make sure that every attribute of a specific table appears with its table name
-- assumes that attributes not found within the schema are from global tables
mtAnnotate :: MtSchemaSpec -> Pa.QueryExpr -> Pa.QueryExpr
mtAnnotate spec (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
        let newSelectList = mtAnnotateSelectList spec selTref selSelectList
            newTrefs = mtAnnotateTrefList spec selTref
            newWhere = mtAnnotateMaybeScalarExpr spec selTref selWhere
            newGroupBy = map (mtAnnotateScalarExpr spec selTref) selGroupBy
        in  Pa.Select ann selDistinct newSelectList newTrefs newWhere
            newGroupBy selHaving selOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
mtAnnotate _ query = query

mtAnnotateTrefList :: MtSchemaSpec -> Pa.TableRefList -> Pa.TableRefList
mtAnnotateTrefList spec (Pa.SubTref ann sel:trefs) = Pa.SubTref ann (mtAnnotate spec sel) : mtAnnotateTrefList spec trefs
mtAnnotateTrefList spec (Pa.TableAlias ann tb tref:trefs) = Pa.TableAlias ann tb (head (mtAnnotateTrefList spec [tref])) : mtAnnotateTrefList spec trefs
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
mtAnnotateSelectItems _ _ [] = []
mtAnnotateSelectItems spec trefs (item:items) = mtAnnotateSelectItem spec trefs item : mtAnnotateSelectItems spec trefs items
mtAnnotateSelectItem :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectItem -> Pa.SelectItem
mtAnnotateSelectItem spec trefs (Pa.SelExp ann scalExp)             = Pa.SelExp ann (mtAnnotateScalarExpr spec trefs scalExp)
mtAnnotateSelectItem spec trefs (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (mtAnnotateScalarExpr spec trefs scalExp) newName

mtAnnotateMaybeScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
mtAnnotateMaybeScalarExpr spec trefs (Just expr) = Just (mtAnnotateScalarExpr spec trefs expr)
mtAnnotateMaybeScalarExpr _ _ Nothing = Nothing

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

