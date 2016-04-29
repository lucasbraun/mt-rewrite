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

mtGetTenantIdentifier :: MtTableName -> String
mtGetTenantIdentifier s = head s : "_TENANT_KEY"

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

mtRewriteSelectList :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtRewriteSelectList spec config tref (Pa.SelectList ann items) = Pa.SelectList ann (concatMap (mtRewriteSelectItem spec config tref) items)

mtRewriteSelectItem :: MtSchemaSpec -> (MtClient, MtDataSet) -> Pa.TableRefList -> Pa.SelectItem -> [Pa.SelectItem]
-- replaces Star Expressions with an enumeration of all attributes instead (* would also display tenant key, which is something we do not want)
mtRewriteSelectItem spec config [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] (Pa.SelExp selAnn (Pa.Star starAnn)) =
    let tableSpec = M.lookup tname spec
        generate (Just (FromMtSpecificTable tab)) = map (Pa.SelExp selAnn . Pa.StringLit starAnn) (M.keys tab)
        generate _ = [Pa.SelExp selAnn (Pa.Star starAnn)]
    in generate tableSpec
-- replace transformable function
-- TODO: continue here
-- default case
mtRewriteSelectItem spec config tref item = [item]

-- make sure that every attribute of a specific table appears with its table name
-- assumes that attributes not found within the schema are from global tables
mtAnnotate :: MtSchemaSpec -> Pa.QueryExpr -> Pa.QueryExpr
mtAnnotate spec (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
        let rewrittenList = mtAnnotateSelectList spec selTref selSelectList
        in  Pa.Select ann selDistinct rewrittenList selTref selWhere
            selGroupBy selHaving selOrderBy selLimit selOffset selOption

mtAnnotateSelectList :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtAnnotateSelectList spec trefs (Pa.SelectList ann (item:items)) = Pa.SelectList ann ((mtAnnotateSelectItem spec trefs item):items)

mtAnnotateSelectItem :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectItem -> Pa.SelectItem
mtAnnotateSelectItem spec trefs (Pa.SelExp ann scalExp)             = Pa.SelExp ann (mtAnnotateScalarExpr spec trefs scalExp)
mtAnnotateSelectItem spec trefs (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (mtAnnotateScalarExpr spec trefs scalExp) newName

mtAnnotateScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExpr -> Pa.ScalarExpr
mtAnnotateScalarExpr spec ((Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])):trefs) (Pa.Identifier iAnn (Pa.Name a ((Pa.Nmc attName):nameComps)))
    | length nameComps > 0  = Pa.Identifier iAnn (Pa.Name a ((Pa.Nmc attName):nameComps))
    | otherwise             =
        let tableSpec = M.lookup tname spec
            containsAttribute (Just (FromMtSpecificTable attrMap)) aName = M.member aName attrMap 
            containsAttribute _ _ = False
            contains = containsAttribute tableSpec attName
            propagate False = mtAnnotateScalarExpr spec trefs (Pa.Identifier iAnn (Pa.Name a [Pa.Nmc attName]))
            propagate True  = Pa.Identifier iAnn (Pa.Name a [(Pa.Nmc tname),(Pa.Nmc attName)])
        in  propagate contains
mtAnnotateScalarExpr _ _ expr = expr

-- TODO: continue here
