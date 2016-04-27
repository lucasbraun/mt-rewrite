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
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
    ,ParseResult
    ,mtParse
    ,mtPrettyPrint
    ,MtRewriteError
    ,MtRewriteResult
    ,mtRewrite
    ,mtPrettyPrintRewrittenQuery
) where

import qualified Data.Map as M

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Data.Text.Lazy as L
import qualified Database.HsSqlPpp.Pretty as Pr

-- MT types and type classes
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

-- Type Construction helper functions
mtSchemaSpecFromList :: [(MtTableName, MtTableSpec)] -> MtSchemaSpec
mtSchemaSpecFromList = M.fromList

mtSpecificTableFromList :: [(MtAttributeName, MtAttributeComparability)] -> MtSpecificTable
mtSpecificTableFromList = M.fromList

-- Parsing and Printing
type ParseResult = Either Pa.ParseErrorExtra Pa.QueryExpr

mtParse :: String -> ParseResult
mtParse query = Pa.parseQueryExpr
                    Pa.defaultParseFlags
                    "./err.txt"
                    (Just (0,0))
                    (L.pack query)

mtPrettyPrint :: ParseResult -> String
mtPrettyPrint (Left err) = show err
mtPrettyPrint (Right query) = L.unpack $ Pr.prettyQueryExpr Pr.defaultPrettyFlags query

-- MT Rewrite
type MtRewriteError = Either Pa.ParseErrorExtra String
type MtRewriteResult = Either MtRewriteError Pa.QueryExpr

mtGetTenantIdentifier :: String -> String
mtGetTenantIdentifier s = s ++ "_TENANT_KEY"

mtRewrite :: MtSchemaSpec -> ParseResult -> MtRewriteResult
mtRewrite _ (Left err) = Left (Left err)
mtRewrite spec (Right (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption)) =
        Right (
            Pa.Select ann selDistinct
            (mtRewriteSelectList spec selTref selSelectList)
            selTref selWhere
            selGroupBy selHaving selOrderBy selLimit selOffset selOption
        )

mtRewriteSelectList :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
mtRewriteSelectList spec tref (Pa.SelectList ann items) = Pa.SelectList ann (concatMap (mtRewriteSelectItem spec tref) items)

mtRewriteSelectItem :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectItem -> [Pa.SelectItem]
-- replaces Star Expressions with an enumeration of all attributes instead (* would also display tenant key, which is something we do not want)
mtRewriteSelectItem spec [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] (Pa.SelExp selAnn (Pa.Star starAnn)) =
    let tableSpec = M.lookup tname spec
        generate (Just (FromMtSpecificTable tab)) = map (Pa.SelExp selAnn . Pa.StringLit starAnn) (M.keys tab)
        generate _ = [Pa.SelExp selAnn (Pa.Star starAnn)]
    in generate tableSpec
-- default case
mtRewriteSelectItem spec tref item = [item]
-- TODO: continue here

mtPrettyPrintRewrittenQuery :: MtRewriteResult -> String
mtPrettyPrintRewrittenQuery (Left (Left err)) = show err
mtPrettyPrintRewrittenQuery (Left (Right err)) = err
mtPrettyPrintRewrittenQuery (Right query) = L.unpack $ Pr.prettyQueryExpr Pr.defaultPrettyFlags query
