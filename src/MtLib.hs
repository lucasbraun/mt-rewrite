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

mtRewrite :: ParseResult -> MtSchemaSpec -> MtRewriteResult
mtRewrite (Left err) _ = Left (Left err)
mtRewrite (Right (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption)) spec =
        mtRewriteSelect (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) spec

mtRewriteSelect :: Pa.QueryExpr -> MtSchemaSpec -> MtRewriteResult
mtRewriteSelect query spec = Right query
-- TODO: continue here

mtPrettyPrintRewrittenQuery :: MtRewriteResult -> String
mtPrettyPrintRewrittenQuery (Left (Left err)) = show err
mtPrettyPrintRewrittenQuery (Left (Right err)) = err
mtPrettyPrintRewrittenQuery (Right query) = L.unpack $ Pr.prettyQueryExpr Pr.defaultPrettyFlags query
