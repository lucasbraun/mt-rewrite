module MtLib 
(
     mtParse
    ,mtPrettyPrint
) where

import qualified Data.Map as M

import qualified Database.HsSqlPpp.Parse as Pa
-- import qualified Database.HsSqlPpp.Internals.AstInternal as A
import qualified Data.Text.Lazy as L
import qualified Database.HsSqlPpp.Pretty as Pr

-- MT types and type classes
type MtFromUniversalFunc        = String
type MtToUniversalFunc          = String
type MtTransformable            = (MtToUniversalFunc, MtFromUniversalFunc)
data MtAttributeComparability   = MtComparable | MtTransformable | MtSpecific
type MtAttributeName            = String
type MtAttributeSpec            = (MtAttributeName, MtAttributeComparability)
type MtSpecificTable            = M.Map MtAttributeName MtAttributeSpec
data MtTableSpec                = MtGlobalTable | MtTSpecificTable
type MtTableName                = String
type MtSchemaSpec               = M.Map MtTableName MtTableSpec

mtParse :: String -> Either Pa.ParseErrorExtra Pa.QueryExpr
mtParse query = Pa.parseQueryExpr
                    Pa.defaultParseFlags
                    "./err.txt"
                    (Just (0,0))
                    (L.pack query)

mtPrettyPrint :: Either Pa.ParseErrorExtra Pa.QueryExpr -> String
mtPrettyPrint (Right exp) = L.unpack $ Pr.prettyQueryExpr Pr.defaultPrettyFlags exp
mtPrettyPrint (Left err) = show err
