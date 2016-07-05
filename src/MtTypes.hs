module MtTypes (
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
    ,mtOptimizationsFromList
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
) where

import qualified Data.Map as M
import qualified Data.Set as S

type MtFromUniversalFunc        = String
type MtToUniversalFunc          = String

data MtAttributeComparability   =
    MtComparable
    | MtConvertible MtToUniversalFunc MtFromUniversalFunc
    | MtSpecific

type MtAttributeName            = String
type MtSpecificTable            = M.Map MtAttributeName MtAttributeComparability
data MtTableSpec                = MtGlobalTable | FromMtSpecificTable MtSpecificTable

type MtTableName                = String
type MtSchemaSpec               = M.Map MtTableName MtTableSpec

type MtClient                   = Int
type MtDataSet                  = [MtClient]
data MtOptimization             =     MtTrivialOptimization         -- if |C|=1, do not add ttids, if D={C}, do not add conversions
                                    | MtClientPresentationPushUp    -- push up the conversion to client format to the uppermost sql query in the tree
                                    | MtConversionPushUp            -- do the conversion to universal format as late as possible
                                                                    -- (when needed on a comparison)
                                    | MtConversionDistribution      -- use the distribution law of aggregation over conversion if properties hold, 
                                                                    -- e.g. SUM(toUniversal(x,ttid)) ==> SUM(toUniversal(SUM-per-ttid(x), ttid))
                                    | MtUnknownOptimization         -- dummy value to map whenever an unknown optimization was chosen
                                    deriving (Eq,Ord,Show)
-- there are more comments about optimizations in MtLib.hs...

type MtOptimizationSet          = S.Set MtOptimization
type MtSetting                  = (MtClient, MtDataSet, S.Set MtOptimization)

mtOptimizationsFromList :: [MtOptimization] -> S.Set MtOptimization
mtOptimizationsFromList = S.fromList

mtSchemaSpecFromList :: [(MtTableName, MtTableSpec)] -> MtSchemaSpec
mtSchemaSpecFromList = M.fromList

mtSpecificTableFromList :: [(MtAttributeName, MtAttributeComparability)] -> MtSpecificTable
mtSpecificTableFromList = M.fromList

