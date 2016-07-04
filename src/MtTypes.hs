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
data MtOptimization             = MtTrivialOptimization | MtClientPresentationPushUp | MtConversionPushUp
                                    | MtConversionDistribution | MtUnknownOptimization deriving (Eq,Ord,Show)
type MtSetting                  = (MtClient, MtDataSet, S.Set MtOptimization)

mtOptimizationsFromList :: [MtOptimization] -> S.Set MtOptimization
mtOptimizationsFromList = S.fromList

mtSchemaSpecFromList :: [(MtTableName, MtTableSpec)] -> MtSchemaSpec
mtSchemaSpecFromList = M.fromList

mtSpecificTableFromList :: [(MtAttributeName, MtAttributeComparability)] -> MtSpecificTable
mtSpecificTableFromList = M.fromList

