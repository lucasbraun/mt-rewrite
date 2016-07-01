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
    ,MtSetting
    ,mtSchemaSpecFromList
    ,mtSpecificTableFromList
) where

import qualified Data.Map as M

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

mtSchemaSpecFromList :: [(MtTableName, MtTableSpec)] -> MtSchemaSpec
mtSchemaSpecFromList = M.fromList

mtSpecificTableFromList :: [(MtAttributeName, MtAttributeComparability)] -> MtSpecificTable
mtSpecificTableFromList = M.fromList

