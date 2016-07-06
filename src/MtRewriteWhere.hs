module MtRewriteWhere (
    rewriteHavingClause
    ,rewriteWhereClause
) where

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A

import MtTypes
import MtUtils

rewriteHavingClause :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Maybe Pa.ScalarExpr)
rewriteHavingClause spec setting clause trefs rFun = do
    adjustedHaving <- adjustClause spec setting clause trefs   -- adds predicates on tenant keys
    convertClause spec setting adjustedHaving trefs rFun       -- adds conversion functions

rewriteWhereClause :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Maybe Pa.ScalarExpr)
rewriteWhereClause spec (c,d,o) clause seltref trefs rFun = do
    convertedWhere   <- rewriteHavingClause spec (c,d,o) clause trefs rFun  -- the first two steps are the same as HAVING
    Right $ filterWhereClause spec d seltref convertedWhere                 -- adds D-filters

-- adds conversion functions whenever necessary and also triggers rewriting a whole scalar subquery in necessary
convertClause :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Maybe Pa.ScalarExpr)
convertClause spec setting (Just expr) trefs rFun = do
    h <- rewriteScalarExpr spec setting expr trefs rFun
    Right $ Just h
convertClause _ _ Nothing _ _ = Right Nothing

rewriteScalarExprList :: MtSchemaSpec -> MtSetting -> Pa.ScalarExprList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError Pa.ScalarExprList
rewriteScalarExprList spec setting (arg:args) trefs rFun = do
    newArg <- rewriteScalarExpr spec setting arg trefs rFun
    newArgs <- rewriteScalarExprList spec setting args trefs rFun
    Right (newArg : newArgs)
rewriteScalarExprList _ _ [] _ _ = Right []

rewriteInList :: MtSchemaSpec -> MtSetting -> Pa.InList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError Pa.InList
rewriteInList spec setting (Pa.InList a elist) trefs rFun = do
    l <- rewriteScalarExprList spec setting elist trefs rFun
    Right $ Pa.InList a l
rewriteInList spec setting (Pa.InQueryExpr a sel) trefs rFun = do
    h <- rFun spec setting sel trefs
    Right $ Pa.InQueryExpr a h

rewriteCases :: MtSchemaSpec -> MtSetting -> CasesType -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError CasesType
rewriteCases spec setting ((elist, expr):rest) trefs rFun = do
    h <- rewriteScalarExprList spec setting elist trefs rFun
    e <- rewriteScalarExpr spec setting expr trefs rFun
    l <- rewriteCases spec setting rest trefs rFun
    Right ((h, e):l)
rewriteCases _ _ [] _ _ = Right []

rewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError Pa.ScalarExpr
rewriteScalarExpr spec setting (Pa.PrefixOp ann opName arg) trefs rFun = do
    h <- rewriteScalarExpr spec setting arg trefs rFun
    Right $ Pa.PrefixOp ann opName h
rewriteScalarExpr spec setting (Pa.PostfixOp ann opName arg) trefs rFun = do
    h <- rewriteScalarExpr spec setting arg trefs rFun
    Right $ Pa.PostfixOp ann opName h
rewriteScalarExpr spec setting (Pa.BinaryOp ann opName arg0 arg1) trefs rFun = do
    b1 <- rewriteScalarExpr spec setting arg0 trefs rFun
    b2 <- rewriteScalarExpr spec setting arg1 trefs rFun
    Right $ Pa.BinaryOp ann opName b1 b2
rewriteScalarExpr spec setting (Pa.SpecialOp ann opName args) trefs rFun = do
    l <- rewriteScalarExprList spec setting args trefs rFun
    Right $ Pa.SpecialOp ann opName l
rewriteScalarExpr spec setting (Pa.App ann funName args) trefs rFun = do
    l <- rewriteScalarExprList spec setting args trefs rFun
    Right $ Pa.App ann funName l
rewriteScalarExpr spec setting (Pa.Parens ann expr) trefs rFun = do
    h <- rewriteScalarExpr spec setting expr trefs rFun
    Right $ Pa.Parens ann h
rewriteScalarExpr spec setting (Pa.InPredicate ann expr i list) trefs rFun = do
    h <- rewriteScalarExpr spec setting expr trefs rFun
    l <- rewriteInList spec setting list trefs rFun
    Right $ Pa.InPredicate ann h i l
rewriteScalarExpr spec setting (Pa.Exists ann sel) trefs rFun = do
    h <- rFun spec setting sel trefs
    Right $ Pa.Exists ann h
rewriteScalarExpr spec setting (Pa.ScalarSubQuery ann sel) trefs rFun = do
    h <- rFun spec setting sel trefs
    Right $ Pa.ScalarSubQuery ann h
rewriteScalarExpr spec setting (Pa.Case ann cases els) trefs rFun = do
    c <- rewriteCases spec setting cases trefs rFun
    e <- convertClause spec setting els trefs rFun
    Right $ Pa.Case ann c e
rewriteScalarExpr spec (c,d,o) (Pa.Identifier iAnn i) trefs _ =
    let (tableName, attName) = getTableAndAttName i
        comparability = lookupAttributeComparability spec (tableName, attName) trefs
        rewrite (Just (MtConvertible to from)) (Just tName) False =
            let (Just oldTName) = getOldTableName (Just tName) trefs
            in  Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from])
                    [Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to])
                        [Pa.Identifier iAnn i, getTenantIdentifier tName oldTName]
                    ,Pa.NumberLit iAnn (show c)]
        rewrite _ _ _ = Pa.Identifier iAnn i
    in Right $ rewrite comparability tableName (MtTrivialOptimization `elem` o && (length d == 1) && (head d == c))
rewriteScalarExpr _ _ expr _ _ = Right expr

-- checks the join predicates and adds necessary constraints
-- right now only checks simple (non-nested and non-complex) join predicates
adjustClause :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> Either MtRewriteError (Maybe Pa.ScalarExpr)
adjustClause spec (_,d,o) (Just whereClause) trefs =
    if MtTrivialOptimization `elem` o && (length d == 1)
        then
            Right $ Just whereClause
        else do
            adjustedExp <- adjustScalarExpr spec whereClause trefs
            Right $ Just adjustedExp
adjustClause _ _ whereClause _ = Right whereClause
        --

-- adds tenant identifier for mt-specific predicates
adjustJoinPredicate :: MtTableName -> MtTableName -> String -> Pa.ScalarExpr -> Pa.TableRefList -> Pa.ScalarExpr
adjustJoinPredicate t0 t1 opName expr trefs =
    let (Just old0) = getOldTableName (Just t0) trefs
        (Just old1) = getOldTableName (Just t1) trefs
        exec
            | opName == "<>"    = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "or"])
                                    (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "<>"])
                                        (getTenantIdentifier t0 old0) (getTenantIdentifier t1 old1)) expr
            | otherwise         = Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "and"])
                                    (Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "="])
                                        (getTenantIdentifier t0 old0) (getTenantIdentifier t1 old1)) expr
    in exec


adjustScalarExpr :: MtSchemaSpec -> Pa.ScalarExpr -> Pa.TableRefList -> Either MtRewriteError Pa.ScalarExpr
adjustScalarExpr spec (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)) trefs =
    let checkNecessary  = opName `elem` ["=", "<>", "<", ">", ">=", "<="]
        (t0, a0)        = getTableAndAttName n0
        comp0           = lookupAttributeComparability spec (t0, a0) trefs
        (t1, a1)        = getTableAndAttName n1
        comp1           = lookupAttributeComparability spec (t1, a1) trefs
        -- if both are specific, check if they are from same table
        adjust True (Just tn0) (Just MtSpecific) (Just tn1) (Just MtSpecific)
            | tn0 == tn1    = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)
            | otherwise     = Right $ adjustJoinPredicate tn0 tn1 opName
                                (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)) trefs
        adjust True _ (Just MtSpecific) _ _ = Left $ FromMtRewriteError ("cannot compare tenant-specific attribute "
            ++ printName n0 ++ " with non-specific attribute " ++ printName n1 ++ "!")
        adjust True _ _ _ (Just MtSpecific) = Left $ FromMtRewriteError ("cannot compare tenant-specific attribute "
            ++ printName n1 ++ " with non-specific attribute " ++ printName n0 ++ "!")
        adjust _ _ _ _ _ = Right $ Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1)
    in adjust checkNecessary t0 comp0 t1 comp1
adjustScalarExpr spec (Pa.BinaryOp ann i arg0 arg1) trefs = do
    h <- adjustScalarExpr spec arg0 trefs
    t <- adjustScalarExpr spec arg1 trefs
    Right $ Pa.BinaryOp ann i h t
adjustScalarExpr spec (Pa.PrefixOp ann opName arg) trefs= do
    h <- adjustScalarExpr spec arg trefs
    Right $ Pa.PrefixOp ann opName h
adjustScalarExpr _ expr _ = Right expr

-- adds the necessary dataset filter to a where clause
filterWhereClause :: MtSchemaSpec -> MtDataSet -> Pa.TableRefList -> Maybe Pa.ScalarExpr  -> Maybe Pa.ScalarExpr
filterWhereClause spec ds (tref:trefs) whereClause = filterWhereClause spec ds trefs (addDFilter spec ds tref whereClause Nothing)
filterWhereClause _ _ [] whereClause = whereClause

-- adds a dataset filter for a specific table to an existing where predicate
-- only adds filter for tables that are tenant-specific
-- only adds filter if dataset length > 0 (an empty dataset means we query everything)
addDFilter :: MtSchemaSpec -> MtDataSet -> Pa.TableRef -> Maybe Pa.ScalarExpr -> Maybe MtTableName -> Maybe Pa.ScalarExpr
addDFilter spec ds (Pa.Tref _ (Pa.Name nAnn nameList)) whereClause priorName =
    let (Pa.Nmc tName)      = last nameList
        isGlobal            = isGlobalTable spec (Just tName)
        newName (Just n)    = n
        newName Nothing     = tName
        inPred
            | isGlobal                  = Nothing
            | null ds                   = Nothing
            | otherwise                 = Just $ Pa.InPredicate nAnn (getTenantIdentifier (newName priorName) tName) True
                                            (Pa.InList A.emptyAnnotation (map (Pa.NumberLit A.emptyAnnotation . show) ds))
        addTo (Just expr) (Just pr)     = Just $ Pa.BinaryOp A.emptyAnnotation (Pa.Name A.emptyAnnotation [Pa.Nmc "and"]) expr pr
        addTo Nothing (Just pr)         = Just pr
        addTo (Just expr) Nothing       = Just expr
        addTo Nothing Nothing           = Nothing
    in addTo whereClause inPred
addDFilter spec ds (Pa.TableAlias _ (Pa.Nmc newName) tref) whereClause priorName =
    let computeFinalName (Just n)       = n
        computeFinalName Nothing        = newName
    in  addDFilter spec ds tref whereClause (Just $ computeFinalName priorName)
addDFilter _ _ _ whereClause _ = whereClause

