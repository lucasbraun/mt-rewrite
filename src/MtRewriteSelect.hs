module MtRewriteSelect (
    rewriteSelectList
) where

import qualified Database.HsSqlPpp.Parse as Pa

import MtTypes
import MtUtils

rewriteSelectList :: MtSchemaSpec -> MtSetting -> Pa.SelectList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError Pa.SelectList
rewriteSelectList spec setting (Pa.SelectList ann items) trefs rFun = do
    t <- rewriteSelectItems spec setting items trefs rFun
    Right $ Pa.SelectList ann t

rewriteSelectItems :: MtSchemaSpec -> MtSetting -> [Pa.SelectItem] -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError [Pa.SelectItem]
rewriteSelectItems spec setting (item:items) trefs rFun = do
    h <- rewriteSelectItem spec setting item trefs rFun
    t <- rewriteSelectItems spec setting items trefs rFun
    Right $ h : t
rewriteSelectItems _ _ [] _ _ = Right []

rewriteSelectItem :: MtSchemaSpec -> MtSetting -> Pa.SelectItem -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError Pa.SelectItem
rewriteSelectItem spec setting (Pa.SelExp ann scalExp) trefs rFun = do
    newExp <- rewriteScalarExpr spec setting scalExp trefs rFun
    -- 'create' makes sure that a converted attribute gets renamed to its original name
    let create (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                    [Pa.Identifier a4 i, arg0]
                ,Pa.NumberLit a5 arg1]) 
                    | containsString to "ToUniversal" && containsString from "FromUniversal"   = let (_ , Just attName) = getTableAndAttName i
                        in  Pa.SelectItem ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1]) (Pa.Nmc attName)
                    | otherwise                                                         =
                        Pa.SelExp ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1])
        create expr                                                                     = Pa.SelExp ann expr
    Right $ create newExp
rewriteSelectItem spec setting (Pa.SelectItem ann scalExp newName) trefs rFun = do
    h <- rewriteScalarExpr spec setting scalExp trefs rFun
    Right $ Pa.SelectItem ann h newName

rewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Maybe Pa.ScalarExpr)
rewriteMaybeScalarExpr spec setting (Just expr) trefs rFun = do
    h <- rewriteScalarExpr spec setting expr trefs rFun
    Right $ Just h
rewriteMaybeScalarExpr _ _ Nothing _ _ = Right Nothing

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
rewriteInList spec setting (Pa.InQueryExpr a sel) trefs  rFun= do
    h <-  rFun spec setting sel trefs
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
    e <- rewriteMaybeScalarExpr spec setting els trefs rFun
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

