module MtRewriteSelect (
    rewriteSelectList
) where

import qualified Database.HsSqlPpp.Parse as Pa

import MtTypes
import MtUtils

rewriteSelectList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.SelectList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.SelectList)
rewriteSelectList spec setting p0 (Pa.SelectList ann items) trefs rFun = do
    (p1,t) <- rewriteSelectItems spec setting p0 items trefs rFun
    Right $ (p1, Pa.SelectList ann t)

rewriteSelectItems :: MtSchemaSpec -> MtSetting -> Provenance -> [Pa.SelectItem] -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, [Pa.SelectItem])
rewriteSelectItems spec setting p0 (item:items) trefs rFun = do
    (p1,h) <- rewriteSelectItem spec setting p0 item trefs rFun
    (p2,t) <- rewriteSelectItems spec setting p1 items trefs rFun
    Right $ (p2, h : t)
rewriteSelectItems _ _ prov [] _ _ = Right (prov, [])

rewriteSelectItem :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.SelectItem -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.SelectItem)
rewriteSelectItem spec setting p0 (Pa.SelExp ann scalExp) trefs rFun = do
    (p1,newExp) <- rewriteScalarExpr spec setting p0 scalExp trefs rFun
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
    Right $ (p1,create newExp)
rewriteSelectItem spec setting p0 (Pa.SelectItem ann scalExp newName) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 scalExp trefs rFun
    Right $ (p1, Pa.SelectItem ann h newName)

rewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, (Maybe Pa.ScalarExpr))
rewriteMaybeScalarExpr spec setting p0 (Just expr) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1, Just h)
rewriteMaybeScalarExpr _ _ prov Nothing _ _ = Right (prov, Nothing)

rewriteScalarExprList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExprList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExprList)
rewriteScalarExprList spec setting p0 (arg:args) trefs rFun = do
    (p1,newArg) <- rewriteScalarExpr spec setting p0 arg trefs rFun
    (p2,newArgs) <- rewriteScalarExprList spec setting p1 args trefs rFun
    Right (p2, (newArg : newArgs))
rewriteScalarExprList _ _ prov [] _ _ = Right (prov, [])

rewriteInList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.InList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.InList)
rewriteInList spec setting p0 (Pa.InList a elist) trefs rFun = do
    (p1,l) <- rewriteScalarExprList spec setting p0 elist trefs rFun
    Right $ (p1, Pa.InList a l)
rewriteInList spec setting p0 (Pa.InQueryExpr a sel) trefs  rFun= do
    (p1,h) <-  rFun spec setting p0 sel trefs
    Right $ (p1, Pa.InQueryExpr a h)

rewriteCases :: MtSchemaSpec -> MtSetting -> Provenance -> CasesType -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, CasesType)
rewriteCases spec setting p0 ((elist, expr):rest) trefs rFun = do
    (p1,h) <- rewriteScalarExprList spec setting p0 elist trefs rFun
    (p2,e) <- rewriteScalarExpr spec setting p1 expr trefs rFun
    (p3,l) <- rewriteCases spec setting p2 rest trefs rFun
    Right (p3, ((h, e):l))
rewriteCases _ _ prov [] _ _ = Right (prov, [])

rewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
rewriteScalarExpr spec setting p0 (Pa.PrefixOp ann opName arg) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 arg trefs rFun
    Right $ (p1, Pa.PrefixOp ann opName h)
rewriteScalarExpr spec setting p0 (Pa.PostfixOp ann opName arg) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 arg trefs rFun
    Right $ (p1, Pa.PostfixOp ann opName h)
rewriteScalarExpr spec setting p0 (Pa.BinaryOp ann opName arg0 arg1) trefs rFun = do
    (p1,b1) <- rewriteScalarExpr spec setting p0 arg0 trefs rFun
    (p2,b2) <- rewriteScalarExpr spec setting p1 arg1 trefs rFun
    Right $ (p2, Pa.BinaryOp ann opName b1 b2)
rewriteScalarExpr spec setting p0 (Pa.SpecialOp ann opName args) trefs rFun = do
    (p1,l) <- rewriteScalarExprList spec setting p0 args trefs rFun
    Right $ (p1, Pa.SpecialOp ann opName l)
rewriteScalarExpr spec setting p0 (Pa.App ann funName args) trefs rFun = do
    (p1,l) <- rewriteScalarExprList spec setting p0 args trefs rFun
    Right $ (p1,Pa.App ann funName l)
rewriteScalarExpr spec setting p0 (Pa.Parens ann expr) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1,Pa.Parens ann h)
rewriteScalarExpr spec setting p0 (Pa.InPredicate ann expr i list) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun
    (p2,l) <- rewriteInList spec setting p1 list trefs rFun
    Right $ (p2, Pa.InPredicate ann h i l)
rewriteScalarExpr spec setting p0 (Pa.Exists ann sel) trefs rFun = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right $ (p1, Pa.Exists ann h)
rewriteScalarExpr spec setting p0 (Pa.ScalarSubQuery ann sel) trefs rFun = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right $ (p1,Pa.ScalarSubQuery ann h)
rewriteScalarExpr spec setting p0 (Pa.Case ann cases els) trefs rFun = do
    (p1,c) <- rewriteCases spec setting p0 cases trefs rFun
    (p2,e) <- rewriteMaybeScalarExpr spec setting p1 els trefs rFun
    Right $ (p2, Pa.Case ann c e)
rewriteScalarExpr spec setting p0 (Pa.AggregateApp ann d expr o) trefs rFun = do
    (p1,e) <- rewriteScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1, Pa.AggregateApp ann d e o)
rewriteScalarExpr spec setting p0 (Pa.Extract ann f expr) trefs rFun = do
    (p1,e) <- rewriteScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1, Pa.Extract ann f e)
rewriteScalarExpr _ _ prov (Pa.StringLit ann s) _ _ =
    Right $ (prov, Pa.StringLit ann s)
rewriteScalarExpr _ _ prov (Pa.NumberLit ann s) _ _ =
    Right $ (prov, Pa.NumberLit ann s)
rewriteScalarExpr _ _ prov (Pa.Star ann) _ _ =
    Right $ (prov, Pa.Star ann)
rewriteScalarExpr spec (c,d,o) p0 (Pa.Identifier iAnn i) trefs _ =
    let (tableName, attName) = getTableAndAttName i
        comparability = lookupAttributeComparability spec (tableName, attName) trefs
        rewrite (Just (MtConvertible to from)) (Just tName) False =
            let (Just oldTName) = getOldTableName (Just tName) trefs
            in  Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from])
                    [Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to])
                        [Pa.Identifier iAnn i, getTenantIdentifier tName oldTName]
                    ,Pa.NumberLit iAnn (show c)]
        rewrite _ _ _ = Pa.Identifier iAnn i
    in Right $ (p0, rewrite comparability tableName (MtTrivialOptimization `elem` o && (length d == 1) && (head d == c)))
rewriteScalarExpr _ _ _ expr _ _ = Left $ FromMtRewriteError $ "Rewrite-select function not implemented yet for scalar expr " ++ show expr

