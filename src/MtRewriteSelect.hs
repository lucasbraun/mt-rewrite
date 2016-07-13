module MtRewriteSelect (
    rewriteSelectList
) where

import qualified Database.HsSqlPpp.Parse as Pa

import MtTypes
import MtUtils

rewriteSelectList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.SelectList -> Pa.TableRefList -> RewriteQueryFun -> Bool
        -> Either MtRewriteError (Provenance, Pa.SelectList)
rewriteSelectList spec (c,d,o) p0 (Pa.SelectList ann items) trefs rFun isUpperMost = do
    (p1,t1)    <- rewriteSelectItems spec (c,d,o) p0 items trefs rFun
    let p2      = flattenProvenance p1
    let p3      = pruneProvenance p2
    if MtConversionPushUp `elem` o
        then
            if isUpperMost
                then Right (p3, Pa.SelectList ann (rewriteUpperMostSelectList (c,d,o) p3 t1))
            else do
                let (p4,t2) = consolidateSelectItems t1 p3
                Right (p4, Pa.SelectList ann t2)
        else Right (p3, Pa.SelectList ann t1)

-- adds client presentation to necessary attributes
-- only makes sence if client-push-up is enabled
rewriteUpperMostSelectList :: MtSetting -> Provenance -> Pa.SelectItemList -> Pa.SelectItemList
rewriteUpperMostSelectList (c,d,o) prov selItems
    | MtClientPresentationPushUp `elem` o   =
        let apply (Pa.SelectItem ann scalExp newName: items)    =
                Pa.SelectItem ann (rewriteUpperMostScalarExpr (c,d,o) prov scalExp) newName : apply items
            apply (Pa.SelExp ann scalExp: items)                =
                Pa.SelExp ann (rewriteUpperMostScalarExpr (c,d,o) prov scalExp) : apply items
            apply []    = []
        in apply selItems
    | otherwise = selItems

-- apps have to be treated specially in case it is the application of a convertion function
rewriteUpperMostApplication :: MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.ScalarExpr
rewriteUpperMostApplication (c,d,o) prov (Pa.App a0 (Pa.Name a1 [Pa.Nmc to]) [Pa.Identifier a2 i, arg0])
    | containsString to "ToUniversal" =
        let idf                 = Pa.Identifier a2 i
            ([provItem],_)       = getProvenanceItem idf prov
        in  createConvFunctionApplication (fromUniversal provItem)
            (Pa.App a0 (Pa.Name a1 [Pa.Nmc to]) [idf, arg0]) (Pa.NumberLit a0 (show c))
    | otherwise = Pa.App a0 (Pa.Name a1 [Pa.Nmc to]) [
        rewriteUpperMostScalarExpr (c,d,o) prov (Pa.Identifier a2 i),
        rewriteUpperMostScalarExpr (c,d,o) prov arg0]
rewriteUpperMostApplication setting prov (Pa.App a n exps) = Pa.App a n (map (rewriteUpperMostScalarExpr setting prov) exps)

rewriteUpperMostMaybe :: MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
rewriteUpperMostMaybe setting prov (Just expr) = Just $ rewriteUpperMostScalarExpr setting prov expr
rewriteUpperMostMaybe _ _ Nothing   = Nothing

rewriteUpperMostCases :: MtSetting -> Provenance -> CasesType -> CasesType
rewriteUpperMostCases setting prov ((elist, expr):rest) =
    let h   = map (rewriteUpperMostScalarExpr setting prov) elist
        e   = rewriteUpperMostScalarExpr setting prov expr
        l   = rewriteUpperMostCases setting prov rest
    in  (h,e):l
rewriteUpperMostCases _ _ [] = []

-- used by rewriteUpperMostSelectList to do the final rewrite to client presentation
rewriteUpperMostScalarExpr :: MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.ScalarExpr
rewriteUpperMostScalarExpr setting prov (Pa.PrefixOp a n ex) = Pa.PrefixOp a n (rewriteUpperMostScalarExpr setting prov ex)
rewriteUpperMostScalarExpr setting prov (Pa.PostfixOp a n ex) = Pa.PostfixOp a n (rewriteUpperMostScalarExpr setting prov ex)
rewriteUpperMostScalarExpr setting prov (Pa.BinaryOp a n exp1 exp2) = Pa.BinaryOp a n
        (rewriteUpperMostScalarExpr setting prov exp1) (rewriteUpperMostScalarExpr setting prov exp2)
rewriteUpperMostScalarExpr setting prov (Pa.SpecialOp a n exps) = Pa.SpecialOp a n (
        map (rewriteUpperMostScalarExpr setting prov) exps)
rewriteUpperMostScalarExpr setting prov (Pa.App a n exps) = rewriteUpperMostApplication setting prov (Pa.App a n exps)
rewriteUpperMostScalarExpr setting prov (Pa.Parens a ex) = Pa.Parens a (rewriteUpperMostScalarExpr setting prov ex)
rewriteUpperMostScalarExpr setting prov (Pa.Case ann cases els) = 
    let newCases    = rewriteUpperMostCases setting prov cases
        newElse     = rewriteUpperMostMaybe setting prov els
    in  Pa.Case ann newCases newElse
rewriteUpperMostScalarExpr setting prov (Pa.AggregateApp ann d exprs o) =
    Pa.AggregateApp ann d (rewriteUpperMostScalarExpr setting prov exprs) o
rewriteUpperMostScalarExpr (c,_,o) prov (Pa.Identifier iAnn i) = 
    let idf                 = Pa.Identifier iAnn i
        (provItems, _)      = getProvenanceItem idf prov
        apply [provItem]    = 
            let conv
                    | (MtConversionPushUp `elem` o) && not (converted provItem) =
                        createConvFunctionApplication (toUniversal provItem) idf (tenantField provItem)
                    | otherwise = idf
            in  createConvFunctionApplication (fromUniversal provItem) conv (Pa.NumberLit iAnn (show c))
        apply _           = idf        -- default case where item is not special in any regard
    in apply provItems
-- the rest of the scalar exprs should not matter
rewriteUpperMostScalarExpr _ _ e    = e

-- rewrites converted expressions that are SelectExprs to SelectItems by using their old name
-- for the items in provenance that are not converted (and not renamed), adds tenant identifier to selection
-- and updates provenance accordingly
-- for the moment, implementation focuses on TPC-H Q22
consolidateSelectItems :: Pa.SelectItemList -> Provenance -> (Provenance, Pa.SelectItemList)
consolidateSelectItems (Pa.SelExp ann (Pa.Identifier i a): items) prov =
    -- as we only check for identifiers, we know that we deal with items that are not converted yet
    let (p, newItems)           = consolidateSelectItems items prov
        item                    = Pa.Identifier i a
        (provItems, attName)    = getProvenanceItem item p
        consolidate [provItem]  =
            let tenantFieldName = getIntermediateTenantIdentifier item
                newTenantField  = Pa.SelectItem ann (tenantField provItem) (Pa.Nmc tenantFieldName)
                newProvItem     = ProvenanceItem {fieldName=fieldName provItem, toUniversal=toUniversal provItem,
                    fromUniversal=fromUniversal provItem,
                    tenantField=Pa.Identifier ann (Pa.Name ann [Pa.Nmc tenantFieldName]),
                    converted=converted provItem, shouldConvert=shouldConvert provItem}
                newProv         = replaceProvenanceItem p attName newProvItem
            in  (newProv, Pa.SelExp ann item:newTenantField:newItems)
        consolidate _           = (p, Pa.SelExp ann item :newItems)        -- default case where item is not special in any regard
    in consolidate provItems
consolidateSelectItems (Pa.SelectItem ann e (Pa.Nmc n): items) prov =
    -- for the examples we look at, we know e is in universal format --> only add provenance for its new name
    let (p, newItems)           = consolidateSelectItems items prov
        (provItems, _)          = getProvenanceItem e p
        consolidate [provItem]  = addToProvenance n provItem p
        consolidate _           = p
    in (consolidate provItems, Pa.SelectItem ann e (Pa.Nmc n):newItems)
consolidateSelectItems (item:items) prov =
    let (p, newItems)   = consolidateSelectItems items prov
    in  (p, item:newItems)
consolidateSelectItems [] prov  = (prov, [])

rewriteSelectItems :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.SelectItemList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.SelectItemList)
rewriteSelectItems spec setting p0 (item:items) trefs rFun = do
    (p1,h) <- rewriteSelectItem spec setting p0 item trefs rFun
    (p2,t) <- rewriteSelectItems spec setting p1 items trefs rFun
    Right (p2, h : t)
rewriteSelectItems _ _ prov [] _ _ = Right (prov, [])

-- rewrites each select item according to the rules
-- rewrites converted expressions that are SelectExprs to SelectItems by using their old name
rewriteSelectItem :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.SelectItem -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.SelectItem)
rewriteSelectItem spec (c,d,o) p0 (Pa.SelExp ann scalExp) trefs rFun = do
    (p1,newExp) <- rewriteScalarExpr spec (c,d,o) p0 scalExp trefs rFun (MtConversionPushUp `notElem` o)
    -- 'create' makes sure that a converted attribute gets renamed to its original name
    let create (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                    [Pa.Identifier a4 i, arg0]
                ,Pa.NumberLit a5 arg1]) 
                    | containsString to "ToUniversal" && containsString from "FromUniversal" =
                        let (_ , Just attName) = getTableAndAttName i
                        in  Pa.SelectItem ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1]) (Pa.Nmc attName)
                    | otherwise = Pa.SelExp ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc from])
                            [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])
                                [Pa.Identifier a4 i, arg0]
                            ,Pa.NumberLit a5 arg1])
        create (Pa.App a0 (Pa.Name a1 [Pa.Nmc to]) [Pa.Identifier a2 i, arg0])
                    | containsString to "ToUniversal" = 
                        let (_ , Just attName) = getTableAndAttName i
                        in  Pa.SelectItem ann (Pa.App a0 (Pa.Name a1 [Pa.Nmc to])
                            [Pa.Identifier a2 i, arg0]) (Pa.Nmc attName)
                    | otherwise = Pa.SelExp ann (
                        Pa.App a0 (Pa.Name a1 [Pa.Nmc to]) [Pa.Identifier a2 i, arg0])
        create expr = Pa.SelExp ann expr
    Right (p1,create newExp)
rewriteSelectItem spec (c,d,o) p0 (Pa.SelectItem ann scalExp newName) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec (c,d,o) p0 scalExp trefs rFun (MtConversionPushUp `notElem` o)
    Right (p1, Pa.SelectItem ann h newName)

rewriteMaybeScalarExpr :: MtSchemaSpec -> MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Maybe Pa.ScalarExpr)
rewriteMaybeScalarExpr spec setting p0 (Just expr) trefs rFun = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun True
    Right (p1, Just h)
rewriteMaybeScalarExpr _ _ prov Nothing _ _ = Right (prov, Nothing)

rewriteScalarExprList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExprList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExprList)
rewriteScalarExprList spec setting p0 (arg:args) trefs rFun = do
    (p1,newArg) <- rewriteScalarExpr spec setting p0 arg trefs rFun True
    (p2,newArgs) <- rewriteScalarExprList spec setting p1 args trefs rFun
    Right (p2, newArg : newArgs)
rewriteScalarExprList _ _ prov [] _ _ = Right (prov, [])

rewriteInList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.InList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.InList)
rewriteInList spec setting p0 (Pa.InList a elist) trefs rFun = do
    (p1,l) <- rewriteScalarExprList spec setting p0 elist trefs rFun
    Right (p1, Pa.InList a l)
rewriteInList spec setting p0 (Pa.InQueryExpr a sel) trefs  rFun= do
    (p1,h) <-  rFun spec setting p0 sel trefs
    Right (p1, Pa.InQueryExpr a h)

rewriteCases :: MtSchemaSpec -> MtSetting -> Provenance -> CasesType -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, CasesType)
rewriteCases spec setting p0 ((elist, expr):rest) trefs rFun = do
    (p1,h) <- rewriteScalarExprList spec setting p0 elist trefs rFun
    (p2,e) <- rewriteScalarExpr spec setting p1 expr trefs rFun True
    (p3,l) <- rewriteCases spec setting p2 rest trefs rFun
    Right (p3, (h, e):l)
rewriteCases _ _ prov [] _ _ = Right (prov, [])

-- the boolean tells us whether a conversion has to happen
-- if it does not have to happen, we simply add the predicate to provenance
rewriteIdentifier :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> Bool
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
rewriteIdentifier spec (c,_,o) prov (Pa.Identifier iAnn i) trefs b =
    let triple  = getConversionFunctions spec trefs i
        idf     = Pa.Identifier iAnn i
        rewrite (Just (to, from, (Just tName, Just attName))) =
            let (Just oldTName) = getOldTableName (Just tName) trefs
                tidf            = getTenantIdentifier tName oldTName
                result
                    | b =
                        let universal   = createConvFunctionApplication to idf tidf
                            rewritten
                                | MtClientPresentationPushUp `elem` o = universal
                                | otherwise = createConvFunctionApplication from universal (Pa.NumberLit iAnn (show c))
                        in  (addIdentifierToProvenance prov (to, from, (Just tName, Just attName)) idf tidf True True, rewritten)
                    | otherwise = (addIdentifierToProvenance prov (to, from, (Just tName, Just attName)) idf tidf False False, idf)
            in Right result
        rewrite _ = Right (prov, idf)
    in rewrite triple
rewriteIdentifier _ _ prov idf _ _ = Right (prov, idf)

-- the boolean in the end tells whether a conversion has to happen (because the scalar expr is part of a more complex
-- expression). Otherwise, we may defer conversion to later
rewriteScalarExpr :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun -> Bool
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
rewriteScalarExpr spec setting p0 (Pa.PrefixOp ann opName arg) trefs rFun _ = do
    (p1,h) <- rewriteScalarExpr spec setting p0 arg trefs rFun True
    Right (p1, Pa.PrefixOp ann opName h)
rewriteScalarExpr spec setting p0 (Pa.PostfixOp ann opName arg) trefs rFun _ = do
    (p1,h) <- rewriteScalarExpr spec setting p0 arg trefs rFun True
    Right (p1, Pa.PostfixOp ann opName h)
rewriteScalarExpr spec setting p0 (Pa.BinaryOp ann opName arg0 arg1) trefs rFun _ = do
    (p1,b1) <- rewriteScalarExpr spec setting p0 arg0 trefs rFun True
    (p2,b2) <- rewriteScalarExpr spec setting p1 arg1 trefs rFun True
    Right (p2, Pa.BinaryOp ann opName b1 b2)
rewriteScalarExpr spec setting p0 (Pa.SpecialOp ann opName args) trefs rFun _ = do
    (p1,l) <- rewriteScalarExprList spec setting p0 args trefs rFun
    Right (p1, Pa.SpecialOp ann opName l)
rewriteScalarExpr spec setting p0 (Pa.App ann funName args) trefs rFun _ = do
    (p1,l) <- rewriteScalarExprList spec setting p0 args trefs rFun
    Right (p1,Pa.App ann funName l)
rewriteScalarExpr spec setting p0 (Pa.Parens ann expr) trefs rFun b = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun b
    Right (p1,Pa.Parens ann h)
rewriteScalarExpr spec setting p0 (Pa.InPredicate ann expr i list) trefs rFun _ = do
    (p1,h) <- rewriteScalarExpr spec setting p0 expr trefs rFun True
    (p2,l) <- rewriteInList spec setting p1 list trefs rFun
    Right (p2, Pa.InPredicate ann h i l)
rewriteScalarExpr spec setting p0 (Pa.Exists ann sel) trefs rFun _ = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right (keepRecommendations p1, Pa.Exists ann h)
rewriteScalarExpr spec setting p0 (Pa.ScalarSubQuery ann sel) trefs rFun _ = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right (keepRecommendations p1,Pa.ScalarSubQuery ann h)
rewriteScalarExpr spec setting p0 (Pa.Case ann cases els) trefs rFun _ = do
    (p1,c) <- rewriteCases spec setting p0 cases trefs rFun
    (p2,e) <- rewriteMaybeScalarExpr spec setting p1 els trefs rFun
    Right (p2, Pa.Case ann c e)
rewriteScalarExpr spec setting p0 (Pa.AggregateApp ann d expr o) trefs rFun _ = do
    (p1,e) <- rewriteScalarExpr spec setting p0 expr trefs rFun True
    Right (p1, Pa.AggregateApp ann d e o)
rewriteScalarExpr spec setting p0 (Pa.Extract ann f expr) trefs rFun _ = do
    (p1,e) <- rewriteScalarExpr spec setting p0 expr trefs rFun True
    Right (p1, Pa.Extract ann f e)
rewriteScalarExpr _ _ prov (Pa.StringLit ann s) _ _ _ =
    Right (prov, Pa.StringLit ann s)
rewriteScalarExpr _ _ prov (Pa.NumberLit ann s) _ _ _ =
    Right (prov, Pa.NumberLit ann s)
rewriteScalarExpr _ _ prov (Pa.Star ann) _ _ _ =
    Right (prov, Pa.Star ann)
rewriteScalarExpr spec (c,d,o) p0 (Pa.Identifier iAnn i) trefs _ b =
    if MtTrivialOptimization `elem` o && (length d == 1) && (head d == c)
        then Right (p0, Pa.Identifier iAnn i)
        else rewriteIdentifier spec (c,d,o) p0 (Pa.Identifier iAnn i) trefs b
rewriteScalarExpr _ _ _ expr _ _ _ = Left $ FromMtRewriteError $ "Rewrite-select function not implemented yet for scalar expr " ++ show expr

