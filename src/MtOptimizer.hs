module MtOptimizer
(
    mtOptimize
) where

import qualified Database.HsSqlPpp.Parse as Pa

import MtTypes
import MtUtils

mtOptimize :: MtSetting -> Pa.QueryExpr -> Pa.QueryExpr
mtOptimize (_,_,o) q0 = 
    let q1
            | MtConversionDistribution  `elem` o    = applyConversionDistribution q0
            | otherwise                             = q0
        q2
            | MtFunctionInlining `elem` o   = applyFunctionInlining q1
            | otherwise                     = q1
    in  q2

-- ##################################
-- MT Optimizer
-- ##################################
--
-- There are the following optimization steps:
--
-- while (i) to (iii) are implemented as part of the normal rewrite, (iv) and (v) are separate passes through the data implemented in MtOptimizer
--
-- (i)      Trivial Optimizations:
--          - D=C optimization ==> only filter at the bottom level, rest of the query looks the same, no conversion needed
--          - |D| = 1: similar, but additionally requires a final presentation to client view at the uppermost select
-- (ii)     Client Presentation push-up: push-up conversion to client format to the uppermost select
--          --> comparisons are done in universal format if needed, constants (and results of scalar subqueries) are brought to each tenant's format
-- (iii)    Conversion push-up: present subqueries in tenant-s format or universal where needed (on joins and aggregations)
-- (iv)     Aggregation distribution: if possible, compute partial aggregations on different client formats and then only convert the final aggregated result
-- (v)      Conversion Function Inlining: inline the content of the conversion functions
-- (vi)     Statistical aggregation optimization: if (v) not possible: figure out in (intermediary) formats to convert values
--          (essentially equivalent to join ordering and site selection in distributed query processing


-- we make the following case distiction:
--  - no split: AGG(fromUniveral(X)) --> fromUniversal(AGG(X))
--  - split:    AGG(fromUniversal(toUniversal(X)) --> fromUniversal(AGG_OUTER(SELECT toUniversal(AGG_INNER(X)) FROM ... GROUP BY ttid))
applyConversionDistribution :: Pa.QueryExpr -> Pa.QueryExpr
applyConversionDistribution query
    | needsSplitQuery query == 0    = applyCDToQueryWithoutSplit query
    | needsSplitQuery query == 1    = applyCDToQueryWithSplit query
    | otherwise                     = query

applyCDToQueryWithSplit :: Pa.QueryExpr -> Pa.QueryExpr
applyCDToQueryWithSplit (Pa.Select ann selDistinct (Pa.SelectList a exps) selTref selWhere
        selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
    let (out,inn,gro) = foldl (\(o1,i1,g1) (o2,i2,g2) -> (o1 ++ [o2], i1 ++ i2, g1 ++ g2))
                        ([], [], [])
                        (map (applyCDWithSplit) exps)
        innerSelExps  =  extendInnerSelect (removeDuplicateSelectItems inn) selGroupBy
    in  (Pa.Select ann selDistinct (Pa.SelectList a out) [Pa.TableAlias ann (Pa.Nmc "tmp") (Pa.SubTref ann
            (Pa.Select ann Pa.All (Pa.SelectList a innerSelExps) selTref selWhere
                (removeDuplicates (selGroupBy ++ gro)) selHaving [] Nothing Nothing selOption))
        ] Nothing (reduceIdentifiers selGroupBy) Nothing (reduceOrderBy selOrderBy) selLimit selOffset selOption)

applyCDToQueryWithoutSplit :: Pa.QueryExpr -> Pa.QueryExpr
applyCDToQueryWithoutSplit (Pa.Select ann selDistinct (Pa.SelectList a exps) selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
    Pa.Select ann selDistinct (Pa.SelectList a (applyCDToSelectListOnly exps)) selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption

applyCDToSelectListOnly :: Pa.SelectItemList -> Pa.SelectItemList
applyCDToSelectListOnly l =
   let mapIt (Pa.SelExp a e) = Pa.SelExp a (applyCDWithoutSplit e)
       mapIt (Pa.SelectItem a e n) = Pa.SelectItem a (applyCDWithoutSplit e) n
   in  map mapIt l

-- extends the inner select with additional tenant keys if they appear in outer group by
extendInnerSelect :: Pa.SelectItemList -> Pa.ScalarExprList -> Pa.SelectItemList
extendInnerSelect sl ((Pa.Identifier iAnn (Pa.Name nAnn nameComps)):items) =
    let extend (_:[Pa.Nmc lastNameComp])
            | containsString lastNameComp "_tenant_key" = extendInnerSelect (sl ++ [Pa.SelExp iAnn (Pa.Identifier iAnn (Pa.Name nAnn nameComps))]) items
            | otherwise = extendInnerSelect sl items
        extend _ = extendInnerSelect sl items
    in  extend nameComps
extendInnerSelect sl (_:items)   = extendInnerSelect sl items
extendInnerSelect sl []             = sl

-- returns the select item for the outer relation, the select item(s) for the inner relation
-- and 0 or 1 expressions to be added to the group-by clause of the inner relation
-- the group-by clause of the outer relation equals the current group-by
-- TODO: this function is really non-exhaustive! needs to be adpated for different queries than tpch
-- TODO: there is a lot of copy-pasted, slightly modified, code. Check for simplifications...
applyCDWithSplit :: Pa.SelectItem -> (Pa.SelectItem, [Pa.SelectItem], [Pa.ScalarExpr])
applyCDWithSplit (Pa.SelectItem iAnn (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [
        (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])[att, ttid], cid])])
        newName)
    | (containsString to "ToUniversal" && containsString from "FromUniversal" && isSqlAggOp appName) =
        let result
                | isAvg appName =
                    let sumString   = "SUM_mt_" ++ (getIntermediateIdentifier att)
                        tmpSumName  = Pa.Name a0 [Pa.Nmc sumString]
                        cntString   = "COUNT_mt_all"
                        tmpCntName  = Pa.Name a0 [Pa.Nmc cntString]
                    in  (Pa.SelectItem iAnn (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [
                            (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "/"])
                                (Pa.App nAnn (Pa.Name a0 [Pa.Nmc "SUM"]) [Pa.Identifier a0 tmpSumName])
                            (Pa.App nAnn (Pa.Name a0 [Pa.Nmc "SUM"]) [Pa.Identifier a0 tmpCntName])), cid])
                            newName,
                        [Pa.SelectItem iAnn (Pa.App a2 (Pa.Name a3 [Pa.Nmc to]) [
                            Pa.App ann (Pa.Name nAnn [Pa.Nmc "SUM"]) [att],ttid]) (Pa.Nmc sumString),
                            Pa.SelectItem iAnn (Pa.App a0 (Pa.Name a0 [Pa.Nmc "COUNT"]) [Pa.Star a0]) (Pa.Nmc cntString)],
                        [ttid]
                        )
                | otherwise =
                    let tmpString   = appName ++ "_mt_" ++ (getIntermediateIdentifier att)
                        tmpName     = Pa.Name a0 [Pa.Nmc tmpString]
                    in  (Pa.SelectItem iAnn (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [
                            Pa.App ann (Pa.Name nAnn [Pa.Nmc (getOutAggFromInnerAgg appName)]) [Pa.Identifier a0 tmpName], cid]) newName,
                        [Pa.SelectItem iAnn (Pa.App a2 (Pa.Name a3 [Pa.Nmc to]) [
                            Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [att],ttid]) (Pa.Nmc tmpString)],
                        [ttid])
        in  result
    | otherwise =   (Pa.SelExp iAnn (Pa.Identifier iAnn (Pa.Name iAnn [newName])),
                    [Pa.SelectItem iAnn (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [
                        (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])[att, ttid], cid])])
                        newName],
                    [])
-- TODO: for now assumes that all binary ops are OK, which is of course not true in the general case
applyCDWithSplit (Pa.SelectItem iAnn (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [Pa.BinaryOp bAnn binOpName
        (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])[att, ttid], cid]) other])
        newName) =
    -- TODO: for now just assume:    (containsString to "ToUniversal" && containsString from "FromUniversal" && appName == "SUM" && binOpName \in (/,*)
    let tmpString   = "mt_" ++ (getIntermediateIdentifier (Pa.BinaryOp bAnn binOpName att other))
        tmpName     = Pa.Name a0 [Pa.Nmc tmpString]
    in  (Pa.SelectItem iAnn (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [
            Pa.App ann (Pa.Name nAnn [Pa.Nmc (getOutAggFromInnerAgg appName)]) [Pa.Identifier a0 tmpName], cid]) newName,
        [Pa.SelectItem iAnn (Pa.App a2 (Pa.Name a3 [Pa.Nmc to]) [
            Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [Pa.BinaryOp bAnn binOpName att other],ttid]) (Pa.Nmc tmpString)],
        [ttid])
-- TODO: for now assumes that all binary ops are OK, which is of course not true in the general case
applyCDWithSplit (Pa.SelExp iAnn (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [Pa.BinaryOp bAnn binOpName
        (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [Pa.App a2 (Pa.Name a3 [Pa.Nmc to])[att, ttid], cid]) other])) =
    -- TODO: for now just assume:    (containsString to "ToUniversal" && containsString from "FromUniversal" && appName == "SUM" && binOpName \in (/,*)
    let tmpString   = "mt_" ++ (getIntermediateIdentifier (Pa.BinaryOp bAnn binOpName att other))
        tmpName     = Pa.Name a0 [Pa.Nmc tmpString]
    in  (Pa.SelExp iAnn (Pa.App a0 (Pa.Name a1 [Pa.Nmc from]) [
            Pa.App ann (Pa.Name nAnn [Pa.Nmc (getOutAggFromInnerAgg appName)]) [Pa.Identifier a0 tmpName], cid]),
        [Pa.SelectItem iAnn (Pa.App a2 (Pa.Name a3 [Pa.Nmc to]) [
            Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [Pa.BinaryOp bAnn binOpName att other],ttid]) (Pa.Nmc tmpString)],
        [ttid])
applyCDWithSplit (Pa.SelectItem iAnn (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [att]) newName)
    | isAvg appName =
        let sumString   = "SUM_mt_" ++ (getIntermediateIdentifier att)
            tmpSumName  = Pa.Name ann [Pa.Nmc sumString]
            cntString   = "COUNT_mt_all"
            tmpCntName  = Pa.Name ann [Pa.Nmc cntString]
        in  (Pa.SelectItem iAnn 
                (Pa.BinaryOp ann (Pa.Name ann [Pa.Nmc "/"])
                    (Pa.App nAnn (Pa.Name ann [Pa.Nmc "SUM"]) [Pa.Identifier ann tmpSumName])
                (Pa.App nAnn (Pa.Name ann [Pa.Nmc "SUM"]) [Pa.Identifier ann tmpCntName]))
                newName,
            [Pa.SelectItem iAnn 
                (Pa.App ann (Pa.Name nAnn [Pa.Nmc "SUM"]) [att]) (Pa.Nmc sumString),
                Pa.SelectItem iAnn (Pa.App ann (Pa.Name ann [Pa.Nmc "COUNT"]) [Pa.Star ann]) (Pa.Nmc cntString)],
            []
            )
    | otherwise =
        let tmpString   = appName ++ "_mt_" ++ (getIntermediateIdentifier att)
            tmpName     = Pa.Name ann [Pa.Nmc tmpString]
        in  (Pa.SelectItem iAnn
                (Pa.App ann (Pa.Name nAnn [Pa.Nmc (getOutAggFromInnerAgg appName)]) [Pa.Identifier ann tmpName]) newName,
            [Pa.SelectItem iAnn 
                (Pa.App ann (Pa.Name nAnn [Pa.Nmc appName]) [att]) (Pa.Nmc tmpString)],
            [])
applyCDWithSplit (Pa.SelectItem a ex n) = (Pa.SelExp a (Pa.Identifier a (Pa.Name a [n])),
        [Pa.SelectItem a ex n],
        [])
applyCDWithSplit (Pa.SelExp a (Pa.Identifier iAnn (Pa.Name nAnn n))) = (
        Pa.SelExp a (Pa.Identifier iAnn (Pa.Name nAnn [last n])),
        [Pa.SelExp a (Pa.Identifier iAnn (Pa.Name nAnn n))],
        [])
applyCDWithSplit (Pa.SelExp a ex) = (Pa.SelExp a ex, [Pa.SelExp a ex], [])

-- ONLY WORKS if we also test for split in these cases (but not needed for TPC-H)
--applyCDWithoutSplitMaybe :: Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
--applyCDWithoutSplitMaybe (Just expr) = Just $ applyCDWithoutSplit expr
--applyCDWithoutSplitMaybe Nothing   = Nothing
--
--applyCDWithoutSplitCases :: CasesType -> CasesType
--applyCDWithoutSplitCases ((elist, expr):rest) =
--    let h   = map applyCDWithoutSplit elist
--        e   = applyCDWithoutSplit expr
--        l   = applyCDWithoutSplitCases rest
--    in  (h,e):l
--applyCDWithoutSplitCases [] = []

applyCDWithoutSplit :: Pa.ScalarExpr -> Pa.ScalarExpr
applyCDWithoutSplit (Pa.BinaryOp a n e1 e2) = Pa.BinaryOp a n (applyCDWithoutSplit e1) (applyCDWithoutSplit e2)
--applyCDWithoutSplit (Pa.Case a cas els) = Pa.Case a (applyCDWithoutSplitCases cas) (applyCDWithoutSplitMaybe els)
applyCDWithoutSplit (Pa.App a0 appName [Pa.App a1 (Pa.Name a2 [Pa.Nmc from]) [att, ttid]])
    | containsString from "FromUniversal" && isSqlAggOp (getAppName appName) =
        Pa.App a1 (Pa.Name a2 [Pa.Nmc from]) [Pa.App a0 appName [att], ttid]
    | otherwise = Pa.App a0 appName [Pa.App a1 (Pa.Name a2 [Pa.Nmc from]) [att, ttid]]
-- TODO: add more cases here if needed
applyCDWithoutSplit e = e

-- returns 0/1 for False/True and 2 if Query is not a Select
needsSplitQuery :: Pa.QueryExpr -> Int
needsSplitQuery (Pa.Select _ _ (Pa.SelectList _ list) _ _ _ _ _ _ _ _) =
    if (needsSplit list)
        then 1
        else 0
needsSplitQuery _ = 2

-- tests whether conversion distribution can happen in same select statement or has to be split into an inner and outer relation
needsSplit :: Pa.SelectItemList -> Bool
needsSplit (Pa.SelExp _ e:items) = needsSplitScalarEx e || needsSplit items
needsSplit (Pa.SelectItem _ e _:items) = needsSplitScalarEx e || needsSplit items
needsSplit []   = False

innerNeedsSplit :: Pa.ScalarExpr -> Bool
innerNeedsSplit (Pa.App _ (Pa.Name _ [Pa.Nmc from]) [Pa.App _ (Pa.Name _ [Pa.Nmc to]) _ ,_]) =
        containsString to "ToUniversal" && containsString from "FromUniversal"
innerNeedsSplit (Pa.BinaryOp _ _ e1 _) = innerNeedsSplit e1   -- TODO: this case is incomplete
innerNeedsSplit _ = False

needsSplitScalarEx :: Pa.ScalarExpr -> Bool
needsSplitScalarEx (Pa.App _ (Pa.Name _ [Pa.Nmc attName]) [inner])
    | (isCnt attName) || not (isSqlAggOp attName)   = False
    | otherwise                                 = innerNeedsSplit inner
-- TODO: add more cases here if needed
needsSplitScalarEx _ = False

-- #####################
-- # FUNCTION INLINING #
-- #####################

applyFunctionInlining :: Pa.QueryExpr -> Pa.QueryExpr
applyFunctionInlining (Pa.Select ann selDistinct (Pa.SelectList a exps) selTref selWhere
        selGroupBy selHaving selOrderBy selLimit selOffset selOption) =
    Pa.Select ann selDistinct (Pa.SelectList a (map inlineSelItem exps))
        (map inlineTableRef selTref)
        (inlineMaybeScalarExpr selWhere)
        selGroupBy
        (inlineMaybeScalarExpr selHaving)
        selOrderBy selLimit selOffset selOption

inlineSelItem :: Pa.SelectItem -> Pa.SelectItem
inlineSelItem (Pa.SelExp a ex) = Pa.SelExp a (inlineScalarExpr ex)
inlineSelItem (Pa.SelectItem a ex n) = Pa.SelectItem a (inlineScalarExpr ex) n

-- we have to do this because if MtConversionDistribution is active, this might create additional query nesting
-- handling these two cases, however, is enough
inlineTableRef :: Pa.TableRef -> Pa.TableRef
inlineTableRef (Pa.SubTref ann query) = Pa.SubTref ann (applyFunctionInlining query)
inlineTableRef (Pa.TableAlias ann n tref) = Pa.TableAlias ann n (inlineTableRef tref)
inlineTableRef t = t

-- inline recursively (without following sub queries, this will be handled by repeating calls to the optimizer from MtLib)
inlineScalarExpr :: Pa.ScalarExpr -> Pa.ScalarExpr
-- For now, we actually hard-code the functions used in tpch
inlineScalarExpr (Pa.App a0 (Pa.Name a1 [Pa.Nmc appName]) [arg1, arg2])
    | containsString appName "currencyToUniversal" = Pa.ScalarSubQuery a0 (Pa.Select a0 Pa.All
        (Pa.SelectList a0 [Pa.SelExp a0 (                                                                   -- SELECT
            Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "*"]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "CT_to_universal"])) (inlineScalarExpr arg1))])
        [Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "Tenant"]), Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "CurrencyTransform"])]   -- FROM
        (Just (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "AND"])                                                   -- WHERE
            (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_tenant_key"])) arg2)
            (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_currency_key"]))
                (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "CT_currency_key"])))))
        [] Nothing [] Nothing Nothing [])
    | containsString appName "currencyFromUniversal" = Pa.ScalarSubQuery a0 (Pa.Select a0 Pa.All
        (Pa.SelectList a0 [Pa.SelExp a0 (                                                                   -- SELECT
            Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "*"]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "CT_from_universal"])) (inlineScalarExpr arg1))])
        [Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "Tenant"]), Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "CurrencyTransform"])]   -- FROM
        (Just (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "AND"])                                                   -- WHERE
            (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_tenant_key"])) arg2)
            (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_currency_key"]))
                (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "CT_currency_key"])))))
        [] Nothing [] Nothing Nothing [])
    | containsString appName "phoneToUniversal" =
        let charlengthFun = if containsString appName "dbo." then "LEN" else "CHAR_LENGTH"
        in  Pa.ScalarSubQuery a0 (Pa.Select a0 Pa.All
            (Pa.SelectList a0 [Pa.SelExp a0 (                                                               -- SELECT
                Pa.App a0 (Pa.Name a0 [Pa.Nmc "SUBSTRING"]) [inlineScalarExpr arg1, Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "+"])
                    (Pa.App a0 (Pa.Name a0 [Pa.Nmc charlengthFun]) [Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "PT_prefix"])])
                    (Pa.NumberLit a0 "1")])])
            [Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "Tenant"]), Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "PhoneTransform"])]  -- FROM
            (Just (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "AND"])                                               -- WHERE
                (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_tenant_key"])) arg2)
                (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_phone_prefix_key"]))
                    (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "PT_phone_prefix_key"])))))
            [] Nothing [] Nothing Nothing [])
    | containsString appName "phoneFromUniversal" =
        let charlengthFun = if containsString appName "dbo." then "LEN" else "CHAR_LENGTH"
        in  Pa.ScalarSubQuery a0 (Pa.Select a0 Pa.All
            (Pa.SelectList a0 [Pa.SelExp a0 (                                                               -- SELECT
                Pa.App a0 (Pa.Name a0 [Pa.Nmc "CONCAT"]) [Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "PT_prefix"]), inlineScalarExpr arg1])])
            [Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "Tenant"]), Pa.Tref a0 (Pa.Name a0 [Pa.Nmc "PhoneTransform"])]  -- FROM
            (Just (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "AND"])                                               -- WHERE
                (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_tenant_key"])) arg2)
                (Pa.BinaryOp a0 (Pa.Name a0 [Pa.Nmc "="]) (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "T_phone_prefix_key"]))
                    (Pa.Identifier a0 (Pa.Name a0 [Pa.Nmc "PT_phone_prefix_key"])))))
            [] Nothing [] Nothing Nothing [])
    | otherwise     = Pa.App a0 (Pa.Name a1 [Pa.Nmc appName]) [inlineScalarExpr arg1, inlineScalarExpr arg2]
-- all other cases
inlineScalarExpr (Pa.PrefixOp a n ex) = Pa.PrefixOp a n (inlineScalarExpr ex)
inlineScalarExpr (Pa.PostfixOp a n ex) = Pa.PostfixOp a n (inlineScalarExpr ex)
inlineScalarExpr (Pa.BinaryOp a n e1 e2) = Pa.BinaryOp a n (inlineScalarExpr e1) (inlineScalarExpr e2)
inlineScalarExpr (Pa.SpecialOp a n exps) = Pa.SpecialOp a n (map inlineScalarExpr exps)
inlineScalarExpr (Pa.App a n exps) = Pa.App a n (map inlineScalarExpr exps)
inlineScalarExpr (Pa.Parens a ex) = Pa.Parens a (inlineScalarExpr ex)
inlineScalarExpr (Pa.InPredicate a0 ex b (Pa.InList a1 exps)) =
    Pa.InPredicate a0 (inlineScalarExpr ex) b (Pa.InList a1 (map inlineScalarExpr exps))
inlineScalarExpr (Pa.Case a0 cases els) = Pa.Case a0 (inlineCases cases) (inlineMaybeScalarExpr els)
inlineScalarExpr (Pa.AggregateApp a d ex o) = Pa.AggregateApp a d (inlineScalarExpr ex) o
inlineScalarExpr ex = ex

inlineCases :: CasesType -> CasesType
inlineCases ((elist, expr):rest) = (map inlineScalarExpr elist, inlineScalarExpr expr) : inlineCases rest
inlineCases [] = []

inlineMaybeScalarExpr :: Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
inlineMaybeScalarExpr (Just ex) = Just (inlineScalarExpr ex)
inlineMaybeScalarExpr Nothing   = Nothing

