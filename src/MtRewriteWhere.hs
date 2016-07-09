module MtRewriteWhere (
    rewriteHavingClause
    ,rewriteWhereClause
) where

import qualified Database.HsSqlPpp.Parse as Pa
import qualified Database.HsSqlPpp.Annotation as A

import MtTypes
import MtUtils

rewriteHavingClause :: MtSchemaSpec -> MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, (Maybe Pa.ScalarExpr))
rewriteHavingClause spec (c,d,o) p0 clause trefs rFun = do
            (p1, convertedHaving) <- convertClause spec (c,d,o) p0 clause trefs rFun    -- adds conversion functions
            adjustedClause <- adjustClause spec (c,d,o) convertedHaving trefs           -- adds predicates on tenant keys
            Right (p1, adjustedClause)

rewriteWhereClause :: MtSchemaSpec -> MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, (Maybe Pa.ScalarExpr))
rewriteWhereClause spec (c,d,o) p0 clause seltref trefs rFun = do
    (p1,convertedWhere) <- rewriteHavingClause spec (c,d,o) p0 clause trefs rFun  -- the first two steps are the same as HAVING
    Right $ (p1, filterWhereClause spec d seltref convertedWhere)                 -- adds D-filters

-- adds conversion functions whenever necessary and also triggers rewriting a whole scalar subquery in necessary
convertClause :: MtSchemaSpec -> MtSetting -> Provenance -> Maybe Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, (Maybe Pa.ScalarExpr))
convertClause spec setting p0 (Just expr) trefs rFun = do
    (p1,h) <- convertScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1, Just h)
convertClause _ _ prov Nothing _ _ = Right (prov, Nothing)

convertScalarExprList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExprList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExprList)
convertScalarExprList spec setting p0 (arg:args) trefs rFun = do
    (p1,newArg) <- convertScalarExpr spec setting p0 arg trefs rFun
    (p2,newArgs) <- convertScalarExprList spec setting p1 args trefs rFun
    Right (p2, (newArg : newArgs))
convertScalarExprList _ _ prov [] _ _ = Right (prov, [])

convertInList :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.InList -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.InList)
convertInList spec setting p0 (Pa.InList a elist) trefs rFun = do
    (p1,l) <- convertScalarExprList spec setting p0 elist trefs rFun
    Right $ (p1, Pa.InList a l)
convertInList spec setting p0 (Pa.InQueryExpr a sel) trefs rFun = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right $ (p1, Pa.InQueryExpr a h)

convertCases :: MtSchemaSpec -> MtSetting -> Provenance -> CasesType -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, CasesType)
convertCases spec setting p0 ((elist, expr):rest) trefs rFun = do
    (p1,h) <- convertScalarExprList spec setting p0 elist trefs rFun
    (p2,e) <- convertScalarExpr spec setting p1 expr trefs rFun
    (p3,l) <- convertCases spec setting p2 rest trefs rFun
    Right (p3, ((h, e):l))
convertCases _ _ prov [] _ _ = Right (prov, [])

-- should only be called with Conversion Push up enabled!
-- if both args are identifiers (or derived from identifiers) [from different tables], bring to universal format,
-- otherwise compare constants
convertComparisonOp :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
convertComparisonOp spec setting p0 (Pa.BinaryOp ann opName arg0 arg1) trefs rFun = do
-- dummy implmenetation for now
     (p1,b1) <- convertScalarExpr spec setting p0 arg0 trefs rFun
     (p2,b2) <- convertScalarExpr spec setting p1 arg1 trefs rFun
     Right $ (p2, Pa.BinaryOp ann opName b1 b2)

-- convertLiteral :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
--         -> Either MtRewriteError (Provenance, Pa.ScalarExpr)

-- at this point, we certainly convert the identifier no matter what
convertIdentifier :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList 
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
convertIdentifier spec (c,_,o) prov (Pa.Identifier iAnn i) trefs =
    let triple  = getConversionFunctions spec trefs i
        idf     = Pa.Identifier iAnn i
        convert (Just (to, from, (Just tName, Just attName))) =
            let (Just oldTName) = getOldTableName (Just tName) trefs
                tidf            = getTenantIdentifier tName oldTName
                universal       = Pa.App iAnn (Pa.Name iAnn [Pa.Nmc to]) [idf, tidf]
                rewritten
                    | MtClientPresentationPushUp `elem` o   = universal
                    | otherwise = Pa.App iAnn (Pa.Name iAnn [Pa.Nmc from])[universal,Pa.NumberLit iAnn (show c)]
                newProv = addIdentifierToProvenance prov (to, from, (Just tName, Just attName)) idf tidf False True
            in Right $ (newProv, rewritten)
        convert _ = Right (prov, idf)
    in convert triple
convertIdentifier _ _ prov idf _ = Right (prov, idf)

-- recursively convert a scalar expression
convertScalarExpr :: MtSchemaSpec -> MtSetting -> Provenance -> Pa.ScalarExpr -> Pa.TableRefList -> RewriteQueryFun
        -> Either MtRewriteError (Provenance, Pa.ScalarExpr)
convertScalarExpr spec setting p0 (Pa.PrefixOp ann opName arg) trefs rFun = do
    (p1,h) <- convertScalarExpr spec setting p0 arg trefs rFun
    Right $ (p1, Pa.PrefixOp ann opName h)
convertScalarExpr spec setting p0 (Pa.PostfixOp ann opName arg) trefs rFun = do
    (p1,h) <- convertScalarExpr spec setting p0 arg trefs rFun
    Right $ (p1, Pa.PostfixOp ann opName h)
convertScalarExpr spec (c,d,o) p0 (Pa.BinaryOp ann opName arg0 arg1) trefs rFun =
    if MtConversionPushUp `elem` o && isComparisonOp (Pa.BinaryOp ann opName arg0 arg1)
        then
            convertComparisonOp spec (c,d,o) p0
                (Pa.BinaryOp ann opName arg0 arg1) trefs rFun
        else do
            let setting = (c,d,o)
            (p1,b1) <- convertScalarExpr spec setting p0 arg0 trefs rFun
            (p2,b2) <- convertScalarExpr spec setting p1 arg1 trefs rFun
            Right $ (p2, Pa.BinaryOp ann opName b1 b2)
convertScalarExpr spec setting p0 (Pa.SpecialOp ann opName args) trefs rFun = do
    (p1,l) <- convertScalarExprList spec setting p0 args trefs rFun
    Right $ (p1, Pa.SpecialOp ann opName l)
convertScalarExpr spec setting p0 (Pa.App ann funName args) trefs rFun = do
    (p1,l) <- convertScalarExprList spec setting p0 args trefs rFun
    Right $ (p1, Pa.App ann funName l)
convertScalarExpr spec setting p0 (Pa.Parens ann expr) trefs rFun = do
    (p1,h) <- convertScalarExpr spec setting p0 expr trefs rFun
    Right $ (p1, Pa.Parens ann h)
convertScalarExpr spec setting p0 (Pa.InPredicate ann expr i list) trefs rFun = do
    (p1,h) <- convertScalarExpr spec setting p0 expr trefs rFun
    (p2,l) <- convertInList spec setting p1 list trefs rFun
    Right $ (p2, Pa.InPredicate ann h i l)
convertScalarExpr spec setting p0 (Pa.Exists ann sel) trefs rFun = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right $ (p1, Pa.Exists ann h)
convertScalarExpr spec setting p0 (Pa.ScalarSubQuery ann sel) trefs rFun = do
    (p1,h) <- rFun spec setting p0 sel trefs
    Right $ (p1, Pa.ScalarSubQuery ann h)
convertScalarExpr spec setting p0 (Pa.Case ann cases els) trefs rFun = do
    (p1,c) <- convertCases spec setting p0 cases trefs rFun
    (p2,e) <- convertClause spec setting p1 els trefs rFun
    Right $ (p2, Pa.Case ann c e)
convertScalarExpr _ _ prov (Pa.StringLit ann s) _ _ =
    Right $ (prov, Pa.StringLit ann s)
convertScalarExpr _ _ prov (Pa.NumberLit ann s) _ _ =
    Right $ (prov, Pa.NumberLit ann s)
convertScalarExpr spec (c,d,o) p0 (Pa.Identifier iAnn i) trefs _ = -- at this point, we assume that we have to convert
    if (MtTrivialOptimization `elem` o && (length d == 1) && (head d == c))
        then Right $ (p0, Pa.Identifier iAnn i)
        else convertIdentifier spec (c,d,o) p0 (Pa.Identifier iAnn i) trefs
convertScalarExpr _ _ _ expr _ _ = Left $ FromMtRewriteError $ "Rewrite-where function not implemented yet for scalar expr " ++ show expr

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
    let checkNecessary  = isComparisonOp (Pa.BinaryOp ann (Pa.Name oAnn [Pa.Nmc opName]) (Pa.Identifier i0 n0) (Pa.Identifier i1 n1))
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
-- TODO: add other predicates if necessary
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
-- all other trefs are filtered by calling the rewrite mechnism recursively on them
addDFilter _ _ _ whereClause _ = whereClause

