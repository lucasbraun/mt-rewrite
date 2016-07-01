module MtAnnotate
(
    mtAnnotateStatement
) where

import qualified Data.Map as M
import qualified Database.HsSqlPpp.Parse as Pa

import MtTypes

mtAnnotateStatement :: MtSchemaSpec -> Pa.Statement -> Pa.Statement
mtAnnotateStatement spec (Pa.QueryStatement a q) = Pa.QueryStatement a (annotateQuery spec q [])
mtAnnotateStatement spec (Pa.CreateView a n c q) = Pa.CreateView a n c (annotateQuery spec q [])
mtAnnotateStatement _ statement = statement

-- make sure that every attribute of a specific table appears with its table name
-- assumes that attributes not found within the schema are from global tables
-- for the case it is used in subqueries, it also take the table refs from the outer queries into account
annotateQuery :: MtSchemaSpec -> Pa.QueryExpr -> Pa.TableRefList -> Pa.QueryExpr
annotateQuery spec (Pa.Select ann selDistinct selSelectList selTref selWhere
    selGroupBy selHaving selOrderBy selLimit selOffset selOption) trefs =
        let allTrefs        = selTref ++ trefs    -- ordering matters here as the first value that fits is taken
            newSelectList   = annotateSelectList spec selTref selSelectList
            newTrefs        = annotateTrefList spec selTref
            newWhere        = annotateMaybeScalarExpr spec allTrefs selWhere
            newGroupBy      = map (annotateScalarExpr spec selTref) selGroupBy
            newHaving       = annotateMaybeScalarExpr spec selTref selHaving
            newOrderBy      = annotateDirectionList spec selTref selOrderBy
        in  Pa.Select ann selDistinct newSelectList newTrefs newWhere
            newGroupBy newHaving newOrderBy selLimit selOffset selOption
-- default case handles anything we do not handle so far
annotateQuery _ query _ = query

annotateTrefList :: MtSchemaSpec -> Pa.TableRefList -> Pa.TableRefList
annotateTrefList spec (Pa.SubTref ann sel:trefs) = Pa.SubTref ann (annotateQuery spec sel []) : annotateTrefList spec trefs
annotateTrefList spec (Pa.TableAlias ann tb tref:trefs) = Pa.TableAlias ann tb (head (annotateTrefList spec [tref])) : annotateTrefList spec trefs
annotateTrefList spec (Pa.JoinTref ann tref0 n t h tref1 (Just (Pa.JoinOn a expr)):trefs) = Pa.JoinTref ann
    (head (annotateTrefList spec [tref0]))
    n t h
    (head (annotateTrefList spec [tref1]))
    (Just (Pa.JoinOn a (annotateScalarExpr spec (tref0:[tref1]) expr))) : annotateTrefList spec trefs
annotateTrefList spec (Pa.FullAlias ann tb cols tref:trefs) = Pa.FullAlias ann tb cols
    (head (annotateTrefList spec [tref])) : annotateTrefList spec trefs
-- default case, recursively call annotation call on single item
annotateTrefList spec (tref:trefs) = tref:annotateTrefList spec trefs
annotateTrefList _ [] = []

annotateSelectList :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectList -> Pa.SelectList
annotateSelectList spec trefs (Pa.SelectList ann items) = Pa.SelectList ann (annotateSelectItems spec trefs items)

annotateSelectItems :: MtSchemaSpec -> Pa.TableRefList -> [Pa.SelectItem] -> [Pa.SelectItem]
-- replaces Star Expressions with an enumeration of all attributes instead (* would also display tenant key, which is something we do not want)
annotateSelectItems spec [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] [Pa.SelExp selAnn (Pa.Star starAnn)] =
    let tableSpec = M.lookup tname spec
        generate (Just (FromMtSpecificTable tab)) = map (\key -> Pa.SelExp selAnn (Pa.Identifier starAnn (Pa.Name starAnn [Pa.Nmc key]))) (M.keys tab)
        generate _ = [Pa.SelExp selAnn (Pa.Star starAnn)]
    in annotateSelectItems spec [Pa.Tref tAnn (Pa.Name nameAnn [Pa.Nmc tname])] (generate tableSpec)
-- default case, recursively call annotation call on single item
annotateSelectItems spec trefs (item:items) = annotateSelectItem spec trefs item : annotateSelectItems spec trefs items
annotateSelectItems _ _ [] = []

annotateSelectItem :: MtSchemaSpec -> Pa.TableRefList -> Pa.SelectItem -> Pa.SelectItem
annotateSelectItem spec trefs (Pa.SelExp ann scalExp)             = Pa.SelExp ann (annotateScalarExpr spec trefs scalExp)
annotateSelectItem spec trefs (Pa.SelectItem ann scalExp newName) = Pa.SelectItem ann (annotateScalarExpr spec trefs scalExp) newName

annotateMaybeScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Maybe Pa.ScalarExpr -> Maybe Pa.ScalarExpr
annotateMaybeScalarExpr spec trefs (Just expr) = Just (annotateScalarExpr spec trefs expr)
annotateMaybeScalarExpr _ _ Nothing = Nothing

annotateDirectionList :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExprDirectionPairList -> Pa.ScalarExprDirectionPairList
annotateDirectionList spec trefs ((expr, dir, no):list) = (annotateScalarExpr spec trefs expr, dir, no) : annotateDirectionList spec trefs list
annotateDirectionList _ _ [] = []

annotateScalarExpr :: MtSchemaSpec -> Pa.TableRefList -> Pa.ScalarExpr -> Pa.ScalarExpr
annotateScalarExpr spec trefs (Pa.PrefixOp ann opName arg) = Pa.PrefixOp ann opName (annotateScalarExpr spec trefs arg)
annotateScalarExpr spec trefs (Pa.PostfixOp ann opName arg) = Pa.PostfixOp ann opName (annotateScalarExpr spec trefs arg)
annotateScalarExpr spec trefs (Pa.BinaryOp ann opName arg0 arg1) = Pa.BinaryOp ann opName
        (annotateScalarExpr spec trefs arg0) (annotateScalarExpr spec trefs arg1)
annotateScalarExpr spec trefs (Pa.SpecialOp ann opName args) = Pa.SpecialOp ann opName (map (annotateScalarExpr spec trefs) args)
annotateScalarExpr spec trefs (Pa.App ann funName args) = Pa.App ann funName (map (annotateScalarExpr spec trefs) args)
annotateScalarExpr spec trefs (Pa.Parens ann expr) = Pa.Parens ann (annotateScalarExpr spec trefs expr)
annotateScalarExpr spec trefs (Pa.InPredicate ann expr i list) =
    let annotateInList (Pa.InList a elist) = Pa.InList a (map (annotateScalarExpr spec trefs) elist)
        annotateInList (Pa.InQueryExpr a sel) = Pa.InQueryExpr a (annotateQuery spec sel trefs)
    in  Pa.InPredicate ann (annotateScalarExpr spec trefs expr) i (annotateInList list)
annotateScalarExpr spec trefs (Pa.Exists ann sel) = Pa.Exists ann (annotateQuery spec sel trefs)
annotateScalarExpr spec trefs (Pa.ScalarSubQuery ann sel) = Pa.ScalarSubQuery ann (annotateQuery spec sel trefs)
annotateScalarExpr spec trefs (Pa.Case ann cases els) = Pa.Case ann
    (map (\(explist, e) -> (map (annotateScalarExpr spec trefs) explist, annotateScalarExpr spec trefs e)) cases)
    (annotateMaybeScalarExpr spec trefs els)
annotateScalarExpr spec (Pa.Tref _ (Pa.Name _ [Pa.Nmc tname]):trefs) (Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName:nameComps)))
    | not (null nameComps)  = Pa.Identifier iAnn (Pa.Name a (Pa.Nmc attName : nameComps))
    | otherwise             =
        let tableSpec = M.lookup tname spec
            containsAttribute (Just (FromMtSpecificTable attrMap)) aName = M.member aName attrMap 
            containsAttribute _ _ = False
            contains = containsAttribute tableSpec attName
            propagate False = annotateScalarExpr spec trefs (Pa.Identifier iAnn (Pa.Name a [Pa.Nmc attName]))
            propagate True  = Pa.Identifier iAnn (Pa.Name a [Pa.Nmc tname,Pa.Nmc attName])
        in  propagate contains
annotateScalarExpr spec (Pa.JoinTref _ tref0 _ _ _ tref1 _ : trefs) expr =
    annotateScalarExpr spec (tref0:tref1:trefs) expr
-- for any other trefs, we still need to make sure that the rest of the trefs gets checked as well
annotateScalarExpr spec (_:trefs) expr = annotateScalarExpr spec trefs expr
-- default case handles anything we do not handle so far
annotateScalarExpr _ _ expr = expr

