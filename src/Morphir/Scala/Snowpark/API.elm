module Morphir.Scala.Snowpark.API exposing (..)

import Morphir.Scala.AST as Scala

import Morphir.Spark.API as API
import Morphir.IR.Value exposing (literal)
import Morphir.Spark.Expression exposing (Expression(..))

literalCreation : Scala.Value -> Scala.Value
literalCreation lit =
        Scala.Apply (Scala.Ref [ "com", "snowflake", "snowpark", "functions" ] "lit")
            [ Scala.ArgValue Nothing lit ]

wrapLiteral : Scala.Value -> Scala.Value
wrapLiteral branch =
        case branch of
             Scala.Literal x  ->
                literalCreation (Scala.Literal x)
             _ ->
                branch

snowparkApi : API.DataFrameApi
snowparkApi = {

    isInMethod = \obj args ->
        Method obj "in" [Function "Seq" args]

    , isNullFunction = \args ->
        Function "is_null" args

    , whenExpression = \args ->
        Function "when" (case args of
                            [condition, Literal _ as value] ->
                                [condition, Function "lit" [value]]
                            _ ->
                                args)

        
    , functionsNamespace = [ "com", "snowflake", "snowpark", "functions" ]

    ,dataFrame =
        Scala.TypeRef [ "com", "snowflake", "snowpark" ] "DataFrame"
    
    , literal = literalCreation

    , column = \name ->
        Scala.Apply (Scala.Ref [ "com", "snowflake", "snowpark", "functions" ] "col")
            [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit name)) ]


    , when = \cond thenBranch ->
             Scala.Apply (Scala.Ref [ "com", "snowflake", "snowpark", "functions" ] "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing (wrapLiteral thenBranch) ]


    , andWhen = \cond thenBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing (wrapLiteral thenBranch) ]


    , otherwise = \elseBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "otherwise")
            [ Scala.ArgValue Nothing  (wrapLiteral elseBranch) ]


    , alias = \name value ->
        Scala.Apply
            (Scala.Select value "alias")
            [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit name)) ]


    , select = \columns from ->
        Scala.Apply
            (Scala.Select
                from
                "select"
            )
            (columns
                |> List.map (Scala.ArgValue Nothing)
            )


    , filter = \predicate from ->
        Scala.Apply
            (Scala.Select
                from
                "filter"
            )
            [ Scala.ArgValue Nothing predicate
            ]


    , join = \rightRelation predicate joinTypeLabel leftRelation ->
        Scala.Apply
            (Scala.Select
                leftRelation
                "join"
            )
            [ Scala.ArgValue Nothing rightRelation
            , Scala.ArgValue Nothing predicate
            , Scala.ArgValue Nothing
                (Scala.Literal
                    (Scala.StringLit joinTypeLabel)
                )
            ]


    , aggregate = \groupField aggregations from ->
        Scala.Apply
            (Scala.Select
                (Scala.Apply
                    (Scala.Select from "groupBy")
                    [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit groupField)) ]
                )
                "agg"
            )
            (aggregations
                |> List.map (Scala.ArgValue Nothing)
            )
 }