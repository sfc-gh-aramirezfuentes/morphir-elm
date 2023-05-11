module Morphir.Spark.API exposing (..)

import Morphir.Scala.AST as Scala
import Morphir.Spark.Expression exposing (Expression)
import Morphir.Spark.Expression exposing (Expression(..))

type alias DataFrameApi = {
    dataFrame : Scala.Type
    , literal : Scala.Value -> Scala.Value
    , column : String -> Scala.Value
    , when : Scala.Value -> Scala.Value -> Scala.Value
    , andWhen : Scala.Value -> Scala.Value -> Scala.Value -> Scala.Value
    , otherwise : Scala.Value -> Scala.Value -> Scala.Value
    , alias : String -> Scala.Value -> Scala.Value
    , select : List Scala.Value -> Scala.Value -> Scala.Value
    , filter : Scala.Value -> Scala.Value -> Scala.Value
    , join : Scala.Value -> Scala.Value -> String -> Scala.Value -> Scala.Value
    , aggregate : String -> List Scala.Value -> Scala.Value -> Scala.Value
    , functionsNamespace : List String
    , whenExpression : List Expression -> Expression
    , isInMethod : Expression -> List Expression -> Expression
    , isNullFunction : List Expression -> Expression
 }

sparkApi : DataFrameApi
sparkApi = {

    isInMethod = \obj args ->
        Method obj "isin" args

    , isNullFunction = \args ->
        Function "isnull" args

    , whenExpression = \args ->
        Function "when" args

    ,functionsNamespace = [ "org", "apache", "spark", "sql", "functions" ]

    ,dataFrame =
        Scala.TypeRef [ "org", "apache", "spark", "sql" ] "DataFrame"
    
    , literal = \lit ->
        Scala.Apply (Scala.Ref [ "org", "apache", "spark", "sql", "functions" ] "lit")
            [ Scala.ArgValue Nothing lit ]


    , column = \name ->
        Scala.Apply (Scala.Ref [ "org", "apache", "spark", "sql", "functions" ] "col")
            [ Scala.ArgValue Nothing (Scala.Literal (Scala.StringLit name)) ]


    , when = \cond thenBranch ->
        Scala.Apply (Scala.Ref [ "org", "apache", "spark", "sql", "functions" ] "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing thenBranch ]


    , andWhen = \cond thenBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "when")
            [ Scala.ArgValue Nothing cond, Scala.ArgValue Nothing thenBranch ]


    , otherwise = \elseBranch soFar ->
        Scala.Apply
            (Scala.Select soFar "otherwise")
            [ Scala.ArgValue Nothing elseBranch ]


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