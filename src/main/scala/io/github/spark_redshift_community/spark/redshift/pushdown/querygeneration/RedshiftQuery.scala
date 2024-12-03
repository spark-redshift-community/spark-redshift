/*
* Copyright 2015-2018 Snowflake Computing
* Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import io.github.spark_redshift_community.spark.redshift.pushdown.{ConstantString, EmptyRedshiftSQLStatement, RedshiftSQLStatement}
import io.github.spark_redshift_community.spark.redshift.{RedshiftFailMessage, RedshiftPushdownUnsupportedException, RedshiftRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, LocalRelation, LogicalPlan, MergeAction, UpdateAction}

import scala.language.postfixOps

/** Building blocks of a translated query, with nested subqueries. */
private[querygeneration] abstract sealed class RedshiftQuery {

  /** Output columns. */
  lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty
    else {
      helper.output
        .map { col =>
          val orig_name =
            if (col.metadata.contains(ORIG_NAME)) {
              col.metadata.getString(ORIG_NAME)
            } else col.name

          Alias(Cast(col, col.dataType), orig_name)(
            col.exprId,
            Seq.empty[String],
            Some(col.metadata)
          )
        }
        .map(_.toAttribute)
    }

  val helper: QueryHelper

  /** What comes after the FROM clause. */
  val suffixStatement: RedshiftSQLStatement = EmptyRedshiftSQLStatement()

  def expressionToStatement(expr: Expression): RedshiftSQLStatement =
    convertStatement(expr, helper.colSet)

  /** Converts this query into a String representing the SQL.
    *
    * @param useAlias Whether or not to alias this translated block of SQL.
    * @return SQL statement for this query.
    */
  def getStatement(useAlias: Boolean = false): RedshiftSQLStatement = {
    log.debug(s"""Generating a query of type: ${getClass.getSimpleName}""")

    def getCols = {
      val cols = helper.columns
      if(cols.isEmpty || cols.get.isEmpty) {
        ConstantString("*") !
      } else {
        cols.get
      }
    }

    val stmt =
      ConstantString("SELECT") + getCols + "FROM" +
        helper.sourceStatement + suffixStatement

    if (useAlias) {
      blockStatement(stmt, helper.alias)
    } else {
      stmt
    }
  }

  /** Finds a particular query type in the overall tree.
    *
    * @param query PartialFunction defining a positive result.
    * @tparam T RedshiftQuery type
    * @return Option[T] for one positive match, or None if nothing found.
    */
  def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (helper.children.isEmpty) None
        else helper.children.head.find(query)
      )

  /** Determines if two RedshiftQuery subtrees can be joined together.
    *
    * @param otherTree The other tree, can it be joined with this one?
    * @return True if can be joined, or False if not.
    */
  def canJoin(otherTree: RedshiftQuery): Boolean = {
    val result = for {
      myBase <- find { case q: SourceQuery => q }
      otherBase <- otherTree.find {
        case q: SourceQuery => q
      }
    } yield {
      myBase.cluster == otherBase.cluster
    }

    result.getOrElse(false)
  }
}

/** The query for a base type.
  *
  * @constructor
  * @param relation   The base RedshiftRelation representing the basic table, view,
  *                   or subquery defined by the user.
  * @param refColumns Columns used to override the output generation for the QueryHelper.
  *                   These are the columns resolved by RedshiftRelation.
  * @param alias      Query alias.
  */
abstract sealed class BaseQuery(redshiftRelation: RedshiftRelation,
                                refCols: Seq[Attribute],
                                queryAlias: String)
  extends RedshiftQuery {
  val relation = redshiftRelation
  val refColumns = refCols
  val alias = queryAlias
}

/** The query for a source base type (representing a table or view).
 *
 * @constructor
 * @param relation   The base RedshiftRelation representing the basic table, view,
 *                   or subquery defined by the user.
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by RedshiftRelation.
 * @param alias      Query alias.
 */
case class SourceQuery(override val relation: RedshiftRelation,
                       override val refColumns: Seq[Attribute],
                       override val alias: String)
  extends BaseQuery(relation, refColumns, alias) {

  override val helper: QueryHelper = QueryHelper(
    children = Seq.empty,
    projections = None,
    outputAttributes = Some(refColumns),
    alias = alias,
      conjunctionStatement = buildAliasStatement(
      relation.params.query
        .map(ConstantString("(") + _ + ")") // user input query, don't parse
        .getOrElse(relation.params.table.get.toConstantString !),
      "rs_connector_query_alias"
    )
  )

  /** Triplet that defines the Redshift cluster that houses this base relation.
    * Currently an exact match on cluster is needed for a join, but we may not need
    * to be this strict.
    */
  val cluster: (String, Option[String], String) = (
    relation.params.uniqueClusterName, None, ""
  )

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query.lift(this)
}

/** The query for a target base type (for DMLs).
 *
 * @constructor
 * @param relation   The base RedshiftRelation representing the basic table defined by the user.
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by RedshiftRelation.
 * @param alias      Query alias.
 */
case class TargetQuery(override val relation: RedshiftRelation,
                       override val refColumns: Seq[Attribute],
                       override val alias: String)
  extends BaseQuery(relation, refColumns, alias) {

  override val helper: QueryHelper = QueryHelper(
    children = Seq.empty,
    projections = None,
    outputAttributes = Some(refColumns),
    alias = alias,
    conjunctionStatement = buildAliasStatement(relation.params.table.get.toStatement.toStatement,
      "rt_connector_query_alias")
  )

  /** Triplet that defines the Redshift cluster that houses this base relation.
   * Currently an exact match on cluster is needed for a join, but we may not need
   * to be this strict.
   */
  val cluster: (String, Option[String], String) = (
    relation.params.uniqueClusterName, None, ""
  )

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query.lift(this)

  def updateQualifier(qualifierStr: Seq[String])
  : PartialFunction[Expression, Expression] = {
    case a: AttributeReference
      if refColumns.exists(_.exprId == a.exprId) =>
      a.withQualifier(qualifierStr)
  }
}

/** The query for a local relation (representing Spark internal rows).
 *
 * @constructor
 * @param relation   LocalRelation node for scanning data from a local collection.
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by LocalRelation.
 * @param alias      Query alias.
 */
case class LocalQuery(relation: LocalRelation,
                      refColumns: Seq[Attribute],
                      alias: String)
  extends RedshiftQuery {

  override val helper: QueryHelper = QueryHelper(
    children = Seq.empty,
    projections = None,
    outputAttributes = Some(refColumns),
    alias = alias
    )

  override def getStatement(useAlias: Boolean = false): RedshiftSQLStatement = {
    log.debug(s"""Generating a query of type: ${getClass.getSimpleName}""")
    val stmt = generateSelectStatement(relation)
    if (useAlias) {
      blockStatement(stmt, helper.alias)
    } else {
      stmt
    }
  }

  private def generateSelectStatement(relation: LocalRelation)
  : RedshiftSQLStatement = {
    val literals: Seq[Seq[Literal]] = extractLiterals(relation)
    ConstantString("(") + formatLiterals(output, literals) + ConstantString(")")
  }

  // Function to extract literals in local relation
  private def extractLiterals(plan: LocalRelation): Seq[Seq[Literal]] = {
    val schema = plan.schema
    // Convert each row to a sequence of Literal expressions
    plan.data.map { row =>
      row.toSeq(schema).zip(schema).map { case (value, field) =>
        Literal(value, field.dataType)
      }
    }
  }

  private def formatLiterals(output: Seq[Attribute], literals: Seq[Seq[Literal]]): String = {
    literals.map { row =>
      val formattedRow = row.zipWithIndex.map { case (l, index) =>
        val colName = output(index).name
        expressionToStatement(l) + "AS" + s""""$colName""""
      }.mkString(", ")
      s"(SELECT $formattedRow)"
    }.mkString(" UNION ALL ")
  }
}

case class DeleteQuery (targetQuery: TargetQuery, condition: Expression,
                        usingTable: Option[RedshiftQuery] = None) extends RedshiftQuery {
  override val helper: QueryHelper = QueryHelper(
    children = usingTable.toSeq,
    projections = None,
    outputAttributes = Some(targetQuery.refColumns),
    alias = targetQuery.alias,
    conjunctionStatement = buildAliasStatement(
      targetQuery.relation.params.table.get.toStatement.toStatement,
      "rt_connector_query_alias")
  )

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    targetQuery.find(query)

  override def getStatement(useAlias: Boolean): RedshiftSQLStatement = {
    if (targetQuery.relation.params.table.isEmpty) {
      throw new RedshiftPushdownUnsupportedException(
        "Unable to pushdown delete query for relation without dbtable",
        "delete from",
        "No table to delete from",
        true
      )
    }

    val tableName = targetQuery.relation.params.table.get
    val tableQualifier: String = tableName.toString.drop(1).dropRight(1)

    // Redshift does not support using aliases in delete from statements like
    // delete from table as t where t.id = 1. In order to account for this, any
    // attribute references like t.id must have their qualifiers replaced like table.id
    // Any attribute references in the logical plan that have a matching exprId to
    // a column in the relation that is being deleted from with have its qualifier replaced
    // in this way.
    val conditionStmt: RedshiftSQLStatement =
      ConstantString("WHERE") + expressionToStatement(
        condition.transform(targetQuery.updateQualifier(Seq(tableQualifier)))
      )

    if (usingTable.isDefined) {
      ConstantString("DELETE FROM") + tableName.toStatement + "USING" +
        usingTable.get.getStatement(true) + conditionStmt
    } else {
      ConstantString("DELETE FROM") + tableName.toStatement + conditionStmt
    }
  }
}
case class MergeQuery(targetQuery: TargetQuery,
                      sourceQuery: RedshiftQuery,
                      mergeCondition: Expression,
                      matchedActions: Seq[MergeAction],
                      notMatchedActions: Seq[MergeAction],
                      notMatchedBySourceActions: Seq[MergeAction])
  extends RedshiftQuery {
  override val helper: QueryHelper = QueryHelper(
    children = Seq(sourceQuery),
    projections = None,
    outputAttributes = Some(targetQuery.refColumns),
    alias = targetQuery.alias,
    conjunctionStatement = buildAliasStatement(
      targetQuery.relation.params.table.get.toStatement.toStatement,
      "rt_connector_query_alias")
  )

  override def find[T](query: PartialFunction[RedshiftQuery, T])
  : Option[T] = targetQuery.find(query)

  override def getStatement(useAlias: Boolean): RedshiftSQLStatement = {
    if (targetQuery.relation.params.table.isEmpty) {
      throw new RedshiftPushdownUnsupportedException(
        "Unable to pushdown merge query for relation without dbtable",
        "set from",
        "No table to set from",
        true
      )
    }

    def checkIdenticalAction(action1: MergeAction, action2: MergeAction)
        : Boolean = {
      implicit object AssignmentOrdering extends Ordering[Assignment] {
        def compare(a1: Assignment, a2: Assignment): Int =
          a1.toString().compareTo(a2.toString())
      }
      (action1, action2) match {
        case (DeleteAction(_), DeleteAction(_)) =>
          true
        case (InsertAction(_, assignments1), InsertAction(_, assignments2)) =>
          assignments1.sorted(AssignmentOrdering) == assignments2.sorted(AssignmentOrdering)
        case (UpdateAction(_, assignments1), UpdateAction(_, assignments2)) =>
          assignments1.sorted(AssignmentOrdering) == assignments2.sorted(AssignmentOrdering)
        case _ =>
          false
      }
    }

    def removeRedundantMergeActions(mergeActions: Seq[MergeAction]) : Seq[MergeAction] = {
      // Redundant unmatched condition occurs when:
      // 1. There are multiple matched/unmatched action
      // 2. One of the condition is empty (unconditioned)
      // 3. All actions are identical
      val unconditionedAction = mergeActions.find(action => action.condition.isEmpty)
      val identicalAction = if (unconditionedAction.nonEmpty) {
        mergeActions.forall(checkIdenticalAction(_, unconditionedAction.get))
      } else {
        false
      }

      if (identicalAction) {
        Seq(unconditionedAction.get)
      } else {
        mergeActions
      }
    }

    val localNotMatchedActions = removeRedundantMergeActions(notMatchedActions)
    val localMatchedActions = removeRedundantMergeActions(matchedActions)

    // Early Rejection invalid syntax
    if( !(localMatchedActions.size <= 1 && localNotMatchedActions.size == 1 &&
      notMatchedBySourceActions.isEmpty) ) {
      throw new RedshiftPushdownUnsupportedException(
        RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_MERGE,
        "UNSUPPORTED MERGE SYNTAX for Redshift",
        "There should be no more than one matched action, one unmatched action," +
          "and no unmatched BY SOURCE action.",
        true
      )
    }

    // Reject AND in conditional matched/unmatched action
    if (localMatchedActions.nonEmpty && localMatchedActions.head.condition.nonEmpty ||
        localNotMatchedActions.head.condition.nonEmpty  ) {
      // the "AND condition" in matched/unmatched action is not supported by Redshift
      throw new RedshiftPushdownUnsupportedException(
        RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_MERGE,
        "Conditional MATCHED/UNMATCHED action not supported",
        "\"AND\" expression not supported in MERGE by Redshift",
        true
      )
    }

    val targetTableName = targetQuery.relation.params.table.get
    val tableQualifier: String = targetTableName.toString.drop(1).dropRight(1)

    val mergeCondExpression = mergeCondition.
      transform(targetQuery.updateQualifier(Seq(tableQualifier)))

    val matchedExpression = localMatchedActions.headOption match {
      case Some(UpdateAction(_, assignments)) =>
        val assignmentsStatement = assignments.map { assignment =>
          // Redshift only take column name for key, setting qualifier to empty str
          expressionToStatement(Assignment(assignment.key.transform(
            targetQuery.updateQualifier(Seq())),
            assignment.value.transform(targetQuery.updateQualifier(Seq(tableQualifier)))))
        }.mkString(", ")
        s"UPDATE SET $assignmentsStatement"
      case Some(DeleteAction(_)) =>
        "DELETE"
      case None => // No-Op equivalent operation when no matchedAction
        val cols = targetQuery.refColumns.head
        val nopAssignment = Assignment(cols,
          cols.withQualifier(Seq(tableQualifier)))
        "UPDATE SET " + expressionToStatement(nopAssignment)
      case _ =>
        val errStmt = expressionToStatement(localMatchedActions.head).toString
        throw new RedshiftPushdownUnsupportedException(
          RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_MERGE,
          s"Unsupported UNMATCHED Action of MERGE $errStmt. Only UPDATE and DELETE.",
          "MergeQuery",
          true
        )
    }

    val unMatchedExpression = localNotMatchedActions.head match {
      case InsertAction(_, assignments) =>
        val pairs = assignments.map { assignment =>
          val assignmentExpr = Assignment(assignment.key.transform(
            targetQuery.updateQualifier(Seq())),
            assignment.value.transform(targetQuery.updateQualifier(Seq(tableQualifier))))
          val keyStatement = expressionToStatement(assignmentExpr.key).statementString
          val valueStatement = expressionToStatement(assignmentExpr.value).statementString
          (keyStatement, valueStatement)
        }
        "(" + pairs.map(_._1).mkString(", ") + ") " +
          "VALUES (" + pairs.map(_._2).mkString(", ") + " )"
       case _ =>
         val errStmt = expressionToStatement(localNotMatchedActions.head).toString
         throw new RedshiftPushdownUnsupportedException(
           RedshiftFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_MERGE,
           s"Unsupported UNMATCHED Action of MERGE $errStmt. Only INSERT is supported.",
           "MergeQuery",
           true
         )
    }

    val res = ConstantString("MERGE INTO") + targetTableName.toStatement +
      ConstantString("USING")  + sourceQuery.getStatement(true) + ConstantString("ON") +
      expressionToStatement(mergeCondExpression) +
      ConstantString("WHEN MATCHED THEN") + matchedExpression +
      ConstantString("WHEN NOT MATCHED THEN INSERT") + unMatchedExpression
    res
  }
}

case class UpdateQuery(targetQuery: TargetQuery,
                       assignments: Seq[Assignment],
                       condition: Option[Expression])
  extends RedshiftQuery {
  override val helper: QueryHelper = targetQuery.helper

  override def find[T](query: PartialFunction[RedshiftQuery, T])
  : Option[T] = targetQuery.find(query)

  override def getStatement(useAlias: Boolean): RedshiftSQLStatement = {
    if (targetQuery.relation.params.table.isEmpty) {
      throw new RedshiftPushdownUnsupportedException(
        "Unable to pushdown update query for relation without dbtable",
        "UPDATE table",
        "No table to update",
        true
      )
    }

    val assignmentsStmt = assignments.map { assignment =>
      // Redshift only take column name for key, setting qualifier to empty str
      expressionToStatement(Assignment(assignment.key.transform(targetQuery.updateQualifier(Seq())),
        assignment.value.transform(targetQuery.updateQualifier(Seq("rt_connector_query_alias")))))
    }.mkString(", ")

    val conditionStmt : RedshiftSQLStatement = condition
      .map(value => ConstantString("WHERE") + expressionToStatement(
        value.transform(targetQuery.updateQualifier(Seq("rt_connector_query_alias")))))
      .getOrElse(EmptyRedshiftSQLStatement())

    ConstantString("UPDATE") + targetQuery.helper.sourceStatement +
      ConstantString("SET") + assignmentsStmt + conditionStmt
  }
}

case class InsertQuery (targetQuery: TargetQuery, redshiftQuery: Option[RedshiftQuery],
                        overwrite: Boolean) extends RedshiftQuery {
  override val helper: QueryHelper = targetQuery.helper

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] = targetQuery
    .find(query)

  override def getStatement(useAlias: Boolean): RedshiftSQLStatement = {
    if (targetQuery.relation.params.table.isEmpty) {
      throw new RedshiftPushdownUnsupportedException(
        "Unable to pushdown insert query for relation without dbtable",
        "insert into",
        "No table to insert into",
        true
      )
    }

    if (overwrite) {
      throw new RedshiftPushdownUnsupportedException(
        "Unable to pushdown insert query with overwrite mode",
        "insert overwrite",
        "Redshift doesn't support Overwrite mode",
        true
      )
    }

    val tableName = targetQuery.relation.params.table.get

    if (redshiftQuery.isEmpty) {
      EmptyRedshiftSQLStatement()
    } else {
      ConstantString("INSERT INTO") + tableName.toStatement + redshiftQuery.get.getStatement()
    }
  }
}

/** The query for a filter operation.
  *
  * @constructor
  * @param conditions The filter condition.
  * @param child      The child node.
  * @param alias      Query alias.
  */
case class FilterQuery(conditions: Seq[Expression],
                       child: RedshiftQuery,
                       alias: String,
                       fields: Option[Seq[Attribute]] = None)
    extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias,
      fields = fields
    )

  override val suffixStatement: RedshiftSQLStatement =
    ConstantString("WHERE") + mkStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}

/** The query for a projection operation.
  *
  * @constructor
  * @param columns The projection columns.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class ProjectQuery(columns: Seq[NamedExpression],
                        child: RedshiftQuery,
                        alias: String)
    extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = Some(columns),
      outputAttributes = None,
      alias = alias
    )
}

/** The query for a aggregation operation.
  *
  * @constructor
  * @param columns The projection columns, containing also the aggregate expressions.
  * @param groups  The grouping columns.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class AggregateQuery(columns: Seq[NamedExpression],
                          groups: Seq[Expression],
                          child: RedshiftQuery,
                          alias: String)
    extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = if (columns.isEmpty) None else Some(columns),
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: RedshiftSQLStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        mkStatement(groups.map(expressionToStatement), ",")
    } else {
      ConstantString("LIMIT 1").toStatement
    }
}

/** The query for Sort and Limit operations.
  *
  * @constructor
  * @param limit   Limit expression.
  * @param orderBy Order By expressions.
  * @param child   The child node.
  * @param alias   Query alias.
  */
case class SortLimitQuery(limit: Option[Expression],
                          orderBy: Seq[Expression],
                          child: RedshiftQuery,
                          alias: String)
    extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: RedshiftSQLStatement = {
    val statementFirstPart =
      if (orderBy.nonEmpty) {
        ConstantString("ORDER BY") + mkStatement(
          orderBy.map(expressionToStatement),
          ","
        )
      } else {
        EmptyRedshiftSQLStatement()
      }

      statementFirstPart + limit
        .map(ConstantString("LIMIT") + expressionToStatement(_))
        .getOrElse(EmptyRedshiftSQLStatement())

  }
}

/** The query for join operations.
  *
  * @constructor
  * @param left       The left query subtree.
  * @param right      The right query subtree.
  * @param conditions The join conditions.
  * @param joinType   The join type.
  * @param alias      Query alias.
  */
case class JoinQuery(left: RedshiftQuery,
                     right: RedshiftQuery,
                     conditions: Option[Expression],
                     joinType: JoinType,
                     alias: String)
    extends RedshiftQuery {

  val conj: String = joinType match {
    case Inner if conditions.isEmpty =>
      // Treat empty join conditions as cross joins
      "CROSS JOIN"
    case Inner =>
      // keep the nullability for both projections
      "INNER JOIN"
    case LeftOuter =>
      // Update the column's nullability of right table as true
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "LEFT OUTER JOIN"
    case RightOuter =>
      // Update the column's nullability of left table as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      "RIGHT OUTER JOIN"
    case FullOuter =>
      // Update the column's nullability of both tables as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "FULL OUTER JOIN"
    case Cross => "CROSS JOIN"
    case _ => throw new MatchError
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left, right),
      projections = Some(
        left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
      ),
      outputAttributes = None,
      alias = alias,
      conjunctionStatement = ConstantString(conj) !
    )

  override val suffixStatement: RedshiftSQLStatement =
    conditions
      .map(ConstantString("ON") + expressionToStatement(_))
      .getOrElse(EmptyRedshiftSQLStatement())

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

case class LeftSemiJoinQuery(left: RedshiftQuery,
                             right: RedshiftQuery,
                             conditions: Option[Expression],
                             isAntiJoin: Boolean = false,
                             alias: Iterator[String])
    extends RedshiftQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left),
      projections = Some(left.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias.next
    )

  val cond: Seq[Expression] =
    if (conditions.isEmpty) Seq.empty else Seq(conditions.get)

  val anti: String = if (isAntiJoin) " NOT " else " "

  override val suffixStatement: RedshiftSQLStatement =
    ConstantString("WHERE") + anti + "EXISTS" + blockStatement(
      FilterQuery(
        conditions = cond,
        child = right,
        alias = alias.next,
        fields = Some(
          left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
        )
      ).getStatement()
    )

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

/** The query for union.
  *
  * @constructor
  * @param children Children of the union expression.
  */
case class UnionQuery(children: Seq[LogicalPlan],
                      alias: String,
                      outputCols: Option[Seq[Attribute]] = None)
    extends RedshiftQuery {

  val queries: Seq[RedshiftQuery] = children.map { child =>
    new QueryBuilder(child).treeRoot
  }

  if(queries.contains(null)) {
    throw new RedshiftPushdownUnsupportedException(
      RedshiftFailMessage.FAIL_PUSHDOWN_STATEMENT,
      "Union",
      "Not all Union children query supported",
      false)
  }
  override val helper: QueryHelper =
    QueryHelper(
      children = queries,
      outputAttributes = Some(queries.head.helper.output),
      alias = alias,
      visibleAttributeOverride =
        Some(queries.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output).map(
          a =>
            AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
              a.exprId,
              Seq[String](alias)
            )))
    )

  override def getStatement(useAlias: Boolean): RedshiftSQLStatement = {
    val query =
      if (queries.nonEmpty) {
        mkStatement(
          queries.map(c => blockStatement(c.getStatement())),
          "UNION ALL"
        )
      } else {
        EmptyRedshiftSQLStatement()
      }

    if (useAlias) {
      blockStatement(query, alias)
    } else {
      query
    }
  }

  override def find[T](query: PartialFunction[RedshiftQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        queries
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}

/** Query including a windowing clause.
  *
  * @constructor
  * @param windowExpressions The windowing expressions.
  * @param child             The child query.
  * @param alias             Query alias.
  * @param fields            The fields generated by this query referenceable by the parent.
  */
case class WindowQuery(windowExpressions: Seq[NamedExpression],
                       child: RedshiftQuery,
                       alias: String,
                       fields: Option[Seq[Attribute]])
    extends RedshiftQuery {

  val projectionVector: Seq[NamedExpression] =
    windowExpressions ++ child.helper.outputWithQualifier

  // We need to reorder the projections based on the output vector
  val orderedProjections: Option[Seq[NamedExpression]] =
    fields.map(_.map(reference => {
      val origPos = projectionVector.map(_.exprId).indexOf(reference.exprId)
      projectionVector(origPos)
    }))

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = orderedProjections,
      outputAttributes = None,
      alias = alias
    )
}
