/*
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package io.github.spark_redshift_community.spark.redshift.pushdown.optimizers

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, EqualNullSafe, EqualTo, Expression, NamedExpression, PredicateHelper, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.PushLeftSemiLeftAntiThroughJoin.PushdownDirection
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftSemi}
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, LEFT_SEMI_OR_ANTI_JOIN}


private[pushdown] object LeftSemiAntiJoinOptimizations extends PredicateHelper {
  def isDistinctAggregate(plan: LogicalPlan): Boolean = {
    plan match {
      // The idea is that if we are grouping by all child output without any transformations,
      // it should be a distinct. An additional "No transformations" check is not necessary
      // due to implicit aggregateExpr == child.output check
      case Aggregate(groupExpr, aggregateExpr, child) if groupExpr == child.output
        && aggregateExpr == child.output => true
      case _ => false
    }
  }

  // check if an expression is a valid EqualsNullSafe condition
  private def isValidEqualsNullSafe(expr: Expression, left: LogicalPlan,
                                    right: LogicalPlan): Boolean = {
    expr match {
      case EqualNullSafe(leftAttr: AttributeReference, rightAttr: AttributeReference) =>
        // Check if it contains one column from left and one column from right
        // Both should be of same datatype
        leftAttr.dataType == rightAttr.dataType &&
          ((left.outputSet.contains(leftAttr)
            && right.outputSet.contains(rightAttr)) ||
            (right.outputSet.contains(leftAttr)
              && left.outputSet.contains(rightAttr)))
      case _ => false
    }
  }

  /**
   * A leftsemi/leftanti join can be converted into intersect/except
   * if the following conditions are met.
   *
   * 1. [Intro] LeftSemi JOIN returns the tuples from the left table for which
   * the JOIN condition is met. LeftAnti JOIN returns tuples from left that for
   * which the condition is not met for any tuple in right.
   * INTERSECT is a set intersection between two tables.
   * EXCEPT is a set difference between two tables.
   * Unlike JOIN, set operations eliminates duplicates and also treats a null == null.
   * 2. left.isDistinct == true
   * 3. The JOIN condition should only have EqualsNullSafe. There can be multiple EqualsNullSafe
   * 4. All EqualsNullSafe conditions should only be joined by AND.
   * 5. Each EqualsNullSafe should contain one column from left and one column from right
   *     1. Eg. left.a <=> right.a && left.b <=> right.b
   *
   * 6. Every projected column must be part of only one EqualsNullSafe.
   * 7. Same number of projections from left and right.
   * 8. Columns in EqualsNullSafe should be in the same order. Eg. If a,b,c are projected from
   * Left and x,y,z from Right, we should have
   *     1. left.a <=> right.x && left.b <=> right.y && left.c <=> right.z
   * */
  def isSetOperation(plan: LogicalPlan, joinType: JoinType,
                             checkDistinct: Boolean): Boolean = {

    plan match {
      case Join(left, right, jt, condition, _) if jt == joinType =>
        // Left child mist be distinct. Its not required for right to be distinct,
        // as in LeftSemi/Anti Join, we only pick elements from left child.
        // Set operations produce distinct values
        if (checkDistinct && !isDistinctAggregate(left) &&
          !isSetOperation(left, joinType, checkDistinct)) {
          return false
        }

        // condition must be non empty
        if (condition.isEmpty) {
          return false
        }

        // equal number of projections in both left and right child
        if (left.output.length != right.output.length) {
          return false
        }

        val conjunctivePredicates = splitConjunctivePredicates(condition.get)

        // Check if the condition is valid
        if (!conjunctivePredicates.forall(expr => isValidEqualsNullSafe(expr, left, right))) {
          return false
        }

        // The number of conditions should be equal to number of output attributes
        if (conjunctivePredicates.length != left.output.length) {
          return false
        }

        // Collect all distinct columns involved in EqualsNullSafe conditions
        val allColumns = conjunctivePredicates.flatMap {
          case EqualNullSafe(leftAttr, rightAttr) => Seq(leftAttr, rightAttr)
        }.distinct

        // Check if the total number of distinct columns is equal to the sum of
        // left and right projections
        if (allColumns.length != left.output.length + right.output.length) {
          return false
        }

        val columnPairs = conjunctivePredicates.map {
          case EqualNullSafe(leftAttr: AttributeReference, rightAttr: AttributeReference) =>
            (leftAttr, rightAttr)
        }

        val leftOrder = left.output.zipWithIndex.toMap
        val rightOrder = right.output.zipWithIndex.toMap

        // Avoid duplicate projections
        if (leftOrder.size != left.output.length || rightOrder.size != right.output.length) {
          return false;
        }

        // Check if columns in EqualsNullSafe are in the same order
        columnPairs.forall {
          case (leftAttr, rightAttr) =>
            (leftOrder.contains(leftAttr) && rightOrder.contains(rightAttr) &&
              leftOrder.get(leftAttr) == rightOrder.get(rightAttr)) ||
              (rightOrder.contains(leftAttr) && leftOrder.contains(rightAttr) &&
                rightOrder.get(leftAttr) == leftOrder.get(rightAttr))
        }

      case _ =>
        false
    }
  }

  def isPassThroughProjection(projectList: Seq[NamedExpression],
                                      childPlan: LogicalPlan): Boolean = {
    projectList.forall {
      case Alias(childAttr: NamedExpression, _) => childPlan.outputSet.contains(childAttr)
      case childAttr: AttributeReference => childPlan.outputSet.contains(childAttr)
      case _ => false
    }
  }

  private object AllowedJoin {
    def unapply(join: Join): Option[Join] = join.joinType match {
      case LeftSemi => Some(join)
      case _ => None
    }
  }

  private object AllowedInnerJoin {
    def unapply(join: Join): Option[Join] = join.joinType match {
      case Inner => Some(join)
      case _ => None
    }
  }

  private def canPushThroughCondition(
                                       plan: LogicalPlan,
                                       condition: Option[Expression],
                                       rightOp: LogicalPlan): Boolean = {
    val attributes = AttributeSet(plan.output)
    if (condition.isDefined) {
      val matched = condition.get.references.intersect(rightOp.outputSet).intersect(attributes)
      matched.isEmpty
    } else {
      true
    }
  }

  /**
   * This rule tries undoes the following files
   * Rules: PushDownLeftSemiAntiJoin and PushLeftSemiLeftAntiThroughJoin
   * over inner joins and projects
   * This is required to convert potential leftsemijoins to intersects
   * */
  def pullUpLeftSemiJoinOverProjectAndInnerJoin(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transformDownWithPruning(_.containsAllPatterns(LEFT_SEMI_OR_ANTI_JOIN,
      AGGREGATE)) {
      case agg@Aggregate(_, _, project@Project(_, _: Join)) if isDistinctAggregate(agg)
        && isPassThroughProjection(project.projectList, project.child) =>

        val newChild = tryPullUpLeftSemiOrAnti(project)
        if (newChild == project) agg else agg.copy(child = newChild)
    }

    newPlan
  }

  private def canPullUp(childLeft: LogicalPlan,
                        childRight: LogicalPlan,
                        childCondition: Option[Expression],
                        newJoin: Join, direction: PushdownDirection.Value): Boolean = {

    // check if the left child projects an extra column (due to pushing inside projects
    // and inner joins)
    // Selective pull-up for optimization as left-semi  with extra columns cannot be
    // converted to intersect
    val extraColumns = (childLeft.outputSet ++ childRight.outputSet) -- extractReferences(childCondition)
    if (childLeft.output.size - childRight.output.size != 1 ||
      childLeft.outputSet.size != childLeft.output.size ||
      childRight.outputSet.size != childRight.output.size || extraColumns.size != 1) {
      return false
    }

    // This is only for safety check/assert. Ideally we should be able to pull up any left semi
    // over inner join
    newJoin match {
      case Join(AllowedInnerJoin(left), right, _,
      joinCond, _) =>

        val l = left.left
        val r = left.right
        val rightOutput = right.outputSet

        if (joinCond.nonEmpty) {
          val conditions = splitConjunctivePredicates(joinCond.get)
          val (leftConditions, rest) =
            conditions.partition(_.references.subsetOf(l.outputSet ++ rightOutput))
          val (rightConditions, commonConditions) =
            rest.partition(_.references.subsetOf(r.outputSet ++ rightOutput))

          if (rest.isEmpty && leftConditions.nonEmpty) {

            if (direction != PushdownDirection.TO_LEFT_BRANCH) {
              return false
            }
          } else if (leftConditions.isEmpty && rightConditions.nonEmpty
            && commonConditions.isEmpty) {
            if (direction != PushdownDirection.TO_RIGHT_BRANCH) {
              return false
            }
          } else {
            return false
          }

        } else {
          if (direction != PushdownDirection.TO_LEFT_BRANCH) {
            return false
          }
        }
    }
    true
  }

  private def tryPullUpLeftSemiOrAnti(plan: LogicalPlan):
  LogicalPlan = {
    plan match {
      case leftSemiJoin@Join(_, _, LeftSemi, _, _)
        if isSetOperation(leftSemiJoin, LeftSemi, checkDistinct = true) =>
        leftSemiJoin
      case project@Project(_, child) =>
        val newChild = tryPullUpLeftSemiOrAnti(child)
        val newProject = project.copy(child = newChild)

        newProject match {
          case Project(pList, AllowedJoin(child))
            if pList.forall(_.deterministic) &&
              !pList.exists(ScalarSubquery.hasCorrelatedScalarSubquery)
              && canPushThroughCondition(child.left, child.condition, child.right)
              && isPassThroughProjection(pList, child) && child.condition.isDefined
              && splitConjunctivePredicates(child.condition.get).forall(expr
                => isValidEqualsNullSafe(expr, child.left, child.right)) =>

            // Try pulling up leftsemijoin

            if (child.condition.isEmpty) {
              // No join condition, just push down the Join below Project
              Join(Project(pList, child.left), child.right, child.joinType,
                child.condition, child.hint)
            } else {
              val sourceToAliasMap = AttributeMap(pList.collect {
                case a@Alias(child: AttributeReference, _) => (child, a.toAttribute)
              })

              val newJoinCond = child.condition.map { expr =>
                expr.transformUp {
                  case a: Attribute => sourceToAliasMap.getOrElse(a, a)
                }
              }
              Join(Project(pList, child.left), child.right, child.joinType,
                newJoinCond, child.hint)
            }
          case _ =>
            newProject
        }

      case innerJoin@Join(left, right, Inner, condition, _)
        if isSingleEquiJoin(condition, left.outputSet, right.outputSet) =>
        val newLeft = tryPullUpLeftSemiOrAnti(left)
        val newRight = tryPullUpLeftSemiOrAnti(right)
        val nj = innerJoin.copy(left = newLeft, right = newRight)

        nj match {
          case j@Join(left, AllowedJoin(right), Inner, joinCond, parentHint)
            if isSingleEquiJoin(joinCond, left.outputSet, right.outputSet) &&
              right.condition.isDefined
              && splitConjunctivePredicates(right.condition.get).forall(expr
              => isValidEqualsNullSafe(expr, right.left, right.right)) =>
            val (childJoinType, childLeft, childRight, childCondition, childHint) =
              (right.joinType, right.left, right.right, right.condition, right.hint)

            // The semi join is in the right. So it could have been pushed to right.
            // construct the pulled up leftsemi join
            val newJoin = Join(Join(left, childLeft, j.joinType, joinCond, parentHint),
              childRight, childJoinType, childCondition, childHint)

            if (canPullUp(childLeft, childRight, childCondition,
              newJoin, PushdownDirection.TO_RIGHT_BRANCH)) {
              newJoin
            } else {
              j
            }

          case j@Join(AllowedJoin(left), right, Inner, joinCond, parentHint)
            if isSingleEquiJoin(joinCond, left.outputSet, right.outputSet) &&
              left.condition.isDefined
              && splitConjunctivePredicates(left.condition.get).forall(expr
              => isValidEqualsNullSafe(expr, left.left, left.right)) =>
            val (childJoinType, childLeft, childRight, childCondition, childHint) =
              (left.joinType, left.left, left.right, left.condition, left.hint)

            // The semi join is in the left. So it could have been pushed to left.
            // construct the pulled up leftsemi join
            val newJoin = Join(Join(childLeft, right, j.joinType, joinCond, parentHint),
              childRight, childJoinType, childCondition, childHint)

            if (canPullUp(childLeft, childRight, childCondition,
              newJoin, PushdownDirection.TO_LEFT_BRANCH)) {
              newJoin
            } else {
              j
            }
        }
      case other =>
        other
    }
  }

  private def isSingleEquiJoin(maybeExpression: Option[Expression],
                               left: AttributeSet, right: AttributeSet): Boolean = {
    maybeExpression match {
      // Single equi-join condition and no self join
      case Some(EqualTo(leftAttr: AttributeReference, rightAttr: AttributeReference))
        if (left.contains(leftAttr) && right.contains(rightAttr) ||
          left.contains(rightAttr) && right.contains(leftAttr)) &&
          left.intersect(right).isEmpty => true
      case _ =>
        false // Any other condition is not a valid equi-join
    }
  }

  private def extractReferences(condition: Option[Expression]): AttributeSet = {
    condition.map(_.references).getOrElse(AttributeSet.empty)
  }
}