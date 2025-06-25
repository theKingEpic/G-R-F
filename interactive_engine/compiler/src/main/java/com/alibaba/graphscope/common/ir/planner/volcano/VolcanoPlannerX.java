/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.planner.volcano;

import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.planner.PlannerGroup;
import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
/**
 * GraphScope 扩展的 Volcano 规划器，专门用于图查询的基于成本优化 (CBO)。
 *
 * <p>该类继承自 Apache Calcite 的 {@code VolcanoPlanner}，针对 GraphScope 的图查询场景
 * 进行了定制化扩展。主要用于 CBO 模式下的图模式匹配优化，能够基于统计信息和成本估算
 * 选择最优的查询执行计划。
 *
 * <p>与标准 Volcano 规划器的主要区别：
 * <ul>
 *   <li>重写了 {@code getCost} 方法，提供更精确的成本计算逻辑</li>
 *   <li>支持 {@code RelSubset} 的最佳成本获取</li>
 *   <li>处理无约定 (NONE Convention) 节点的无限成本设置</li>
 *   <li>累积计算输入节点的成本和非累积成本</li>
 * </ul>
 *
 * <p>该规划器在 GraphScope CBO 优化流程中的作用：
 * <ol>
 *   <li>接收图模式匹配的逻辑计划</li>
 *   <li>应用 CBO 优化规则（如 {@code ExtendIntersectRule}、{@code JoinDecompositionRule}）</li>
 *   <li>基于 Glogue 统计信息进行成本估算</li>
 *   <li>选择成本最低的执行计划</li>
 * </ol>
 *
 * <p>成本计算策略：
 * <ul>
 *   <li>对于 {@code RelSubset}，直接返回其缓存的最佳成本</li>
 *   <li>对于无约定节点，根据配置返回无限成本</li>
 *   <li>递归计算所有输入节点的累积成本</li>
 *   <li>添加当前节点的非累积成本</li>
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 在 PlannerGroup 中创建 CBO 匹配规划器
 * if (config.getOpt() == PlannerConfig.Opt.CBO) {
 *     VolcanoPlanner planner = new VolcanoPlannerX();
 *     planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
 *     planner.setTopDownOpt(true);
 *     // 添加 CBO 优化规则...
 * }
 * }</pre>
 *
 * @author GraphScope Team
 * @since 1.0
 * @see org.apache.calcite.plan.volcano.VolcanoPlanner
 * @see PlannerGroup#createMatchPlanner()
 * @see GraphRelOptimizer
 */
public class VolcanoPlannerX extends VolcanoPlanner {
    /**
     * 计算输入节点的成本上界。
     *
     * <p>当前实现直接返回传入的上界，未实现剪枝优化。
     *
     * @param mExpr 当前表达式节点
     * @param upperBound 成本上界
     * @return 输入节点的成本上界
     * @todo 支持剪枝优化以提升性能
     */
    @Override
    protected RelOptCost upperBoundForInputs(RelNode mExpr, RelOptCost upperBound) {
        // todo: support pruning optimizations
        return upperBound;
    }
    /**
     * 获取关系节点的总成本，包括累积成本和非累积成本。
     *
     * <p>该方法是 CBO 优化的核心，提供精确的成本计算逻辑：
     * <ol>
     *   <li>对于 {@code RelSubset}，直接返回其最佳成本</li>
     *   <li>检查无约定节点的无限成本设置</li>
     *   <li>递归计算所有输入节点的累积成本</li>
     *   <li>添加当前节点的非累积成本</li>
     * </ol>
     * R等价关系表达式是在查询优化中能够产生相同结果但执行方式不同的关系代数表达式。 在 Volcano 优化器中，RelSubset 就是用来管理这些等价表达式的集合。
     * RelSubset 维护了一组等价的关系表达式，并跟踪其中成本最低的执行计划。 当系统需要获取成本时，它直接返回已经计算好的 bestCost 字段值，这个值代表该等价集合中最优执行计划的成本。
     * 为什么能直接返回最优成本
     * 这是因为 Volcano 优化器采用了动态规划的思想：
     * 预计算优化：在优化过程中，系统会为每个 RelSubset 计算并缓存最优成本
     * 等价性保证：由于 RelSubset 中的所有表达式都是等价的，选择成本最低的那个就是最优解
     * 避免重复计算：通过缓存机制避免重复计算相同子问题的成本
     * @param rel 要计算成本的关系节点
     * @param mq 关系元数据查询接口，用于获取统计信息
     * @return 节点的总成本，如果无法计算则返回 null
     * @throws IllegalArgumentException 如果 rel 参数为 null
     */
    @Override
    public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        // 参数验证：确保传入的关系节点不为空，这是方法健壮性的基本要求
        Preconditions.checkArgument(rel != null, "rel is null");

        // RelSubset 特殊处理：如果是等价关系表达式集合，直接返回预计算的最优成本
        // RelSubset 维护了一组等价的关系表达式，bestCost 字段存储了其中成本最低的执行计划成本
        if (rel instanceof RelSubset) {
            return Utils.getFieldValue(RelSubset.class, rel, "bestCost");
        }

        // Convention 检查：获取是否启用了 "无约定具有无限成本" 的标志
        // 这个机制用于防止选择未实现的执行路径
        boolean noneConventionHasInfiniteCost =
                Utils.getFieldValue(VolcanoPlanner.class, this, "noneConventionHasInfiniteCost");

        // 如果启用了该标志且节点的约定为 NONE，则返回无限成本
        // Convention.NONE 表示该节点没有具体的执行约定，通常意味着无法执行
        if (noneConventionHasInfiniteCost
                && rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE) {
            return costFactory.makeInfiniteCost();
        }

        // 初始化累积成本：从零成本开始计算
        RelOptCost cost = this.costFactory.makeZeroCost();

        // 递归计算所有输入节点的成本：自底向上的成本计算策略
        // 遍历当前节点的所有输入节点，递归计算它们的成本并累加
        for (RelNode input : rel.getInputs()) {
            RelOptCost inputCost = getCost(input, mq);

            // 短路优化：如果任何输入成本为空或无限，直接返回该成本
            // 这避免了不必要的计算，因为整个计划的成本已经确定为无限或无效
            if (inputCost == null || inputCost.isInfinite()) {
                return inputCost;
            }

            // 累加输入成本到总成本中
            cost = cost.plus(inputCost);
        }

        // 获取节点自身的非累积成本：即节点本身的执行成本（不包括子节点）
        // 通过元数据查询获取，这包括了节点特定的操作成本
        RelOptCost relCost = mq.getNonCumulativeCost(rel);

        // 如果无法获取节点成本，返回 null 表示成本计算失败
        if (relCost == null) return null;

        // 返回总成本：输入成本 + 节点自身成本
        return cost.plus(relCost);
    }
    /**
     * 注册关系模式。
     *
     * <p>当前实现为空，不执行任何操作。GraphScope 使用自己的模式管理机制。
     *
     * @param schema 要注册的关系模式
     */
    @Override
    public void registerSchema(RelOptSchema schema) {
        // 空实现：GraphScope 不使用 Calcite 的标准模式注册机制
        // 原因：
        // 1. Apache Calcite 接口要求：作为 VolcanoPlanner 的子类，必须实现此方法
        // 2. GraphScope 自有机制：使用自己的图模式管理系统，不依赖 Calcite 的模式注册
        // 3. 简化设计：避免与 GraphScope 现有的图模式管理产生冲突
        // 4. 保持兼容性：保留接口兼容性但使用自己的实现逻辑
        // do nothing
    }
}
