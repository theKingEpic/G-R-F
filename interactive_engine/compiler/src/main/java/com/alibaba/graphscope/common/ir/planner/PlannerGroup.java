/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKeyMeta;
import com.alibaba.graphscope.common.ir.planner.rules.*;
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
/**
 * GraphScope 查询规划器组，管理和协调多个查询优化规划器的执行。
 *
 * <p>该类是 GraphScope 查询优化框架的核心组件，负责管理三个不同阶段的规划器：
 * <ul>
 *   <li><strong>关系规划器</strong> - 处理基本的关系代数优化</li>
 *   <li><strong>匹配规划器</strong> - 专门优化图模式匹配操作</li>
 *   <li><strong>物理规划器</strong> - 生成最终的物理执行计划</li>
 * </ul>
 *
 * <p>优化流程按以下顺序执行：
 * <ol>
 *   <li><strong>关系优化阶段</strong>：应用过滤下推等基础优化规则</li>
 *   <li><strong>连接扁平化</strong>：将复杂连接转换为图扩展操作（可选）</li>
 *   <li><strong>模式匹配优化</strong>：使用 CBO 优化图模式匹配（可选）</li>
 *   <li><strong>字段修剪</strong>：移除不必要的字段以提升性能（可选）</li>
 *   <li><strong>物理优化阶段</strong>：应用物理层面的优化规则</li>
 * </ol>
 *
 * <p>支持的优化规则包括：
 * <ul>
 *   <li>{@code FilterIntoJoinRule} - 过滤条件下推到连接操作</li>
 *   <li>{@code FilterMatchRule} - 模式匹配中的过滤优化</li>
 *   <li>{@code ExtendIntersectRule} - 扩展交集操作优化</li>
 *   <li>{@code JoinDecompositionRule} - 连接分解优化</li>
 *   <li>{@code ScanExpandFusionRule} - 扫描扩展融合优化</li>
 *   <li>{@code ExpandGetVFusionRule} - 扩展顶点获取融合优化</li>
 *   <li>{@code TopKPushDownRule} - TopK 操作下推优化</li>
 *   <li>{@code ScanEarlyStopRule} - 扫描早停优化</li>
 * </ul>
 *
 * <p>该类支持两种优化模式：
 * <ul>
 *   <li><strong>RBO (Rule-Based Optimization)</strong> - 基于规则的优化</li>
 *   <li><strong>CBO (Cost-Based Optimization)</strong> - 基于成本的优化</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * PlannerConfig config = new PlannerConfig(configs);
 * RelBuilderFactory factory = new GraphBuilderFactory(configs);
 * PlannerGroup plannerGroup = new PlannerGroup(config, factory);
 *
 * // 优化查询计划
 * GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, irMeta);
 * RelNode optimizedPlan = plannerGroup.optimize(logicalPlan, ioProcessor);
 *
 * // 获取匹配规划器
 * RelOptPlanner matchPlanner = plannerGroup.getMatchPlanner();
 * }</pre>
 *
 * <p>线程安全性：该类的 {@code optimize} 和 {@code clear} 方法是同步的，
 * 可以在多线程环境中安全使用。
 *
 * @author GraphScope Team
 * @since 1.0
 * @see PlannerConfig
 * @see GraphIOProcessor
 * @see GraphHepPlanner
 * @see VolcanoPlannerX
 */
public class PlannerGroup {
    /** 关系优化规划器，处理基本的关系代数优化 */
    private final RelOptPlanner relPlanner;

    /** 匹配优化规划器，专门处理图模式匹配优化 */
    private final RelOptPlanner matchPlanner;

    /** 物理优化规划器，生成最终的物理执行计划 */
    private final RelOptPlanner physicalPlanner;

    /** 规划器配置信息 */
    private final PlannerConfig config;

    /** 关系构建器工厂 */
    private final RelBuilderFactory relBuilderFactory;

    /**
     * 构造一个新的 PlannerGroup 实例。
     *
     * <p>根据配置信息创建三个不同阶段的规划器实例，每个规划器负责特定的优化任务。
     *
     * @param config 规划器配置信息，包含优化规则和参数设置
     * @param relBuilderFactory 关系构建器工厂，用于创建关系节点
     */
    public PlannerGroup(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
        this.config = config;
        this.relBuilderFactory = relBuilderFactory;
        this.relPlanner = createRelPlanner();
        this.matchPlanner = createMatchPlanner();
        this.physicalPlanner = createPhysicalPlanner();
    }
    /**
     * 优化给定的关系节点，执行完整的查询优化流程。
     *
     * <p>该方法是规划器组的主要入口点，按顺序执行以下优化步骤：
     * <ol>
     *   <li>应用关系优化规则（如过滤下推）</li>
     *   <li>执行连接扁平化转换（如果配置启用）</li>
     *   <li>应用模式匹配优化（如果启用 CBO）</li>
     *   <li>执行字段修剪优化（如果配置启用）</li>
     *   <li>应用物理优化规则</li>
     *   <li>清理规划器状态</li>
     * </ol>
     *
     * @param before 待优化的关系节点
     * @param ioProcessor 图输入输出处理器，用于模式匹配转换
     * @return 优化后的关系节点，如果优化未启用则返回原始节点
     */
    public synchronized RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        if (config.isOn()) {
            // 第一阶段：逻辑优化 - 应用过滤器下推等规则
            // apply rules of 'FilterPushDown' before the match optimization
            relPlanner.setRoot(before);
            RelNode relOptimized = relPlanner.findBestExp();
            // 应用平坦连接转换为图扩展的规则
            if (config.getRules().contains(FlatJoinToExpandRule.class.getSimpleName())) {
                relOptimized = relOptimized.accept(new FlatJoinToExpandRule(config));
            }
            // 如果启用了CBO模式，应用图匹配优化器
            if (config.getOpt() == PlannerConfig.Opt.CBO) {
                relOptimized =
                        relOptimized.accept(
                                new GraphRelOptimizer.MatchOptimizer(ioProcessor, matchPlanner));
            }
            // apply rules of 'FieldTrim' after the match optimization
            // 应用字段裁剪规则，移除不必要的字段
            if (config.getRules().contains(FieldTrimRule.class.getSimpleName())) {
                relOptimized = FieldTrimRule.trim(ioProcessor.getBuilder(), relOptimized, config);
            }

            // Yuanmei Zhao
            // 在这里添加基数估计打印 - 逻辑优化完成，物理优化开始前
            if (config.getOpt() == PlannerConfig.Opt.CBO) {
                Logger cardinalityLogger = LoggerFactory.getLogger("FinalCardinalityEstimation");
                try {
                    RelMetadataQuery mq = relOptimized.getCluster().getMetadataQuery();
                    Double cardinality = mq.getRowCount(relOptimized);
                    // 获取模式信息
                    //String schema = relOptimized.explain();
                    //cardinalityLogger.info("基数估计值: " + cardinality + ", 对应模式: " + schema);
                    cardinalityLogger.info("基数估计值: " + cardinality );
                } catch (Exception e) {
                    cardinalityLogger.info("基数估计失败: " + e.getMessage());
                }
            }

            // 第二阶段：物理优化 - 生成最终执行计划
            physicalPlanner.setRoot(relOptimized);
            RelNode physicalOptimized = physicalPlanner.findBestExp();

            // 清理规划器状态并返回优化后的执行计划
            clear();
            return physicalOptimized;
        }
        return before;
    }

    /**
     * 创建关系优化规划器。
     *
     * <p>该规划器使用 HEP (Heuristic) 算法，应用以下优化规则：
     * <ul>
     *   <li>{@code FilterIntoJoinRule} - 过滤条件下推到连接操作</li>
     *   <li>{@code FilterMatchRule} - 模式匹配中的过滤优化</li>
     * </ul>
     *
     * @return 配置好的关系优化规划器
     */
    private RelOptPlanner createRelPlanner() {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (config.isOn()) {
            List<RelRule.Config> ruleConfigs = Lists.newArrayList();
            config.getRules()
                    .forEach(
                            k -> {
                                if (k.equals(
                                        FilterJoinRule.FilterIntoJoinRule.class.getSimpleName())) {
                                    ruleConfigs.add(CoreRules.FILTER_INTO_JOIN.config);
                                } else if (k.equals(FilterMatchRule.class.getSimpleName())) {
                                    ruleConfigs.add(FilterMatchRule.Config.DEFAULT);
                                }
                            });
            ruleConfigs.forEach(
                    k -> hepBuilder.addRuleInstance(
                            k.withRelBuilderFactory(relBuilderFactory).toRule()));
        }
        return new GraphHepPlanner(hepBuilder.build());
    }
    /**
     * 创建匹配优化规划器。
     *
     * <p>根据配置的优化模式创建不同类型的规划器：
     * <ul>
     *   <li><strong>CBO 模式</strong>：使用 {@code VolcanoPlannerX} 进行基于成本的优化</li>
     *   <li><strong>RBO 模式</strong>：使用 {@code GraphHepPlanner} 进行基于规则的优化</li>
     * </ul>
     *
     * <p>CBO 模式支持的优化规则：
     * <ul>
     *   <li>{@code ExtendIntersectRule} - 扩展交集操作优化</li>
     *   <li>{@code JoinDecompositionRule} - 连接分解优化</li>
     * </ul>
     *
     * @return 配置好的匹配优化规划器
     */
    private RelOptPlanner createMatchPlanner() {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            VolcanoPlanner planner = new VolcanoPlannerX();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            planner.setTopDownOpt(true);
            planner.setNoneConventionHasInfiniteCost(false);
            config.getRules()
                    .forEach(
                            k -> {
                                RelRule.Config ruleConfig = null;
                                if (k.equals(ExtendIntersectRule.class.getSimpleName())) {
                                    ruleConfig =
                                            ExtendIntersectRule.Config.DEFAULT
                                                    .withMaxPatternSizeInGlogue(
                                                            config.getGlogueSize())
                                                    .withLabelConstraintsEnabled(
                                                            config.labelConstraintsEnabled());
                                } else if (k.equals(JoinDecompositionRule.class.getSimpleName())) {
                                    ruleConfig =
                                            JoinDecompositionRule.Config.DEFAULT
                                                    .withMinPatternSize(
                                                            config.getJoinMinPatternSize())
                                                    .withJoinQueueCapacity(
                                                            config.getJoinQueueCapacity())
                                                    .withJoinByEdgeEnabled(
                                                            config.isJoinByEdgeEnabled());
                                    ForeignKeyMeta foreignKeyMeta =
                                            config.getJoinByForeignKeyUri().isEmpty()
                                                    ? null
                                                    : new ForeignKeyMeta(
                                                            config.getJoinByForeignKeyUri());
                                    ((JoinDecompositionRule.Config) ruleConfig)
                                            .withForeignKeyMeta(foreignKeyMeta);
                                }
                                if (ruleConfig != null) {
                                    planner.addRule(
                                            ruleConfig
                                                    .withRelBuilderFactory(relBuilderFactory)
                                                    .toRule());
                                }
                            });
            return planner;
        }
        // todo: re-implement heuristic rules in ir core match
        return new GraphHepPlanner(HepProgram.builder().build());
    }
    /**
     * 创建物理优化规划器。
     *
     * <p>该规划器使用 HEP 算法，应用以下物理层面的优化规则：
     * <ul>
     *   <li>{@code ScanExpandFusionRule} - 扫描扩展融合优化</li>
     *   <li>{@code ExpandGetVFusionRule} - 扩展顶点获取融合优化</li>
     *   <li>{@code TopKPushDownRule} - TopK 操作下推优化</li>
     *   <li>{@code ScanEarlyStopRule} - 扫描早停优化</li>
     * </ul>
     *
     * @return 配置好的物理优化规划器
     */
    private RelOptPlanner createPhysicalPlanner() {
        HepProgramBuilder hepBuilder = HepProgram.builder();
        if (config.isOn()) {
            List<RelRule.Config> ruleConfigs = Lists.newArrayList();
            config.getRules()
                    .forEach(
                            k -> {
                                if (k.equals(ScanExpandFusionRule.class.getSimpleName())) {
                                    ruleConfigs.add(ScanExpandFusionRule.Config.DEFAULT);
                                } else if (k.equals(ExpandGetVFusionRule.class.getSimpleName())) {
                                    ruleConfigs.add(
                                            ExpandGetVFusionRule.BasicExpandGetVFusionRule.Config
                                                    .DEFAULT);
                                    ruleConfigs.add(
                                            ExpandGetVFusionRule.PathBaseExpandGetVFusionRule.Config
                                                    .DEFAULT);
                                } else if (k.equals(TopKPushDownRule.class.getSimpleName())) {
                                    ruleConfigs.add(TopKPushDownRule.Config.DEFAULT);
                                } else if (k.equals(ScanEarlyStopRule.class.getSimpleName())) {
                                    ruleConfigs.add(ScanEarlyStopRule.Config.DEFAULT);
                                }
                            });
            ruleConfigs.forEach(
                    k -> hepBuilder.addRuleInstance(
                            k.withRelBuilderFactory(relBuilderFactory).toRule()));
        }
        return new GraphHepPlanner(hepBuilder.build());
    }

    public synchronized void clear() {
        List<RelOptRule> logicalRBORules = this.relPlanner.getRules();
        this.relPlanner.clear();
        for (RelOptRule rule : logicalRBORules) {
            this.relPlanner.addRule(rule);
        }
        List<RelOptRule> logicalCBORules = this.matchPlanner.getRules();
        this.matchPlanner.clear();
        for (RelOptRule rule : logicalCBORules) {
            this.matchPlanner.addRule(rule);
        }
        List<RelOptRule> physicalRBORules = this.physicalPlanner.getRules();
        physicalPlanner.clear();
        for (RelOptRule rule : physicalRBORules) {
            this.physicalPlanner.addRule(rule);
        }
    }

    public RelOptPlanner getMatchPlanner() {
        return this.matchPlanner;
    }
}
