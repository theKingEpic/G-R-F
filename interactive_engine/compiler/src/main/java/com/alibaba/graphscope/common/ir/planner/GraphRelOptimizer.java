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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler.GraphMetadataHandlerProvider;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilderFactory;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * GraphScope 图关系查询优化器，负责优化包含匹配操作和其他关系操作的图查询计划树。
 *
 * <p>该优化器是 GraphScope 查询处理管道的核心组件，提供了完整的查询优化功能：
 * <ul>
 *   <li>基于规则的优化 (RBO) - 应用预定义的优化规则</li>
 *   <li>基于成本的优化 (CBO) - 使用统计信息进行成本估算</li>
 *   <li>模式匹配优化 - 专门针对图模式匹配的优化</li>
 *   <li>字段修剪优化 - 移除不必要的字段以提升性能</li>
 * </ul>
 *
 * <p>优化器支持的主要优化规则包括：
 * <ul>
 *   <li>{@code FilterIntoJoinRule} - 将过滤条件下推到连接操作</li>
 *   <li>{@code FilterMatchRule} - 优化模式匹配中的过滤条件</li>
 *   <li>{@code ExtendIntersectRule} - 优化扩展和交集操作</li>
 *   <li>{@code ExpandGetVFusionRule} - 融合扩展和顶点获取操作</li>
 *   <li>{@code JoinDecompositionRule} - 分解复杂的连接操作</li>
 *   <li>{@code NotMatchToAntiJoinRule} - 将否定匹配转换为反连接</li>
 * </ul>
 *
 * <p>该优化器实现了 {@link IrMetaTracker} 接口，能够响应图模式和统计信息的变化：
 * <ul>
 *   <li>当图模式发生变化时，清理规划器缓存</li>
 *   <li>当统计信息更新时，重建 Glogue 查询结构用于 CBO</li>
 * </ul>
 *
 * <p>优化过程分为以下阶段：
 * <ol>
 *   <li><strong>关系优化</strong>：应用通用的关系代数优化规则</li>
 *   <li><strong>模式匹配优化</strong>：使用 {@link MatchOptimizer} 优化图模式匹配</li>
 *   <li><strong>字段修剪</strong>：移除查询中不需要的字段</li>
 *   <li><strong>物理优化</strong>：生成最终的物理执行计划</li>
 * </ol>
 *
 * <p>使用示例：
 * <pre>{@code
 * Configs configs = new Configs(configMap);
 * GraphRelOptimizer optimizer = new GraphRelOptimizer(configs);
 *
 * // 设置元数据跟踪
 * IrMeta irMeta = ...;
 * optimizer.onStatsChanged(irMeta.getStats());
 *
 * // 优化查询计划
 * RelNode logicalPlan = ...;
 * GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, irMeta);
 * RelNode optimizedPlan = optimizer.optimize(logicalPlan, ioProcessor);
 * }</pre>
 *
 * @author GraphScope Team
 * @since 1.0
 * @see GraphIOProcessor
 * @see PlannerGroup
 * @see IrMetaTracker
 * @see Closeable
 */
public class GraphRelOptimizer implements Closeable, IrMetaTracker {
    /** 日志记录器 */
    private static final Logger logger = LoggerFactory.getLogger(GraphRelOptimizer.class);

    /** 规划器配置信息 */
    private final PlannerConfig config;

    public PlannerConfig getConfig() {
        return this.config;
    }
    
    /** 关系构建器工厂 */
    private final RelBuilderFactory relBuilderFactory;

    /** 规划器组管理器，负责管理不同类型的规划器 */
    private final PlannerGroupManager plannerGroupManager;

    /** Glogue 查询的原子引用，用于 CBO 优化 */
    private final AtomicReference<GlogueQuery> glogueRef;

    /**
     * 使用指定的配置和规划器组管理器类构造优化器。
     *
     * @param graphConfig 图配置信息
     * @param instance 规划器组管理器的实现类
     * @throws RuntimeException 如果无法实例化规划器组管理器
     */
    public GraphRelOptimizer(Configs graphConfig, Class<? extends PlannerGroupManager> instance) {
        try {
            this.config = new PlannerConfig(graphConfig);
            this.relBuilderFactory = new GraphBuilderFactory(graphConfig);
            this.plannerGroupManager =
                    instance.getDeclaredConstructor(PlannerConfig.class, RelBuilderFactory.class)
                            .newInstance(this.config, this.relBuilderFactory);
            this.glogueRef = new AtomicReference<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 使用默认的动态规划器组管理器构造优化器。
     *
     * @param graphConfig 图配置信息
     */
    public GraphRelOptimizer(Configs graphConfig) {
        this(graphConfig, PlannerGroupManager.Dynamic.class);
    }
    /**
     * 获取当前的匹配规划器实例。
     *
     * <p>匹配规划器专门用于优化图模式匹配操作，是 CBO 优化的核心组件。
     *
     * @return 匹配规划器实例
     */
    public RelOptPlanner getMatchPlanner() {
        PlannerGroup currentGroup = this.plannerGroupManager.getCurrentGroup();
        return currentGroup.getMatchPlanner();
    }
    /**
     * 优化给定的关系节点。
     *
     * <p>该方法是优化器的主要入口点，执行完整的优化流程：
     * <ol>
     *   <li>应用关系优化规则</li>
     *   <li>执行模式匹配优化（如果启用 CBO）</li>
     *   <li>应用字段修剪优化</li>
     *   <li>生成物理执行计划</li>
     * </ol>
     *
     * @param before 待优化的关系节点
     * @param ioProcessor 图输入输出处理器，用于处理模式匹配转换
     * @return 优化后的关系节点
     */
    public RelNode optimize(RelNode before, GraphIOProcessor ioProcessor) {
        PlannerGroup currentGroup = this.plannerGroupManager.getCurrentGroup();
        return currentGroup.optimize(before, ioProcessor);
    }
    /**
     * 为指定的 IR 元数据创建关系元数据查询实例。
     *
     * <p>仅在启用 CBO 优化时返回有效的元数据查询实例，用于成本估算。
     *
     * @param irMeta IR 元数据
     * @return 关系元数据查询实例，如果未启用 CBO 则返回 null
     * @throws IllegalArgumentException 如果启用了 CBO 但 Glogue 未就绪
     */
    public @Nullable RelMetadataQuery createMetaDataQuery(IrMeta irMeta) {
        if (config.isOn() && config.getOpt() == PlannerConfig.Opt.CBO) {
            GlogueQuery gq = this.glogueRef.get();
            Preconditions.checkArgument(gq != null, "glogue is not ready");
            return new GraphRelMetadataQuery(
                    new GraphMetadataHandlerProvider(getMatchPlanner(), gq, this.config));
        }
        return null;
    }
    /**
     * 关闭优化器并释放相关资源。
     *
     * <p>该方法会关闭规划器组管理器，清理所有缓存的规划器实例。
     */
    @Override
    public void close() {
        if (this.plannerGroupManager != null) {
            this.plannerGroupManager.close();
        }
    }
    /**
     * 响应图模式变化事件。
     *
     * <p>当图模式发生变化时，清理规划器缓存以确保使用最新的模式信息。
     *
     * @param meta 新的 IR 元数据
     */
    @Override
    public void onSchemaChanged(IrMeta meta) {
        if (this.plannerGroupManager != null) {
            this.plannerGroupManager.clean();
        }
    }
    /**
     * 响应统计信息变化事件。
     *
     * <p>当统计信息更新时，重建 Glogue 查询结构以支持基于成本的优化。
     *
     * @param stats 新的 IR 元数据统计信息
     */
    @Override
    public void onStatsChanged(IrMetaStats stats) {
        GlogueSchema g = GlogueSchema.fromMeta(stats);
        Glogue gl = new Glogue(g, config.getGlogueSize());
        GlogueQuery gq = new GlogueQuery(gl);
        this.glogueRef.compareAndSet(glogueRef.get(), gq);
    }
    /**
     * 匹配优化器，专门负责优化图模式匹配操作。
     *
     * <p>该内部类继承自 {@code GraphShuttle}，使用访问者模式遍历查询计划树，
     * 对图模式匹配节点进行专门的优化处理。主要功能包括：
     * <ul>
     *   <li>处理 {@code CommonTableScan} 节点的公共表优化</li>
     *   <li>优化 {@code GraphLogicalSingleMatch} 单模式匹配</li>
     *   <li>优化 {@code GraphLogicalMultiMatch} 多模式匹配</li>
     *   <li>缓存已优化的公共关系节点以避免重复处理</li>
     * </ul>
     *
     * <p>优化过程：
     * <ol>
     *   <li>使用 {@code GraphIOProcessor} 将匹配节点转换为模式结构</li>
     *   <li>应用匹配规划器进行优化</li>
     *   <li>将优化结果转换回关系节点</li>
     * </ol>
     */
    public static class MatchOptimizer extends GraphShuttle {
        /** 图输入输出处理器 */
        private final GraphIOProcessor ioProcessor;

        /** 匹配规划器 */
        private final RelOptPlanner matchPlanner;

        /** 已优化的公共表映射缓存 */
        private final Map<String, RelNode> commonTableToOpt;

        /**
         * 构造匹配优化器。
         *
         * @param ioProcessor 图输入输出处理器
         * @param matchPlanner 匹配规划器
         */
        public MatchOptimizer(GraphIOProcessor ioProcessor, RelOptPlanner matchPlanner) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.commonTableToOpt = Maps.newHashMap();
        }
        /**
         * 访问公共表扫描节点。
         *
         * <p>对公共表进行优化并缓存结果，避免重复优化相同的公共表。
         *
         * @param tableScan 公共表扫描节点
         * @return 优化后的公共表扫描节点
         */
        @Override
        public RelNode visit(CommonTableScan tableScan) {
            CommonOptTable optTable = (CommonOptTable) tableScan.getTable();
            String tableName = optTable.getQualifiedName().get(0);
            RelNode commonOpt = commonTableToOpt.get(tableName);
            if (commonOpt == null) {
                commonOpt = optTable.getCommon().accept(this);
                commonTableToOpt.put(tableName, commonOpt);
            }
            return new CommonTableScan(
                    tableScan.getCluster(), tableScan.getTraitSet(), new CommonOptTable(commonOpt));
        }
        /**
         * 访问单模式匹配节点。
         *
         * <p>将单模式匹配转换为模式结构，应用匹配规划器优化，然后转换回关系节点。
         *
         * @param match 单模式匹配节点
         * @return 优化后的关系节点
         */
        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(matchPlanner.findBestExp());
        }
        /**
         * 访问多模式匹配节点。
         *
         * <p>将多模式匹配转换为模式结构，应用匹配规划器优化，然后转换回关系节点。
         *
         * @param match 多模式匹配节点
         * @return 优化后的关系节点
         */
        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            return ioProcessor.processOutput(matchPlanner.findBestExp());
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            List<RelNode> matchList = Lists.newArrayList();
            List<RelNode> filterList = Lists.newArrayList();
            if (!decomposeJoin(join, matchList, filterList)) {
                return super.visit(join);
            } else {
                matchPlanner.setRoot(ioProcessor.processInput(matchList));
                RelNode match = ioProcessor.processOutput(matchPlanner.findBestExp());
                for (RelNode filter : filterList) {
                    match = filter.copy(filter.getTraitSet(), ImmutableList.of(match));
                }
                return match;
            }
        }

        private boolean decomposeJoin(
                LogicalJoin join, List<RelNode> matchList, List<RelNode> filterList) {
            AtomicBoolean decomposable = new AtomicBoolean(true);
            RelVisitor visitor =
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            if (node instanceof LogicalJoin) {
                                JoinRelType joinType = ((LogicalJoin) node).getJoinType();
                                if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
                                    decomposable.set(false);
                                    return;
                                }
                                visit(((LogicalJoin) node).getLeft(), 0, node);
                                if (!decomposable.get()) {
                                    return;
                                }
                                int leftMatchSize = matchList.size();
                                visit(((LogicalJoin) node).getRight(), 1, node);
                                if (!decomposable.get()) {
                                    return;
                                }
                                if (joinType == JoinRelType.LEFT) {
                                    for (int i = leftMatchSize; i < matchList.size(); i++) {
                                        if (matchList.get(i) instanceof GraphLogicalSingleMatch) {
                                            GraphLogicalSingleMatch singleMatch =
                                                    (GraphLogicalSingleMatch) matchList.get(i);
                                            matchList.set(
                                                    i,
                                                    GraphLogicalSingleMatch.create(
                                                            (GraphOptCluster)
                                                                    singleMatch.getCluster(),
                                                            ImmutableList.of(),
                                                            singleMatch.getInput(),
                                                            singleMatch.getSentence(),
                                                            GraphOpt.Match.OPTIONAL));
                                        }
                                    }
                                }
                            } else if (node instanceof AbstractLogicalMatch) {
                                matchList.add(node);
                            } else if (node instanceof GraphLogicalSource) {
                                matchList.add(
                                        GraphLogicalSingleMatch.create(
                                                (GraphOptCluster) node.getCluster(),
                                                ImmutableList.of(),
                                                null,
                                                node,
                                                GraphOpt.Match.INNER));
                            } else if (node instanceof Filter) {
                                filterList.add(node);
                                visit(node.getInput(0), 0, node);
                            } else {
                                decomposable.set(false);
                            }
                        }
                    };
            visitor.go(join);
            return decomposable.get();
        }
    }
}
