package com.alibaba.graphscope.common.config;

import java.util.Collections;
import java.util.List;

public class PlannerConfig {
    public static final Config<Boolean> GRAPH_PLANNER_IS_ON =
            Config.boolConfig("graph.planner.is.on", false);
    public static final Config<String> GRAPH_PLANNER_OPT =
            Config.stringConfig("graph.planner.opt", "RBO");
    public static final Config<String> GRAPH_PLANNER_RULES =
            Config.stringConfig(
                    "graph.planner.rules",
                    "FilterIntoJoinRule,FilterMatchRule,ExtendIntersectRule,ExpandGetVFusionRule");
    public static final Config<Integer> GRAPH_PLANNER_CBO_GLOGUE_SIZE =
            Config.intConfig("graph.planner.cbo.glogue.size", 2);
    public static final Config<Integer> JOIN_MIN_PATTERN_SIZE =
            Config.intConfig("graph.planner.join.min.pattern.size", 5);
    public static final Config<Integer> JOIN_COST_FACTOR_1 =
            Config.intConfig("graph.planner.join.cost.factor.1", 1);
    public static final Config<Integer> JOIN_COST_FACTOR_2 =
            Config.intConfig("graph.planner.join.cost.factor.2", 1);
    // if enabled, the cost estimation of expand will take the intermediate count of <src, edge>
    // into consideration
    public static final Config<Boolean> LABEL_CONSTRAINTS_ENABLED =
            Config.boolConfig("graph.planner.label.constraints.enabled", false);
    // the cost factor of ExtendIntersect operator
    public static final Config<Integer> INTERSECT_COST_FACTOR =
            Config.intConfig("graph.planner.intersect.cost.factor", 1);
    // control the output plan space after applying `JoinDecomposition` each time
    public static final Config<Integer> JOIN_QUEUE_CAPACITY =
            Config.intConfig("graph.planner.join.queue.capacity", 3);
    // if enabled, the triangle pattern will be converted to `JoinByEdge`, to support optimizations
    // in Neo4j
    public static final Config<Boolean> JOIN_BY_EDGE_ENABLED =
            Config.boolConfig("graph.planner.join.by.edge.enabled", false);
    public static final Config<Integer> GRAPH_PLANNER_GROUP_SIZE =
            Config.intConfig("graph.planner.group.size", 8);
    public static final Config<Integer> GRAPH_PLANNER_GROUP_CLEAR_INTERVAL_MINUTES =
            Config.intConfig("graph.planner.group.clear.interval.minutes", 30);
    public static final Config<String> TRIM_CLASS_NAMES =
            Config.stringConfig(
                    "graph.planner.trim.class.names",
                    "GraphLogicalExpand, GraphLogicalSource, GraphLogicalGetV");
    public static final Config<String> GRAPH_PLANNER_PATTERN_CARDINALITY_ESTIMATOR =
            Config.stringConfig("graph.planner.pattern.cardinality.estimator", "GLOGUE");

    public enum PatternCardinalityEstimator {
        GLOGUE,  // 原有的Glogue基数估计
        FIXED    // 新的固定值基数估计
    }

    public PatternCardinalityEstimator getPatternCardinalityEstimator() {
        return PatternCardinalityEstimator.valueOf(GRAPH_PLANNER_PATTERN_CARDINALITY_ESTIMATOR.get(configs));
    }

    /**
     * 基数估计器类型配置
     * 用于指定使用哪种基数估计算法
     * 可选值: "GLOGUE"(默认), "FIXED", "SAMPLING", "MACHINE_LEARNING"
     */
    public static final Config<String> GRAPH_PLANNER_CARDINALITY_ESTIMATOR =
            Config.stringConfig("graph.planner.cardinality.estimator", "GLOGUE");

    /**
     * 固定基数估计器的返回值配置
     * 当使用FIXED类型的基数估计器时，所有查询都返回此固定值
     * 默认值: 99.0，主要用于测试和实验
     */
    public static final Config<Double> GRAPH_PLANNER_FIXED_CARDINALITY_VALUE =
            Config.doubleConfig("graph.planner.fixed.cardinality.value", 99.0);

    /**
     * 采样基数估计器的基础基数值配置
     * 当使用SAMPLING类型的基数估计器时，作为估计的基准值
     * 默认值: 100.0，实际估计值会在此基础上根据模式复杂度调整
     */
    public static final Config<Double> GRAPH_PLANNER_SAMPLING_BASE_CARDINALITY =
            Config.doubleConfig("graph.planner.sampling.base.cardinality", 100.0);

    /**
     * 采样基数估计器的方差配置
     * 控制采样估计的随机性程度，值越大随机波动越大
     * 默认值: 0.1，用于模拟真实环境中基数估计的不确定性
     */
    public static final Config<Double> GRAPH_PLANNER_SAMPLING_VARIANCE =
            Config.doubleConfig("graph.planner.sampling.variance", 0.1);

    private final Configs configs;
    private final List<String> rules;
    private final List<String> trimClasses;

    public PlannerConfig(Configs configs) {
        this.configs = configs;
        this.rules = Utils.convertDotString(GRAPH_PLANNER_RULES.get(configs));
        this.trimClasses = Utils.convertDotString(TRIM_CLASS_NAMES.get(configs));
    }

    public enum Opt {
        RBO,
        CBO
    }

    public boolean isOn() {
        return GRAPH_PLANNER_IS_ON.get(configs);
    }

    public Opt getOpt() {
        return Opt.valueOf(GRAPH_PLANNER_OPT.get(configs));
    }

    public List<String> getRules() {
        return Collections.unmodifiableList(rules);
    }

    public int getGlogueSize() {
        return GRAPH_PLANNER_CBO_GLOGUE_SIZE.get(configs);
    }

    public int getJoinMinPatternSize() {
        return JOIN_MIN_PATTERN_SIZE.get(configs);
    }

    public int getJoinCostFactor1() {
        return JOIN_COST_FACTOR_1.get(configs);
    }

    public int getJoinCostFactor2() {
        return JOIN_COST_FACTOR_2.get(configs);
    }

    public List<String> getTrimClassNames() {
        return Collections.unmodifiableList(trimClasses);
    }

    public boolean labelConstraintsEnabled() {
        return LABEL_CONSTRAINTS_ENABLED.get(configs);
    }

    public int getIntersectCostFactor() {
        return INTERSECT_COST_FACTOR.get(configs);
    }

    public boolean isJoinByEdgeEnabled() {
        return JOIN_BY_EDGE_ENABLED.get(configs);
    }

    public int getJoinQueueCapacity() {
        return JOIN_QUEUE_CAPACITY.get(configs);
    }

    public String getJoinByForeignKeyUri() {
        return GraphConfig.GRAPH_FOREIGN_KEY_URI.get(configs);
    }

    public int getPlannerGroupSize() {
        return GRAPH_PLANNER_GROUP_SIZE.get(configs);
    }

    public int getPlannerGroupClearIntervalMinutes() {
        return GRAPH_PLANNER_GROUP_CLEAR_INTERVAL_MINUTES.get(configs);
    }
    // 添加getter方法
    public String getCardinalityEstimator() {
        return GRAPH_PLANNER_CARDINALITY_ESTIMATOR.get(configs);
    }

    public double getFixedCardinalityValue() {
        return GRAPH_PLANNER_FIXED_CARDINALITY_VALUE.get(configs);
    }

    public double getSamplingBaseCardinality() {
        return GRAPH_PLANNER_SAMPLING_BASE_CARDINALITY.get(configs);
    }

    public double getSamplingVariance() {
        return GRAPH_PLANNER_SAMPLING_VARIANCE.get(configs);
    }
    @Override
    public String toString() {
        return "PlannerConfig{"
                + "isOn="
                + isOn()
                + ", opt="
                + getOpt()
                + ", rules="
                + rules
                + ", glogueSize="
                + getGlogueSize()
                + '}';
    }
}
