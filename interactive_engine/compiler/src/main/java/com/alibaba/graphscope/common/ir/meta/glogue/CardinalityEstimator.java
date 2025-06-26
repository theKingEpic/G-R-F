package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * 基数估计器接口，定义了所有基数估计算法的统一接口
 */
public interface CardinalityEstimator {

    /**
     * 估计图模式的基数
     * @param pattern 图模式
     * @return 估计的基数值
     */
    @Nullable Double estimate(Pattern pattern);

    /**
     * 估计顶点的基数
     * @param vertex 图顶点
     * @return 估计的基数值
     */
    double estimate(PatternVertex vertex);

    /**
     * 估计边的基数
     * @param edge 图边
     * @return 估计的基数值
     */
    double estimate(PatternEdge edge);

    /**
     * 获取估计器名称
     * @return 估计器名称
     */
    String getName();
}