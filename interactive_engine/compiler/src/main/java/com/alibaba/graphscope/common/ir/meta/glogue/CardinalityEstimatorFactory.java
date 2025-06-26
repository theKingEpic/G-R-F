package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CardinalityEstimatorFactory {
    private static Logger logger = LoggerFactory.getLogger(Glogue.class);
    public enum EstimatorType {
        GLOGUE,
        FIXED,
        SAMPLING,
        MACHINE_LEARNING // 预留给未来的ML算法
    }

    /**
     * 根据配置创建相应的基数估计器
     */
    public static CardinalityEstimator createEstimator(
            EstimatorType type,
            GlogueQuery glogueQuery,
            PlannerConfig config) {

        logger.info("Creating cardinality estimator: {}", type);
        switch (type) {
            case GLOGUE:
                return new GlogueCardinalityEstimator(glogueQuery);

            case FIXED:
                double fixedValue = config.getFixedCardinalityValue(); // 需要在PlannerConfig中添加
                return new FixedCardinalityEstimator(fixedValue);

            case SAMPLING:
                double baseCardinality = config.getSamplingBaseCardinality();
                double variance = config.getSamplingVariance();
                return new SamplingCardinalityEstimator(baseCardinality, variance);

            case MACHINE_LEARNING:
                // 预留给未来的ML算法实现
                throw new UnsupportedOperationException("ML estimator not implemented yet");

            default:
                return new GlogueCardinalityEstimator(glogueQuery);
        }
    }

    /**
     * 从字符串配置创建估计器
     */
    public static CardinalityEstimator createEstimator(
            String estimatorName,
            GlogueQuery glogueQuery,
            PlannerConfig config) {

        try {
            EstimatorType type = EstimatorType.valueOf(estimatorName.toUpperCase());
            return createEstimator(type, glogueQuery, config);
        } catch (IllegalArgumentException e) {
            // 默认使用Glogue估计器
            return new GlogueCardinalityEstimator(glogueQuery);
        }
    }
}