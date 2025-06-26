package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.util.Random;

public class SamplingCardinalityEstimator implements CardinalityEstimator {
    private final Random random;
    private final double baseCardinality;
    private final double variance;

    public SamplingCardinalityEstimator(double baseCardinality, double variance) {
        this.random = new Random();
        this.baseCardinality = baseCardinality;
        this.variance = variance;
    }

    @Override
    public @Nullable Double estimate(Pattern pattern) {
        // 基于模式复杂度的采样估计
        int complexity = pattern.getVertexNumber() + pattern.getEdgeNumber();
        double factor = 1.0 + (complexity - 1) * 0.1; // 复杂度因子
        return baseCardinality * factor * (1.0 + (random.nextGaussian() * variance));
    }

    @Override
    public double estimate(PatternVertex vertex) {
        return baseCardinality * (1.0 + (random.nextGaussian() * variance));
    }

    @Override
    public double estimate(PatternEdge edge) {
        return baseCardinality * 0.8 * (1.0 + (random.nextGaussian() * variance));
    }

    @Override
    public String getName() {
        return "SAMPLING";
    }
}
