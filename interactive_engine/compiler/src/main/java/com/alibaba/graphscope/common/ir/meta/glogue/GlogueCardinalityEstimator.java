package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GlogueCardinalityEstimator implements CardinalityEstimator {
    private final PrimitiveCountEstimator primitiveEstimator;

    public GlogueCardinalityEstimator(GlogueQuery glogueQuery) {
        this.primitiveEstimator = new PrimitiveCountEstimator(glogueQuery);
    }

    @Override
    public @Nullable Double estimate(Pattern pattern) {
        return primitiveEstimator.estimate(pattern);
    }

    @Override
    public double estimate(PatternVertex vertex) {
        return primitiveEstimator.estimate(vertex);
    }

    @Override
    public double estimate(PatternEdge edge) {
        return primitiveEstimator.estimate(edge);
    }

    @Override
    public String getName() {
        return "GLOGUE";
    }
}