package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

public class FixedCardinalityEstimator implements CardinalityEstimator {
    private static final Logger logger = LoggerFactory.getLogger(FixedCardinalityEstimator.class);
    private final double fixedValue;

    public FixedCardinalityEstimator(double fixedValue) {
        this.fixedValue = fixedValue;
    }

    public FixedCardinalityEstimator() {
        this(99.0); // 默认值
    }

    @Override
    public @Nullable Double estimate(Pattern pattern) {
        logEstimation("Pattern", pattern.toString(), fixedValue);
        return fixedValue;
    }

    @Override
    public double estimate(PatternVertex vertex) {
        logEstimation("PatternVertex", vertex.toString(), fixedValue);
        return fixedValue;
    }

    @Override
    public double estimate(PatternEdge edge) {
        logEstimation("PatternEdge", edge.toString(), fixedValue);
        return fixedValue;
    }

    @Override
    public String getName() {
        return "FIXED";
    }

    private void logEstimation(String type, String element, double cardinality) {
        String logMessage = String.format("[%s] %s: %s, Estimated Cardinality: %.1f",
                LocalDateTime.now(), type, element, cardinality);

        logger.info(logMessage);

        try (FileWriter writer = new FileWriter("cardinality_estimates.log", true)) {
            writer.write(logMessage + "\\n");
        } catch (IOException e) {
            logger.warn("Failed to write cardinality estimate to file", e);
        }
    }
}