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
import com.google.common.base.Preconditions;

import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
/**
 * GraphScope 规划器组管理器，负责管理和协调多个 {@link PlannerGroup} 实例的生命周期。
 *
 * <p>该抽象类提供了两种不同的实现策略来管理规划器组：
 * <ul>
 *   <li><strong>静态管理</strong> - 使用单一的规划器组实例</li>
 *   <li><strong>动态管理</strong> - 维护多个规划器组实例池，支持并发访问和自动清理</li>
 * </ul>
 *
 * <p>管理器的主要职责包括：
 * <ul>
 *   <li>创建和维护 {@code PlannerGroup} 实例</li>
 *   <li>提供线程安全的规划器组访问</li>
 *   <li>管理内存使用和资源清理</li>
 *   <li>响应配置变化和模式更新</li>
 * </ul>
 *
 * <p>该类实现了 {@link Closeable} 接口，确保资源能够正确释放。
 *
 * @author GraphScope Team
 * @since 1.0
 * @see PlannerGroup
 * @see PlannerConfig
 * @see Closeable
 */
public abstract class PlannerGroupManager implements Closeable {
    /** 规划器配置信息 */
    protected final PlannerConfig config;

    /** 关系构建器工厂 */
    protected final RelBuilderFactory relBuilderFactory;

    /**
     * 构造规划器组管理器。
     *
     * @param config 规划器配置信息
     * @param relBuilderFactory 关系构建器工厂
     */
    public PlannerGroupManager(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
        this.config = config;
        this.relBuilderFactory = relBuilderFactory;
    }
    /**
     * 关闭管理器并释放相关资源。
     *
     * <p>默认实现为空，子类可以重写此方法来实现特定的清理逻辑。
     */
    @Override
    public void close() {}
    /**
     * 清理规划器组的内部状态。
     *
     * <p>该方法通常在图模式发生变化时调用，用于清理缓存的规划器状态。
     * 默认实现为空，子类可以重写此方法来实现特定的清理逻辑。
     */
    public void clean() {}
    /**
     * 获取当前可用的规划器组实例。
     *
     * <p>该方法必须是线程安全的，因为可能被多个线程并发调用。
     *
     * @return 当前可用的规划器组实例
     */
    public abstract PlannerGroup getCurrentGroup();
    /**
     * 静态规划器组管理器实现。
     *
     * <p>该实现使用单一的 {@code PlannerGroup} 实例来处理所有查询优化请求。
     * 适用于单线程环境或对并发性能要求不高的场景。
     *
     * <p>特点：
     * <ul>
     *   <li>内存占用较小</li>
     *   <li>实现简单</li>
     *   <li>适合轻量级应用</li>
     * </ul>
     */
    public static class Static extends PlannerGroupManager {
        /** 单一的规划器组实例 */
        private final PlannerGroup singleGroup;

        /**
         * 构造静态规划器组管理器。
         *
         * @param config 规划器配置信息
         * @param relBuilderFactory 关系构建器工厂
         */
        public Static(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
            super(config, relBuilderFactory);
            this.singleGroup = new PlannerGroup(config, relBuilderFactory);
        }
        /**
         * 获取当前规划器组实例。
         *
         * @return 单一的规划器组实例
         */
        @Override
        public PlannerGroup getCurrentGroup() {
            return this.singleGroup;
        }
        /**
         * 清理规划器组状态。
         *
         * <p>清理单一规划器组实例的内部状态。
         */
        @Override
        public void clean() {
            if (this.singleGroup != null) {
                this.singleGroup.clear();
            }
        }
    }
    /**
     * 动态规划器组管理器实现。
     *
     * <p>该实现维护一个规划器组实例池，支持多线程并发访问。
     * 通过线程 ID 进行负载均衡，确保不同线程使用不同的规划器组实例。
     *
     * <p>特点：
     * <ul>
     *   <li>支持高并发访问</li>
     *   <li>自动内存管理和清理</li>
     *   <li>基于线程 ID 的负载均衡</li>
     *   <li>定时监控内存使用情况</li>
     * </ul>
     *
     * <p>内存管理策略：
     * <ul>
     *   <li>定期检查 JVM 内存使用情况</li>
     *   <li>当可用内存低于 20% 时自动清理规划器组</li>
     *   <li>可配置的清理间隔时间</li>
     * </ul>
     */
    public static class Dynamic extends PlannerGroupManager {
        /** 日志记录器 */
        private final Logger logger = LoggerFactory.getLogger(PlannerGroupManager.class);

        /** 规划器组实例列表 */
        private final List<PlannerGroup> plannerGroups;

        /** 定时清理调度器 */
        private final ScheduledExecutorService clearScheduler;

        /**
         * 构造动态规划器组管理器。
         *
         * <p>根据配置创建指定数量的规划器组实例，并启动定时清理任务。
         *
         * @param config 规划器配置信息
         * @param relBuilderFactory 关系构建器工厂
         * @throws IllegalArgumentException 如果规划器组大小小于等于 0
         */
        public Dynamic(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
            super(config, relBuilderFactory);
            Preconditions.checkArgument(
                    config.getPlannerGroupSize() > 0,
                    "planner group size should be greater than 0");
            this.plannerGroups = new ArrayList(config.getPlannerGroupSize());
            for (int i = 0; i < config.getPlannerGroupSize(); ++i) {
                this.plannerGroups.add(new PlannerGroup(config, relBuilderFactory));
            }
            this.clearScheduler = new ScheduledThreadPoolExecutor(1);
            int clearInterval = config.getPlannerGroupClearIntervalMinutes();
            this.clearScheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            long freeMemBytes = Runtime.getRuntime().freeMemory();
                            long totalMemBytes = Runtime.getRuntime().totalMemory();
                            Preconditions.checkArgument(
                                    totalMemBytes > 0, "total memory should be greater than 0");
                            if (freeMemBytes / (double) totalMemBytes < 0.2d) {
                                logger.warn(
                                        "start to clear planner groups. There are no enough memory"
                                                + " in JVM, with free memory: {}, total memory: {}",
                                        freeMemBytes,
                                        totalMemBytes);
                                clean();
                            }
                        } catch (Throwable t) {
                            logger.error("failed to clear planner group.", t);
                        }
                    },
                    clearInterval,
                    clearInterval,
                    TimeUnit.MINUTES);
        }
        /**
         * 获取当前线程对应的规划器组实例。
         *
         * <p>使用线程 ID 对规划器组数量取模来实现负载均衡，
         * 确保不同线程使用不同的规划器组实例以避免竞争。
         *
         * @return 当前线程对应的规划器组实例
         * @throws IllegalArgumentException 如果规划器组列表为空
         */
        @Override
        public PlannerGroup getCurrentGroup() {
            Preconditions.checkArgument(
                    !plannerGroups.isEmpty(), "planner groups should not be empty");
            int groupId = (int) Thread.currentThread().getId() % plannerGroups.size();
            return plannerGroups.get(groupId);
        }
        /**
         * 关闭动态管理器并释放资源。
         *
         * <p>停止定时清理调度器，等待最多 10 秒钟让正在执行的任务完成。
         */
        @Override
        public void close() {
            try {
                if (this.clearScheduler != null) {
                    this.clearScheduler.shutdown();
                    this.clearScheduler.awaitTermination(10 * 1000, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                logger.error("failed to close planner group manager.", e);
            }
        }
        /**
         * 同步清理所有规划器组的状态。
         *
         * <p>该方法是线程安全的，会清理所有规划器组实例的内部状态。
         */
        @Override
        public synchronized void clean() {
            plannerGroups.forEach(PlannerGroup::clear);
        }
    }
}
