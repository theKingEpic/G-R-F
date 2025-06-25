package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.gremlin.Utils;

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * GraphScope 图查询优化器的 HEP 规划器实现。
 *  Original {@code HepPlanner} skip optimizations to the nested structures, i.e. {@code RelNode} nested in the {@code CommonTableScan} or {@code RexSubQuery},
 *  e supplement this functionality by overriding the {@code findBestExp} method.
 * <p>原始的 {@code HepPlanner} 会跳过对嵌套结构的优化，例如嵌套在 {@code CommonTableScan}
 * 或 {@code RexSubQuery} 中的 {@code RelNode}。本类通过重写 {@code findBestExp} 方法来补充此功能。
 *
 * <p>该规划器专门处理图数据库查询计划的优化，支持以下图特有的关系节点：
 * <ul>
 *   <li>{@link GraphLogicalSource} - 图数据源节点</li>
 *   <li>{@link GraphLogicalExpand} - 图扩展操作节点</li>
 *   <li>{@link GraphLogicalGetV} - 图顶点获取节点</li>
 *   <li>{@link GraphLogicalPathExpand} - 图路径扩展节点</li>
 *   <li>{@link GraphLogicalSingleMatch} - 图单模式匹配节点</li>
 *   <li>{@link GraphLogicalMultiMatch} - 图多模式匹配节点</li>
 *   <li>{@link GraphLogicalAggregate} - 图聚合节点</li>
 *   <li>{@link GraphLogicalProject} - 图投影节点</li>
 *   <li>{@link GraphLogicalSort} - 图排序节点</li>
 *   <li>{@link GraphPhysicalExpand} - 图物理扩展节点</li>
 *   <li>{@link GraphPhysicalGetV} - 图物理顶点获取节点</li>
 *   <li>{@link GraphProcedureCall} - 图存储过程调用节点</li>
 * </ul>
 *
 * <p>此外还支持标准的关系操作节点如 {@code LogicalUnion}、{@code LogicalFilter}、
 * {@code LogicalJoin} 和 {@code MultiJoin}。
 *
 * <p>该规划器的核心特性包括：
 * <ul>
 *   <li>递归优化嵌套的查询结构，包括公共表表达式和子查询</li>
 *   <li>通过访问者模式遍历整个查询计划树</li>
 *   <li>对子查询中的 {@code RexSubQuery} 进行深度优化</li>
 *   <li>维护公共关系节点的访问缓存以避免重复优化</li>
 * </ul>
 *
 * @author GraphScope Team
 * @since 1.0
 * @see org.apache.calcite.plan.hep.HepPlanner
 * @see GraphShuttle
 */
public class GraphHepPlanner extends HepPlanner {
    private @Nullable RelNode originalRoot;

    public GraphHepPlanner(HepProgram program) {
        super(program);
    }
    /**
     * 查找最佳的执行计划。
     *
     * <p>重写父类方法以支持对嵌套结构的递归优化。使用访问者模式遍历整个查询计划树，
     * 确保所有嵌套的关系节点和子查询都得到适当的优化。
     *
     * @return 优化后的关系节点
     */
    @Override
    public RelNode findBestExp() {
        return originalRoot.accept(new PlannerVisitor(originalRoot));
    }
    /**
     * 设置查询计划的根节点。
     *
     * @param rel 要设置为根节点的关系节点
     */
    @Override
    public void setRoot(RelNode rel) {
        this.originalRoot = rel;
    }
    /**
     * 对指定的根节点查找最佳执行计划。
     *
     * <p>此方法直接调用父类的优化逻辑，用于处理已经通过访问者模式预处理的子树。
     *
     * @param root 要优化的根节点
     * @return 优化后的关系节点
     */
    public RelNode findBestExpOfRoot(RelNode root) {
        super.setRoot(root);
        return super.findBestExp();
    }

    /**
     * 规划器访问者类，实现对查询计划树的深度遍历和优化。
     *
     * <p>该访问者负责：
     * <ul>
     *   <li>遍历所有类型的图关系节点</li>
     *   <li>处理嵌套在 {@code CommonTableScan} 中的公共表表达式</li>
     *   <li>优化 {@code RexSubQuery} 中的子查询</li>
     *   <li>维护已访问节点的缓存以避免重复处理</li>
     * </ul>
     */
    private class PlannerVisitor extends GraphShuttle {
        private final RelNode root;
        private final IdentityHashMap<RelNode, RelNode> commonRelVisitedMap;
        // apply optimization to the sub-query
        private final RexShuttle subQueryPlanner;

        public PlannerVisitor(RelNode root) {
            this.root = root;
            this.commonRelVisitedMap = new IdentityHashMap<>();
            this.subQueryPlanner =
                    new RexShuttle() {
                        @Override
                        public RexNode visitSubQuery(RexSubQuery subQuery) {
                            RelNode subRel = subQuery.rel;
                            RelNode newSubRel = subRel.accept(new PlannerVisitor(subRel));
                            if (newSubRel == subRel) {
                                return subQuery;
                            }
                            return subQuery.clone(newSubRel);
                        }
                    };
        }

        @Override
        public RelNode visit(GraphLogicalSource source) {
            return findBestIfRoot(source, source);
        }

        @Override
        public RelNode visit(GraphLogicalExpand expand) {
            return findBestIfRoot(expand, visitChildren(expand));
        }

        @Override
        public RelNode visit(GraphLogicalGetV getV) {
            return findBestIfRoot(getV, visitChildren(getV));
        }

        @Override
        public RelNode visit(GraphLogicalPathExpand expand) {
            return findBestIfRoot(expand, visitChildren(expand));
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestIfRoot(match, match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestIfRoot(match, match);
        }

        @Override
        public RelNode visit(GraphLogicalAggregate aggregate) {
            return findBestIfRoot(aggregate, visitChildren(aggregate));
        }

        @Override
        public RelNode visit(GraphLogicalProject project) {
            return findBestIfRoot(project, visitChildren(project));
        }

        @Override
        public RelNode visit(GraphLogicalSort sort) {
            return findBestIfRoot(sort, visitChildren(sort));
        }

        @Override
        public RelNode visit(GraphPhysicalExpand physicalExpand) {
            return findBestIfRoot(physicalExpand, visitChildren(physicalExpand));
        }

        @Override
        public RelNode visit(GraphPhysicalGetV physicalGetV) {
            return findBestIfRoot(physicalGetV, visitChildren(physicalGetV));
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            return findBestIfRoot(union, visitChildren(union));
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            return findBestIfRoot(filter, visitChildren(filter));
        }

        @Override
        public RelNode visit(MultiJoin join) {
            return findBestIfRoot(join, visitChildren(join));
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return findBestIfRoot(join, visitChildren(join));
        }

        @Override
        public RelNode visit(GraphProcedureCall procedureCall) {
            return findBestIfRoot(procedureCall, visitChildren(procedureCall));
        }

        @Override
        public RelNode visit(CommonTableScan tableScan) {
            RelOptTable optTable = tableScan.getTable();
            if (optTable instanceof CommonOptTable) {
                RelNode common = ((CommonOptTable) optTable).getCommon();
                RelNode visited = commonRelVisitedMap.get(common);
                if (visited == null) {
                    visited = common.accept(new PlannerVisitor(common));
                    commonRelVisitedMap.put(common, visited);
                }
                return new CommonTableScan(
                        tableScan.getCluster(),
                        tableScan.getTraitSet(),
                        new CommonOptTable(visited));
            }
            return tableScan;
        }

        private RelNode findBestIfRoot(RelNode oldRel, RelNode newRel) {
            newRel = newRel.accept(this.subQueryPlanner);
            return oldRel == root ? findBestExpOfRoot(newRel) : newRel;
        }
    }
    /**
     * 清理规划器的内部状态。
     *
     * <p>除了调用父类的清理方法外，还会清理内部的摘要到顶点的映射缓存，
     * 以确保规划器可以被重复使用而不会出现状态污染。
     */
    @Override
    public void clear() {
        super.clear();
        Map<RelDigest, HepRelVertex> mapDigestToVertex =
                Utils.getFieldValue(HepPlanner.class, this, "mapDigestToVertex");
        mapDigestToVertex.clear();
    }
}
