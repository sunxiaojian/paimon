/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/** Compact task for merge tree compaction. */
public class MergeTreeCompactTask extends CompactTask {

    private final long minFileSize;
    private final CompactRewriter rewriter;
    private final int outputLevel;
    private final Supplier<CompactDeletionFile> compactDfSupplier;

    private final List<List<SortedRun>> partitioned;

    private final boolean dropDelete;
    private final int maxLevel;

    // metric
    private int upgradeFilesNum;

    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            int maxLevel,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            Supplier<CompactDeletionFile> compactDfSupplier) {
        super(metricsReporter);
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.compactDfSupplier = compactDfSupplier;

        // 将有范围交叉的进行分区后处理
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
        this.dropDelete = dropDelete;
        this.maxLevel = maxLevel;

        this.upgradeFilesNum = 0;
    }

    // 开始进行压缩
    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>();
        CompactResult result = new CompactResult();

        // Checking the order and compacting adjacent and contiguous files
        // Note: can't skip an intermediate file to compact, this will destroy the overall
        // orderliness

        // 检查相邻和顺序的文件并进行压缩，顺序去执行压缩
        // sorted run 之间的文件不进行交叉
        for (List<SortedRun> section : partitioned) {
            // 当前分割批次的数量> 1, 则直接加到后补名单中
            if (section.size() > 1) {
                candidate.add(section);
            } else {

                SortedRun run = section.get(0);
                // No overlapping:
                // We can just upgrade the large file and just change the level instead of
                // rewriting it
                // But for small files, we will try to compact it

                // 小文件进行压缩，大的文件，直接升级到下个层级即可
                for (DataFileMeta file : run.files()) {
                    if (file.fileSize() < minFileSize) {
                        // Smaller files are rewritten along with the previous files
                        // 小文件需要先压缩
                        candidate.add(singletonList(SortedRun.fromSingle(file)));
                    } else {
                        // Large file appear, rewrite previous and upgrade it
                        // 出现大文件，重写之前的文件并升级
                        rewrite(candidate, result);
                        // 再把当前的大文件升级到最高层
                        upgrade(file, result);
                    }
                }
            }
        }
        // 重写
        rewrite(candidate, result);
        result.setDeletionFile(compactDfSupplier.get());
        return result;
    }

    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), upgradeFilesNum);
    }

    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        // 若当前文件的level与输出文件的level相等，则不进行任何操作
        if (file.level() == outputLevel) {
            return;
        }

        if (outputLevel != maxLevel || file.deleteRowCount().map(d -> d == 0).orElse(false)) {
            // 如果outputLevel不是最大的level, 或者当前文件的删除的行是0， 则直接将文件升级到下一层
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult);
            upgradeFilesNum++;
        } else {
            // files with delete records should not be upgraded directly to max level
            // 有删除记录的文件不能直接升级到最高的level
            List<List<SortedRun>> candidate = new ArrayList<>();
            candidate.add(new ArrayList<>());
            candidate.get(0).add(SortedRun.fromSingle(file));
            rewriteImpl(candidate, toUpdate);
        }
    }

    // 小文件重写
    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        if (candidate.isEmpty()) {
            return;
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {
                // 遍历选中的文件进行压缩
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate);
                }
                candidate.clear();
                return;
            }
        }
        rewriteImpl(candidate, toUpdate);
    }

    private void rewriteImpl(List<List<SortedRun>> candidate, CompactResult toUpdate)
            throws Exception {
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        toUpdate.merge(rewriteResult);
        candidate.clear();
    }
}
