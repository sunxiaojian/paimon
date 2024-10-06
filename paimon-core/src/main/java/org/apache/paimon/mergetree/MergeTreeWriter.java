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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link CompactIncrement}. */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final boolean writeBufferSpillable;
    private final MemorySize maxDiskSize;
    private final int sortMaxFan;
    private final CompressOptions sortCompression;
    private final IOManager ioManager;

    private final RowType keyType;
    private final RowType valueType;
    private final CompactManager compactManager;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;
    private final KeyValueFileWriterFactory writerFactory;
    private final boolean commitForceCompact;
    private final ChangelogProducer changelogProducer;
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    private final LinkedHashSet<DataFileMeta> newFiles;
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    @Nullable private CompactDeletionFile compactDeletionFile;

    private long newSequenceNumber;
    private WriteBuffer writeBuffer;
    private boolean isInsertOnly;

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        userDefinedSeqComparator,
                        memoryPool,
                        writeBufferSpillable,
                        maxDiskSize,
                        sortMaxFan,
                        sortCompression,
                        ioManager);
    }

    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber = newSequenceNumber();
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            // 若写入不成功，先将内存中数据刷新到磁盘中，并进行一次压缩
            flushWriteBuffer(false, false);
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long maxSequenceNumber() {
        return newSequenceNumber - 1;
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }

            // 判断是否要声明 change log writer（若是input 是直接写入 change log 目录， 所以需要创建新的滚动log 的文件）
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    (changelogProducer == ChangelogProducer.INPUT && !isInsertOnly)
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;

            // 文件先滚动刷新到 l0 中
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

            try {
                // 将 memory 中的数据刷新到 level0 , Memory中的数据是已经排序过的, 写入到对应的磁盘文件中
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        // 直接写入change log 文件中
                        changelogWriter == null ? null : changelogWriter::write,
                        dataWriter::write);
            } finally {
                // 清空 memory， 供后续使用
                writeBuffer.clear();
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            // 返回写入文件的描述 DataFileMeta
            List<DataFileMeta> dataMetas = dataWriter.result();
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            } else if (changelogProducer == ChangelogProducer.INPUT && isInsertOnly) {
                List<DataFileMeta> changelogMetas = new ArrayList<>();
                for (DataFileMeta dataMeta : dataMetas) {
                    DataFileMeta changelogMeta =
                            dataMeta.rename(writerFactory.newChangelogPath(0).getName());
                    writerFactory.copyFile(dataMeta.fileName(), changelogMeta.fileName(), 0);
                    changelogMetas.add(changelogMeta);
                }
                newFilesChangelog.addAll(changelogMetas);
            }

            // 将返回的文件结果添加到 level0 层
            for (DataFileMeta dataMeta : dataMetas) {
                newFiles.add(dataMeta);
                compactManager.addNewFile(dataMeta);
            }
        }
        // waitForLatestCompaction=true等待上一个compaction 完成，再进行下一个压缩开始， 若
        // waitForLatestCompaction=false, 则不需要等待，直接触发压缩
        trySyncLatestCompaction(waitForLatestCompaction);
        // 触发对文件的压缩，内部会分为全压缩和部分压缩 ， 部分压缩会有三个方式
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flushWriteBuffer(waitCompaction, false);
        if (commitForceCompact) {
            waitCompaction = true;
        }
        // Decide again whether to wait here.
        // For example, in the case of repeated failures in writing, it is possible that Level 0
        // files were successfully committed, but failed to restart during the compaction phase,
        // which may result in an increasing number of Level 0 files. This wait can avoid this
        // situation.
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }
        trySyncLatestCompaction(waitCompaction);
        return drainIncrement();
    }

    @Override
    public boolean isCompacting() {
        return compactManager.isCompacting();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    @Override
    public void withInsertOnly(boolean insertOnly) {
        if (insertOnly && writeBuffer != null && writeBuffer.size() > 0) {
            throw new IllegalStateException(
                    "Insert-only can only be set before any record is received.");
        }
        this.isInsertOnly = insertOnly;
    }

    private CommitIncrement drainIncrement() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));
        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();
        compactDeletionFile = null;

        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }

    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    private void updateCompactResult(CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file.fileName(), file.level());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());

        updateCompactDeletionFile(result.deletionFile());
    }

    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }

        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }
}
