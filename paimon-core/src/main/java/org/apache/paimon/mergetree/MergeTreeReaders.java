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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Utility class to create commonly used {@link RecordReader}s for merge trees. */
public class MergeTreeReaders {

    private MergeTreeReaders() {}

    public static <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<ReaderSupplier<T>> readers = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            // 每一个 sort 对应一个 SortMergeReader
            readers.add(
                    () ->
                            // return sort reader(min-heap reader or loser-sort reader)
                            readerForSection(
                                    section,
                                    readerFactory,
                                    userKeyComparator,
                                    userDefinedSeqComparator,
                                    mergeFunctionWrapper,
                                    mergeSorter));
        }
        return ConcatRecordReader.create(readers);
    }

    public static <T> RecordReader<T> readerForSection(
            List<SortedRun> section,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<SizedReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (SortedRun run : section) {
            readers.add(
                    new SizedReaderSupplier<KeyValue>() {
                        @Override
                        public long estimateSize() {
                            return run.totalSize();
                        }

                        @Override
                        public RecordReader<KeyValue> get() throws IOException {
                            /**
                             * 1. 遍历 SortRun 中的文件，创建 FileReader 2.
                             * 返回ConcatReader，简单的将所有的文件读取作为一个整体进行返回
                             */
                            return readerForRun(run, readerFactory);
                        }
                    });
        }
        // 对section 中需要合并的每个SortRun中的文件进行合并， 生成 MergeSort 排序， 会存在spill 和 no spill两种（随后进行分析）
        return mergeSorter.mergeSort(
                readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
    }

    // 为 SortRun 中的每个文件生成一个reader
    private static RecordReader<KeyValue> readerForRun(
            SortedRun run, FileReaderFactory<KeyValue> readerFactory) throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        // 对每个文件生成一个 file reader
        for (DataFileMeta file : run.files()) {
            // 通过 KeyValueFileReaderFactory  创建reader
            readers.add(() -> readerFactory.createRecordReader(file));
        }
        return ConcatRecordReader.create(readers);
    }
}
