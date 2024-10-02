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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.ArrayList;
import java.util.List;

/** A files unit for compaction. */

// 一个压缩单元
public interface CompactUnit {

    // 需要输出到的level， 一般是将当前层级压缩到指定的层级
    int outputLevel();

    // 需要进行压缩的文件
    List<DataFileMeta> files();

    static CompactUnit fromLevelRuns(int outputLevel, List<LevelSortedRun> runs) {
        List<DataFileMeta> files = new ArrayList<>();
        for (LevelSortedRun run : runs) {
            // 添加所有的文件
            files.addAll(run.run().files());
        }
        // 返回包含所有文件的 Compact Unit
        return fromFiles(outputLevel, files);
    }

    static CompactUnit fromFiles(int outputLevel, List<DataFileMeta> files) {
        return new CompactUnit() {
            @Override
            public int outputLevel() {
                return outputLevel;
            }

            @Override
            public List<DataFileMeta> files() {
                return files;
            }
        };
    }
}
