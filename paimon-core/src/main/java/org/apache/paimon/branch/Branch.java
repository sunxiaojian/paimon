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

package org.apache.paimon.branch;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

public class Branch {

    private static final String FIELD_BRANCH_NAME = "branchName";
    private static final String FIELD_FROM_SNAPSHOT_ID = "fromSnapshotId";
    private static final String FIELD_FROM_TAG_NAME = "fromTagName";
    private static final String FIELD_BRANCH_CREATE_TIME = "branchCreateTime";
    private static final String FIELD_BRANCH_TIME_RETAINED = "branchTimeRetained";

    @JsonProperty(FIELD_BRANCH_NAME)
    @Nullable
    private final String branchName;

    @JsonProperty(FIELD_FROM_SNAPSHOT_ID)
    @Nullable
    private final Long fromSnapshotId;

    @JsonProperty(FIELD_FROM_TAG_NAME)
    @Nullable
    private final String fromTagName;

    @JsonProperty(FIELD_BRANCH_CREATE_TIME)
    @Nullable
    private final LocalDateTime branchCreateTime;

    @JsonProperty(FIELD_BRANCH_TIME_RETAINED)
    @Nullable
    private final Duration branchTimeRetained;

    public Branch(
            @JsonProperty(FIELD_BRANCH_NAME) String branchName,
            @JsonProperty(FIELD_FROM_SNAPSHOT_ID) Long fromSnapshotId,
            @JsonProperty(FIELD_FROM_TAG_NAME) String fromTagName,
            @JsonProperty(FIELD_BRANCH_CREATE_TIME) LocalDateTime branchCreateTime,
            @JsonProperty(FIELD_BRANCH_TIME_RETAINED) Duration branchTimeRetained) {
        this.branchName = branchName;
        this.fromSnapshotId = fromSnapshotId;
        this.fromTagName = fromTagName;
        this.branchCreateTime = branchCreateTime;
        this.branchTimeRetained = branchTimeRetained;
    }

    @JsonGetter(FIELD_BRANCH_NAME)
    @Nullable
    public String getBranchName() {
        return branchName;
    }

    @JsonGetter(FIELD_FROM_SNAPSHOT_ID)
    @Nullable
    public Long getFromSnapshotId() {
        return fromSnapshotId;
    }

    @JsonGetter(FIELD_FROM_TAG_NAME)
    @Nullable
    public String getFromTagName() {
        return fromTagName;
    }

    @JsonGetter(FIELD_BRANCH_CREATE_TIME)
    @Nullable
    public LocalDateTime getBranchCreateTime() {
        return branchCreateTime;
    }

    @JsonGetter(FIELD_BRANCH_TIME_RETAINED)
    @Nullable
    public Duration getBranchTimeRetained() {
        return branchTimeRetained;
    }

    public static Branch fromTagAndBranchTtl(
            String branchName,
            String tagName,
            Snapshot snapshot,
            LocalDateTime branchCreateTime,
            Duration branchTimeRetained) {
        return new Branch(
                branchName,
                snapshot == null ? null : snapshot.id(),
                tagName,
                branchCreateTime,
                branchTimeRetained);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Branch fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Branch.class);
    }

    @Nullable
    public static Branch safelyFromPath(FileIO fileIO, Path path) throws IOException {
        try {
            if (!fileIO.exists(path)) {
                // Compatible with older versions
                return null;
            }
            String json = fileIO.readFileUtf8(path);
            return Branch.fromJson(json);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Branch branch = (Branch) o;
        return Objects.equals(branchName, branch.branchName)
                && Objects.equals(fromSnapshotId, branch.fromSnapshotId)
                && Objects.equals(fromTagName, branch.fromTagName)
                && Objects.equals(branchCreateTime, branch.branchCreateTime)
                && Objects.equals(branchTimeRetained, branch.branchTimeRetained);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                branchName, fromSnapshotId, fromTagName, branchCreateTime, branchTimeRetained);
    }
}
