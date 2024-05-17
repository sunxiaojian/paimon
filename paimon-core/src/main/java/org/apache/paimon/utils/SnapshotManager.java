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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.getBranchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;

/** Manager for {@link Snapshot}, providing utility methods related to paths and snapshot hints. */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SNAPSHOT_PREFIX = "snapshot-";
    private static final String CHANGELOG_PREFIX = "changelog-";
    public static final String EARLIEST = "EARLIEST";
    public static final String LATEST = "LATEST";
    private static final int READ_HINT_RETRY_NUM = 3;
    private static final int READ_HINT_RETRY_INTERVAL = 1;

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;

    public SnapshotManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    /** Specify the default branch for data writing. */
    public SnapshotManager(FileIO fileIO, Path tablePath, String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = StringUtils.isBlank(branchName) ? DEFAULT_MAIN_BRANCH : branchName;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Path snapshotDirectory() {
        return new Path(tablePath + "/snapshot");
    }

    public Path changelogDirectory() {
        return new Path(tablePath + "/changelog");
    }

    public Path changelogDirectory(String branchName) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? changelogDirectory()
                : new Path(getBranchPath(tablePath, branchName) + "/changelog");
    }

    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(tablePath + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    public Path longLivedChangelogPath(String branchName, long snapshotId) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? longLivedChangelogPath(snapshotId)
                : new Path(
                        getBranchPath(tablePath, branchName)
                                + "/changelog/"
                                + CHANGELOG_PREFIX
                                + snapshotId);
    }

    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path snapshotPath(String branchName, long snapshotId) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotPath(snapshotId)
                : new Path(
                        getBranchPath(tablePath, branchName)
                                + "/snapshot/"
                                + SNAPSHOT_PREFIX
                                + snapshotId);
    }

    public Path snapshotDirectory(String branchName) {
        return branchName.equals(DEFAULT_MAIN_BRANCH)
                ? snapshotDirectory()
                : new Path(getBranchPath(tablePath, branchName) + "/snapshot");
    }

    public Snapshot snapshot(long snapshotId) {
        return snapshot(snapshotId, branch);
    }

    private Snapshot snapshot(long snapshotId, String branch) {
        Path snapshotPath = snapshotPath(branch, snapshotId);
        return Snapshot.fromPath(fileIO, snapshotPath);
    }

    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(branch, snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(branch, snapshotId));
    }

    public boolean snapshotExists(long snapshotId) {
        return snapshotExists(snapshotId, branch);
    }

    public boolean snapshotExists(long snapshotId, String branch) {
        Path path = snapshotPath(branch, snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(branch, snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public @Nullable Snapshot latestSnapshot() {
        return latestSnapshot(branch);
    }

    public @Nullable Snapshot latestSnapshot(String branch) {
        Long snapshotId = latestSnapshotId(branch);
        return snapshotId == null ? null : snapshot(snapshotId, branch);
    }

    public @Nullable Long latestSnapshotId() {
        return latestSnapshotId(branch);
    }

    public @Nullable Long latestSnapshotId(String branch) {
        try {
            return findLatest(
                    snapshotDirectory(branch), SNAPSHOT_PREFIX, branch, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    public @Nullable Snapshot earliestSnapshot(String branch) {
        Long snapshotId = earliestSnapshotId(branch);
        return snapshotId == null ? null : snapshot(snapshotId, branch);
    }

    public @Nullable Long earliestSnapshotId() {
        return earliestSnapshotId(branch);
    }

    @VisibleForTesting
    public @Nullable Long earliestSnapshotId(String branch) {
        try {
            return findEarliest(
                    snapshotDirectory(branch), SNAPSHOT_PREFIX, branch, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    public @Nullable Long earliestLongLivedChangelogId() {
        return earliestLongLivedChangelogId(branch);
    }

    public @Nullable Long earliestLongLivedChangelogId(String branch) {
        try {
            return findEarliest(
                    changelogDirectory(branch),
                    CHANGELOG_PREFIX,
                    branch,
                    this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }

    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(
                    changelogDirectory(branch),
                    CHANGELOG_PREFIX,
                    branch,
                    this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }

    public @Nullable Long latestChangelogId() {
        return latestSnapshotId(branch);
    }

    public @Nullable Long pickOrLatest(Predicate<Snapshot> predicate) {
        return pickOrLatest(branch, predicate);
    }

    public @Nullable Long pickOrLatest(String branch, Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId(branch);
        Long earliestId = earliestSnapshotId(branch);
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId, branch)) {
                Snapshot snapshot = snapshot(snapshotId, branch);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return latestId;
    }

    private Snapshot changelogOrSnapshot(long snapshotId) {
        if (longLivedChangelogExists(snapshotId)) {
            return changelog(snapshotId);
        } else {
            return snapshot(snapshotId);
        }
    }

    public @Nullable Long earlierThanTimeMills(long timestampMills, boolean startFromChangelog) {
        return earlierThanTimeMills(branch, timestampMills, startFromChangelog);
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public @Nullable Long earlierThanTimeMills(
            String branch, long timestampMills, boolean startFromChangelog) {
        Long earliestSnapshot = earliestSnapshotId(branch);
        Long earliest;
        if (startFromChangelog) {
            Long earliestChangelog = earliestLongLivedChangelogId(branch);
            earliest = earliestChangelog == null ? earliestSnapshot : earliestChangelog;
        } else {
            earliest = earliestSnapshot;
        }
        Long latest = latestSnapshotId(branch);
        if (earliest == null || latest == null) {
            return null;
        }

        if (changelogOrSnapshot(earliest).timeMillis() >= timestampMills) {
            return earliest - 1;
        }

        while (earliest < latest) {
            long mid = (earliest + latest + 1) / 2;
            if (changelogOrSnapshot(mid).timeMillis() < timestampMills) {
                earliest = mid;
            } else {
                latest = mid - 1;
            }
        }
        return earliest;
    }

    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        return earlierOrEqualTimeMills(branch, timestampMills);
    }

    /**
     * Returns a {@link Snapshot} whoes commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(String branch, long timestampMills) {
        Long earliest = earliestSnapshotId(branch);
        Long latest = latestSnapshotId(branch);
        if (earliest == null || latest == null) {
            return null;
        }

        if (snapshot(earliest, branch).timeMillis() > timestampMills) {
            return null;
        }
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid, branch);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }
        Long earliestWatermark = null;
        // find the first snapshot with watermark
        if ((earliestWatermark = snapshot(earliest).watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                    finalSnapshot = snapshot;
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public long snapshotCount() throws IOException {
        return snapshotCount(branch);
    }

    public long snapshotCount(String branch) throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(branch), SNAPSHOT_PREFIX).count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(branch), SNAPSHOT_PREFIX)
                .map(id -> snapshot(id))
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(branch), CHANGELOG_PREFIX)
                .map(snapshotId -> changelog(snapshotId))
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, snapshotDirectory(branch), SNAPSHOT_PREFIX)
                        .map(id -> snapshotPath(branch, id))
                        .collect(Collectors.toList());

        List<Snapshot> snapshots = new ArrayList<>();
        for (Path path : paths) {
            Snapshot snapshot = Snapshot.safelyFromPath(fileIO, path);
            if (snapshot != null) {
                snapshots.add(snapshot);
            }
        }

        return snapshots;
    }

    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(branch), CHANGELOG_PREFIX)
                        .map(id -> longLivedChangelogPath(branch, id))
                        .collect(Collectors.toList());

        List<Changelog> changelogs = new ArrayList<>();
        for (Path path : paths) {
            try {
                String json = fileIO.readFileUtf8(path);
                changelogs.add(Changelog.fromJson(json));
            } catch (FileNotFoundException ignored) {
            }
        }

        return changelogs;
    }

    /**
     * Try to get non snapshot files. If any error occurred, just ignore it and return an empty
     * result.
     */
    public List<Path> tryGetNonSnapshotFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(
                snapshotDirectory(branch), fileStatusFilter, nonSnapshotFileFilter());
    }

    public List<Path> tryGetNonChangelogFiles(Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(changelogDirectory(), fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Path> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        try {
            FileStatus[] statuses = fileIO.listStatus(directory);
            if (statuses == null) {
                return Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .filter(fileStatusFilter)
                    .map(FileStatus::getPath)
                    .filter(fileFilter)
                    .collect(Collectors.toList());
        } catch (IOException ignored) {
            return Collections.emptyList();
        }
    }

    private Predicate<Path> nonSnapshotFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private Predicate<Path> nonChangelogFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        Long latestId = latestSnapshotId(branch);
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(branch),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");
        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshot(id, branch);
            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /** Find the snapshot of the specified identifiers written by the specified user. */
    public List<Snapshot> findSnapshotsForIdentifiers(
            @Nonnull String user, List<Long> identifiers) {
        if (identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Long latestId = latestSnapshotId(branch);
        if (latestId == null) {
            return Collections.emptyList();
        }
        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(branch),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");

        long minSearchedIdentifier = identifiers.stream().min(Long::compareTo).get();
        List<Snapshot> matchedSnapshots = new ArrayList<>();
        Set<Long> remainingIdentifiers = new HashSet<>(identifiers);
        for (long id = latestId; id >= earliestId && !remainingIdentifiers.isEmpty(); id--) {
            Snapshot snapshot = snapshot(id, branch);
            if (user.equals(snapshot.commitUser())) {
                if (remainingIdentifiers.remove(snapshot.commitIdentifier())) {
                    matchedSnapshots.add(snapshot);
                }
                if (snapshot.commitIdentifier() <= minSearchedIdentifier) {
                    break;
                }
            }
        }
        return matchedSnapshots;
    }

    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFileUtf8(longLivedChangelogPath(branch, id), changelog.toJson());
    }

    /**
     * Traversal snapshots from latest to earliest safely, this is applied on the writer side
     * because the committer may delete obsolete snapshots, which may cause the writer to encounter
     * unreadable snapshots.
     */
    @Nullable
    public Snapshot traversalSnapshotsFromLatestSafely(Filter<Snapshot> checker) {
        Long latestId = latestSnapshotId(branch);
        if (latestId == null) {
            return null;
        }
        Long earliestId = earliestSnapshotId(branch);
        if (earliestId == null) {
            return null;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id, branch);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId(branch);
                if (newEarliestId == null) {
                    return null;
                }

                // this is a valid snapshot, should not throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return null;
            }

            if (checker.test(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private @Nullable Long findLatest(
            Path dir, String prefix, String branch, BiFunction<String, Long, Path> file)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return null;
        }

        Long snapshotId = readHint(LATEST, dir);
        if (snapshotId != null && snapshotId > 0) {
            long nextSnapshot = snapshotId + 1;
            // it is the latest only there is no next one
            if (!fileIO.exists(file.apply(branch, nextSnapshot))) {
                return snapshotId;
            }
        }
        return findByListFiles(Math::max, dir, prefix);
    }

    private @Nullable Long findEarliest(
            Path dir, String prefix, String branch, BiFunction<String, Long, Path> file)
            throws IOException {
        if (!fileIO.exists(dir)) {
            return null;
        }

        Long snapshotId = readHint(EARLIEST, dir);
        // null and it is the earliest only it exists
        if (snapshotId != null && fileIO.exists(file.apply(branch, snapshotId))) {
            return snapshotId;
        }

        return findByListFiles(Math::min, dir, prefix);
    }

    public Long readHint(String fileName) {
        return readHint(fileName, branch);
    }

    @VisibleForTesting
    public Long readHint(String fileName, String branch) {
        return readHint(fileName, snapshotDirectory(branch));
    }

    public Long readHint(String fileName, Path dir) {
        Path path = new Path(dir, fileName);
        int retryNumber = 0;
        while (retryNumber++ < READ_HINT_RETRY_NUM) {
            try {
                return fileIO.readOverwrittenFileUtf8(path).map(Long::parseLong).orElse(null);
            } catch (Exception ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_HINT_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private Long findByListFiles(BinaryOperator<Long> reducer, Path dir, String prefix)
            throws IOException {
        return listVersionedFiles(fileIO, dir, prefix).reduce(reducer).orElse(null);
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, snapshotDirectory(branch));
    }

    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, LATEST, changelogDirectory());
    }

    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, changelogDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        commitHint(snapshotId, EARLIEST, snapshotDirectory(branch));
    }

    private void commitHint(long snapshotId, String fileName, Path dir) throws IOException {
        Path hintFile = new Path(dir, fileName);
        fileIO.overwriteFileUtf8(hintFile, String.valueOf(snapshotId));
    }
}
