/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.consistentcut.recovery;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutVersion;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Finds the latest common {@link ConsistentCutVersion} from multiple nodes to use for recovery.
 */
public class FindRecoveryConsistentSegmentsTask
    extends ComputeTaskAdapter<FindRecoveryConsistentSegmentsTaskArg, RecoveryConsistentSegments> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable FindRecoveryConsistentSegmentsTaskArg arg
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node: subgrid) {
            map.put(new ComputeJobAdapter() {
                /** */
                @LoggerResource
                private transient IgniteLogger log;

                /** */
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public @Nullable List<ConsistentSegment> execute() throws IgniteException {
                    try {
                        FileDescriptor[] walDesc = ignite.context().cache().context().wal().walArchiveFiles();

                        File[] walArchiveFiles = Arrays.stream(walDesc).map(FileDescriptor::file)
                            .toArray(File[]::new);

                        if (walArchiveFiles.length == 0) {
                            log.warning("No WAL archive files found for PITR. Will restore on ClusterSnapshot only.");

                            return Collections.emptyList();
                        }

                        return new NodeConsistentCutSegments(log, walArchiveFiles, arg.snapshotName()).get();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }

            }, node);
        }

        return map;
    }

    /**
     * Reduces collections of available Consistent Cuts from multiple nodes. Finds a common version for using for recovery.
     * If no common version found returns {@code null}. It means Ignite won't use WAL for recovery.
     */
    @Override public @Nullable RecoveryConsistentSegments reduce(
        List<ComputeJobResult> results
    ) throws IgniteException {
        List<Iterator<ConsistentSegment>> nodesSegments = new ArrayList<>(results.size());

        boolean noCuts = false;

        Map<ClusterNode, Exception> ex = new HashMap<>();

        for (ComputeJobResult res: results) {
            List<ConsistentSegment> segments = res.getData();

            if (res.getException() != null || segments == null) {
                ex.put(res.getNode(), res.getException());

                continue;
            }

            noCuts |= segments.isEmpty();

            // If no cuts then just check nodes for exceptions.
            if (!noCuts)
                nodesSegments.add(segments.iterator());
        }

        if (!ex.isEmpty())
            return new RecoveryConsistentSegments(null, ex);

        List<ConsistentCutVersion> recoveryVersions = null;

        ConsistentCutVersion latestCutVer = null;

        if (noCuts)
            latestCutVer = null;
        else {
            while (latestCutVer == null) {
                List<ConsistentSegment> segments = findGreatestCommon(nodesSegments, ConsistentSegment::compareTo);

                if (F.isEmpty(segments))
                    break;

                List<Iterator<ConsistentCutVersion>> segVers = segments.stream()
                    .map(ConsistentSegment::segment)
                    .map(List::iterator)
                    .collect(Collectors.toList());

                List<ConsistentCutVersion> common = findGreatestCommon(segVers, ConsistentCutVersion::compareTo);

                if (!F.isEmpty(common)) {
                    latestCutVer = common.get(0);

                    recoveryVersions = new ArrayList<>();
                    recoveryVersions.add(0, latestCutVer);
                }
            }

            if (latestCutVer != null) {
                while (nodesSegments.get(0).hasNext()) {
                    ConsistentSegment s = nodesSegments.get(0).next();

                    if (s.recovered())
                        recoveryVersions.add(0, s.last());
                }
            }
        }

        log.info("Found common ConsistentCut version for restart: " + latestCutVer);

        return new RecoveryConsistentSegments(recoveryVersions, null);
    }

    /**
     * @return The greatest common object in specified iterators (reverse ordered).
     */
    public static <T> @Nullable List<T> findGreatestCommon(List<Iterator<T>> iters, Comparator<T> comp) {
        if (!iters.get(0).hasNext())
            return null;

        List<T> res = new ArrayList<>(iters.size());

        int node = 0;
        T candidate = iters.get(0).next();

        res.add(candidate);

        for (int i = 0; i < iters.size(); i++) {
            if (i == node)
                continue;  // Candidate for common was already extracted from this node.

            T next;
            int cmp;

            do {
                if (!iters.get(i).hasNext())
                    return null;

                next = iters.get(i).next();
            } while ((cmp = comp.compare(next, candidate)) > 0);

            if (cmp < 0) {
                candidate = next;
                node = i;

                res.clear();
                res.add(candidate);

                i = 0;
            }
            else if (cmp == 0)
                res.add(next);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Need handle exceptions during the `reduce` phase to gracefully handle it on checking ClusterSnapshot phase.
        return ComputeJobResultPolicy.WAIT;
    }
}
