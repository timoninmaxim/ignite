/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.consistentcut.recovery.FindRecoveryConsistentSegmentsTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** */
public class CommonConsistentCutVersionReduceTest {
    /** */
    @Test
    public void testFindCommonVersion() {
        for (int nodesCnt = 1; nodesCnt < 10; nodesCnt++) {
            for (int verCnt = 1; verCnt < 10; verCnt++) {
                for (int commonVer = 1; commonVer < verCnt; commonVer++) {
                    for (int missCnt = 1; missCnt < nodesCnt - 1; missCnt++) {
                        List<Iterator<ConsistentCutVersion>> data = testData(nodesCnt, verCnt, commonVer, missCnt);

                        ConsistentCutVersion cutVer = FindRecoveryConsistentSegmentsTask
                            .findGreatestCommon(data, ConsistentCutVersion::compareTo).get(0);

                        String err = String.format("%d %d %d %d", nodesCnt, verCnt, commonVer, missCnt);

                        assertNotNull(err, cutVer);
                        assertEquals(err, commonVer, cutVer.version());
                    }
                }
            }
        }
    }

    /** */
    @Test
    public void testNoCommonVersion() {
        for (int nodesCnt = 1; nodesCnt < 10; nodesCnt++) {
            for (int verCnt = 1; verCnt < 10; verCnt++) {
                for (int missCnt = 1; missCnt < nodesCnt - 1; missCnt++) {
                    List<Iterator<ConsistentCutVersion>> data = testData(nodesCnt, verCnt, Integer.MAX_VALUE, missCnt);

                    List<ConsistentCutVersion> cutVer =
                        FindRecoveryConsistentSegmentsTask.findGreatestCommon(data, ConsistentCutVersion::compareTo);

                    String err = String.format("%d %d %d", nodesCnt, verCnt, missCnt);

                    assertNull(err, cutVer);
                }
            }
        }
    }

    /**
     * Prepares collections of Consistent Cut version from multiple nodes. It emulates misses (inconsistent cuts).
     */
    private List<Iterator<ConsistentCutVersion>> testData(int nodesCnt, int verCnt, int commonVer, int missCnt) {
        List<List<ConsistentCutVersion>> nodesCutVers = new ArrayList<>(nodesCnt);
        for (int i = 0; i < nodesCnt; i++) {
            List<ConsistentCutVersion> vers = IntStream.rangeClosed(1, verCnt).boxed()
                .sorted(Comparator.reverseOrder())
                .map(ver -> new ConsistentCutVersion(ver, AffinityTopologyVersion.NONE))
                .collect(Collectors.toList());

            nodesCutVers.add(vers);
        }

        List<List<Integer>> missMask = new ArrayList<>(nodesCnt);
        for (int i = 0; i < nodesCnt; i++)
            missMask.add(new ArrayList<>());

        for (int currVer = verCnt; currVer > 0; currVer -= 1) {
            if (currVer == commonVer)
                continue;

            for (int i = 0; i < missCnt; i++)
                missMask.get(i).add(currVer);
        }

        for (int i = 0; i < nodesCnt; i++) {
            for (int j = 0; j < missMask.get(i).size(); j++)
                nodesCutVers.get(i).remove(verCnt - missMask.get(i).get(j) - j);
        }

        Collections.shuffle(nodesCutVers);

        return nodesCutVers.stream().map(List::iterator).collect(Collectors.toList());
    }
}
