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
import java.util.List;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Prepares collection of cases to check:
 * - Case is a collection of entries to write within single transaction.
 * - Entry is described with a pair { primary -> backup }.
 */
public class ConsistentCutBlockingCases {
    /**
     * @param nodesCnt Count of nodes that participated in a test case.
     */
    public static List<List<T2<Integer, Integer>>> casesNoBackup(int nodesCnt) {
        List<List<T2<Integer, Integer>>> cases = new ArrayList<>();

        // One entry.
        for (int n = 0; n < nodesCnt; n++) {
            List<T2<Integer, Integer>> c = new ArrayList<>();
            c.add(new T2<>(n, null));

            cases.add(c);
        }

        // Two entries.
        for (int n = 0; n < nodesCnt; n++) {
            List<T2<Integer, Integer>> c = new ArrayList<>();

            for (int n2 = 0; n2 < nodesCnt; n2++) {
                if (n == n2)
                    continue;

                c.add(new T2<>(n, null));
                c.add(new T2<>(n2, null));
            }

            cases.add(c);
        }

        return cases;
    }

    /**
     * @param nodesCnt Count of nodes that participated in a test case.
     */
    public static List<List<T2<Integer, Integer>>> casesWithBackup(int nodesCnt) {
        List<List<T2<Integer, Integer>>> cases = new ArrayList<>();

        // Possible pairs { primary -> backup }.
        List<T2<Integer, Integer>> pairs = new ArrayList<>();

        for (int p = 0; p < nodesCnt; p++) {
            for (int b = 0; b < nodesCnt; b++) {
                if (b == p)
                    continue;

                pairs.add(new T2<>(p, b));
            }
        }

        // One entry.
        for (int p = 0; p < pairs.size(); p++) {
            List<T2<Integer, Integer>> c = new ArrayList<>();
            c.add(pairs.get(p));

            cases.add(c);
        }

        // Two entries.
        for (int p1 = 0; p1 < pairs.size(); p1++) {
            for (int p2 = 0; p2 < pairs.size(); p2++) {
                List<T2<Integer, Integer>> c = new ArrayList<>();

                c.add(pairs.get(p1));
                c.add(pairs.get(p2));

                cases.add(c);
            }
        }

        return cases;
    }
}