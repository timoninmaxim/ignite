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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ServicesTest.TestNodeIdService;
import org.apache.ignite.internal.client.thin.ServicesTest.TestNodeIdServiceInterface;
import org.junit.Test;

/** */
public class ServicesFailoverTest extends AbstractThinClientTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrids(3);

        try (IgniteClient client = startClient(0, 1, 2)) {
            srv.services().deployClusterSingleton("svc", new TestNodeIdService());

            TestNodeIdServiceInterface svc = client.services().serviceProxy("svc", TestNodeIdServiceInterface.class);
            svc.nodeId();

            System.out.println("==================================================");


            stopGrid(0);
            stopGrid(1);

            svc = client.services().serviceProxy("svc", TestNodeIdServiceInterface.class);

            svc.nodeId();
        }
    }
}
