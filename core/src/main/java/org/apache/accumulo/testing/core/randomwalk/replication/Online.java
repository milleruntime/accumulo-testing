/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing.core.randomwalk.replication;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.testing.core.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.core.randomwalk.State;
import org.apache.accumulo.testing.core.randomwalk.Test;

public class Online extends Test {

  /**
   * Ensure the replication table is online
   */
  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    Connector c = env.getAccumuloConnector();

    ReplicationTable.setOnline(c);
    boolean online = ReplicationTable.isOnline(c);
    for (int i = 0; i < 10; i++) {
      if (online)
        break;
      sleepUninterruptibly(2, TimeUnit.SECONDS);
      online = ReplicationTable.isOnline(c);
    }
    assertTrue("Replication table was not online", online);
  }

  // junit isn't a dependency
  private void assertTrue(String string, boolean test) {
    if (!test)
      throw new RuntimeException(string);
  }
}
