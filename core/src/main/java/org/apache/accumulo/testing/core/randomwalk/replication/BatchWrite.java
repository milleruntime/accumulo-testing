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

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.testing.core.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.core.randomwalk.State;
import org.apache.accumulo.testing.core.randomwalk.Test;

public class BatchWrite extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String sourceTable = state.getString("source");
    Connector c = env.getAccumuloConnector();
    int ROWS = state.getInteger("rows");
    int COLS = state.getInteger("cols");

    // write some checkable data
    BatchWriter bw = c.createBatchWriter(sourceTable, null);
    for (int row = 0; row < ROWS; row++) {
      Mutation m = new Mutation(Utils.itos(row));
      for (int col = 0; col < COLS; col++) {
        m.put("", Utils.itos(col), "");
      }
      bw.addMutation(m);
    }
    log.debug("Close batch writer after adding " + ROWS + " mutations");
    bw.close();

    // zookeeper propagation wait
    UtilWaitThread.sleepUninterruptibly(5, TimeUnit.SECONDS);

    // attempt to force the WAL to roll so replication begins
    final Set<String> origRefs = c.replicationOperations().referencedFiles(sourceTable);

    // write some data we will ignore
    while (true) {
      final Set<String> updatedFileRefs = c.replicationOperations().referencedFiles(sourceTable);
      updatedFileRefs.retainAll(origRefs);
      log.debug("updateFileRefs size " + updatedFileRefs.size());
      if (updatedFileRefs.isEmpty()) {
        break;
      }
      bw = c.createBatchWriter(sourceTable, null);
      for (int row = 0; row < ROWS; row++) {
        Mutation m = new Mutation(Utils.itos(row));
        for (int col = 0; col < COLS; col++) {
          m.put("ignored", Utils.itos(col), "");
        }
        bw.addMutation(m);
      }
      bw.close();
    }
  }
}
