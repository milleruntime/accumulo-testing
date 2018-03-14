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
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.testing.core.randomwalk.State;
import org.apache.accumulo.testing.core.randomwalk.Test;
import org.apache.accumulo.testing.core.randomwalk.RandWalkEnv;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class Split extends Test {
  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    final Connector c = env.getAccumuloConnector();
    final TableOperations tOps = c.tableOperations();
    final String sourceTable = state.getString("source");
    final String destTable = state.getString("dest");
    int ROWS = state.getInteger("rows");
    final String tables[] = new String[] {sourceTable, destTable};

    // Maybe split the tables
    Random rand = new Random(System.currentTimeMillis());
    for (String tableName : tables) {
      if (rand.nextBoolean()) {
        SortedSet<Text> splits = new TreeSet<>();
        for (int i = 1; i <= 9; i++) {
          splits.add(new Text(Utils.itos(i * (ROWS / 10))));
        }
        log.debug("Adding splits to " + tableName);
        tOps.addSplits(tableName, splits);
      }
    }
  }

}
