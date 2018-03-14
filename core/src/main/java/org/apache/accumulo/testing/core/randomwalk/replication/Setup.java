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

import static org.apache.accumulo.core.conf.Property.MASTER_REPLICATION_SCAN_INTERVAL;
import static org.apache.accumulo.core.conf.Property.REPLICATION_NAME;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEERS;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEER_PASSWORD;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEER_USER;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_ASSIGNMENT_SLEEP;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_PROCESSOR_DELAY;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_PROCESSOR_PERIOD;
import static org.apache.accumulo.core.conf.Property.TABLE_REPLICATION;
import static org.apache.accumulo.core.conf.Property.TABLE_REPLICATION_TARGET;
import static org.apache.accumulo.server.replication.ReplicaSystemFactory.getPeerConfigurationValue;

import java.net.InetAddress;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.testing.core.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.core.randomwalk.State;
import org.apache.accumulo.testing.core.randomwalk.Test;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;

public class Setup extends Test {

  static String sourceTableName = null;
  static String destTableName = null;

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = env.getPid();
    long ts = System.currentTimeMillis();
    sourceTableName = String.format("repl_source_%s_%s_%d", hostname, pid, ts);
    destTableName = String.format("repl_dest_%s_%s_%d", hostname, pid, ts);
    log.info("Starting replication test on source table " + sourceTableName);
    int rows = Integer.parseInt(props.getProperty("rows", "1000"));
    int cols = Integer.parseInt(props.getProperty("cols", "50"));

    final Connector c = env.getAccumuloConnector();
    final Instance inst = c.getInstance();
    final String instName = inst.getInstanceName();
    final InstanceOperations iOps = c.instanceOperations();
    final TableOperations tOps = env.getAccumuloConnector().tableOperations();

    // Replicate to ourselves
    iOps.setProperty(REPLICATION_NAME.getKey(), instName);
    iOps.setProperty(REPLICATION_PEERS.getKey() + instName, getPeerConfigurationValue(AccumuloReplicaSystem.class, instName + "," + inst.getZooKeepers()));
    iOps.setProperty(REPLICATION_PEER_USER.getKey() + instName, env.getAccumuloUserName());
    iOps.setProperty(REPLICATION_PEER_PASSWORD.getKey() + instName, env.getAccumuloPassword());
    // Tweak some replication parameters to make the replication go faster
    iOps.setProperty(MASTER_REPLICATION_SCAN_INTERVAL.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_ASSIGNMENT_SLEEP.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_PROCESSOR_DELAY.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_PROCESSOR_PERIOD.getKey(), "1s");

    tOps.create(sourceTableName);
    tOps.create(destTableName);

    // Point the source to the destination
    String destID = tOps.tableIdMap().get(destTableName);
    tOps.setProperty(sourceTableName, TABLE_REPLICATION.getKey(), "true");
    tOps.setProperty(sourceTableName, TABLE_REPLICATION_TARGET.getKey() + instName, destID);

    state.set("source", sourceTableName);
    state.set("dest", destTableName);
    state.set("rows", rows);
    state.set("cols", cols);
    state.set("replicationSuccess", "true");
  }
}
