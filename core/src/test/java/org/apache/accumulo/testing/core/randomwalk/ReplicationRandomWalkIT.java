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
package org.apache.accumulo.testing.core.randomwalk;

import static org.apache.accumulo.core.conf.Property.TSERV_ARCHIVE_WALOGS;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_MAX_SIZE;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.testing.core.randomwalk.replication.BatchWrite;
import org.apache.accumulo.testing.core.randomwalk.replication.Online;
import org.apache.accumulo.testing.core.randomwalk.replication.Setup;
import org.apache.accumulo.testing.core.randomwalk.replication.Verify;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ReplicationRandomWalkIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(TSERV_ARCHIVE_WALOGS, "false");
    cfg.setProperty(TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(TSERV_NATIVEMAP_ENABLED, "false");
    cfg.setNumTservers(1);
  }

  @Test(timeout = 5 * 60 * 1000)
  public void runReplicationRandomWalkStep() throws Exception {
    State state = new State();
    Properties props = new Properties();
    Setup s = new Setup();
    Online o = new Online();
    BatchWrite b = new BatchWrite();
    Verify v = new Verify();

    RandWalkEnv env = new RandWalkEnv(props) {
      @Override
      public String getAccumuloUserName() {
        return "root";
      }

      @Override
      public String getAccumuloPassword() {
        return ROOT_PASSWORD;
      }

      @Override
      public Connector getAccumuloConnector() throws AccumuloException, AccumuloSecurityException {
        return ReplicationRandomWalkIT.this.getConnector();
      }

    };
    s.visit(state, env, props);
    o.visit(state, env, props);
    b.visit(state, env, props);
    v.visit(state, env, props);
  }

}
