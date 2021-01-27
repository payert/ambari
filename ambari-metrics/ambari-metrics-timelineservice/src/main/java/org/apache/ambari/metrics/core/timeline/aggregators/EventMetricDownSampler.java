/**
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

package org.apache.ambari.metrics.core.timeline.aggregators;


import org.apache.ambari.metrics.core.timeline.PhoenixHBaseAccessor;
import org.apache.ambari.metrics.core.timeline.query.Condition;
import org.apache.ambari.metrics.core.timeline.query.DefaultCondition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.ambari.metrics.core.timeline.query.PhoenixTransactSQL.EVENT_DOWNSAMPLER_CLUSTER_METRIC_SELECT_SQL;
import static org.apache.ambari.metrics.core.timeline.query.PhoenixTransactSQL.EVENT_DOWNSAMPLER_HOST_METRIC_SELECT_SQL;
import static org.apache.ambari.metrics.core.timeline.query.PhoenixTransactSQL.METRICS_CLUSTER_AGGREGATE_TABLE_NAME;

public class EventMetricDownSampler implements CustomDownSampler{

  private final String metricPatterns;
  private final PhoenixHBaseAccessor hBaseAccessor;
  private static final Log LOG = LogFactory.getLog(EventMetricDownSampler.class);

  public static EventMetricDownSampler fromConfig(Map<String, String> conf, PhoenixHBaseAccessor hBaseAccessor) {
    String metricPatterns = conf.get(DownSamplerUtils.downSamplerConfigPrefix + DownSamplerUtils.eventDownSamplerKey + "." +
      DownSamplerUtils.downSamplerMetricPatternsConfig);

    return new EventMetricDownSampler(metricPatterns, hBaseAccessor);
  }

  public EventMetricDownSampler(String metricPatterns, PhoenixHBaseAccessor hBaseAccessor) {
    this.metricPatterns = metricPatterns;
    this.hBaseAccessor = hBaseAccessor;
  }

  @Override
  public boolean validateConfigs() {
    return true;
  }

  @Override
  public List<String> prepareDownSamplingStatement(Long startTime, Long endTime, String tableName) {
    List<String> stmts = new ArrayList<>();
    List<String> metricPatternList = Arrays.asList(metricPatterns.split(","));

    String aggregateColumnName = "METRIC_COUNT";

    if (tableName.equals(METRICS_CLUSTER_AGGREGATE_TABLE_NAME)) {
      aggregateColumnName = "HOSTS_COUNT";
    }

    for (String metricPattern : metricPatternList) {
      String metricPatternClause = "'" + metricPattern + "'";
      if (tableName.contains("RECORD")) {
        stmts.add(String.format(EVENT_DOWNSAMPLER_HOST_METRIC_SELECT_SQL,
          endTime, tableName, metricPatternClause,
          startTime, endTime));
      } else {

        Collection<byte[]> metricUuids = getMetricUuids(metricPattern);

/*        SELECT METRIC_NAME, APP_ID, " +
        "INSTANCE_ID, %s AS SERVER_TIME, UNITS, SUM(METRIC_SUM), SUM(%s), " +
            "MAX(METRIC_MAX), MIN(METRIC_MIN) FROM %s WHERE METRIC_NAME LIKE %s AND SERVER_TIME > %s AND " +
            "SERVER_TIME <= %s GROUP BY METRIC_NAME, APP_ID, INSTANCE_ID, UNITS";

            "SELECT METRIC_NAME, APP_ID, " +
    "INSTANCE_ID, %s AS SERVER_TIME, UNITS, SUM(METRIC_SUM), SUM(%s), " +
    "MAX(METRIC_MAX), MIN(METRIC_MIN) FROM %s WHERE METRIC_NAME LIKE %s AND SERVER_TIME > %s AND " +
    "SERVER_TIME <= %s GROUP BY METRIC_NAME, APP_ID, INSTANCE_ID, UNITS";

 */


        String statement = "SELECT UUID, %s AS SERVER_TIME, SUM(METRIC_SUM), SUM(METRIC_COUNT), MAX(METRIC_MAX), MIN(METRIC_MIN) " +
            "FROM %s WHERE SERVER_TIME > %s AND " +
            "SERVER_TIME <= %s GROUP BY UUID";

        stmts.add(String.format(EVENT_DOWNSAMPLER_CLUSTER_METRIC_SELECT_SQL,
            endTime, tableName, startTime, endTime));

/*        stmts.add(String.format(EVENT_DOWNSAMPLER_CLUSTER_METRIC_SELECT_SQL,
          endTime, tableName, metricPatternClause,
          startTime, endTime));

 */
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Downsampling Stmt: " + stmts.toString());
    }
    return stmts;
  }

  @Override
  public List<Condition> prepareDownSamplingCondition(Long startTime, Long endTime, String tableName) {
    List<Condition> conditions = new ArrayList<>();
    String[] metricPatternList = metricPatterns.split(",");

    String aggregateColumnName = "METRIC_COUNT";
    if (tableName.equals(METRICS_CLUSTER_AGGREGATE_TABLE_NAME)) {
      aggregateColumnName = "HOSTS_COUNT";
    }

    for (String metricPattern : metricPatternList) {
      // METRIC_RECORD_UUID -> METRIC_RECORD_MINUTE_UUID or METRIC_AGGREGATE_UUID -> METRIC_AGGREGATE_MINUTE_UUID
      String query = String.format("SELECT UUID, %s AS SERVER_TIME, SUM(METRIC_SUM), SUM(%s), MAX(METRIC_MAX), MIN(METRIC_MIN) FROM %s", endTime, aggregateColumnName, tableName);

      List<byte[]> metricUuids = getMetricUuids(metricPattern);
      DefaultCondition c = new DefaultCondition(metricUuids, null, null, null, null, startTime, endTime, null, null, true);
      c.setStatement(query);
      conditions.add(c);
    }

    return conditions;
  }

  private List<byte[]> getMetricUuids(String metricPattern) {
    List<byte[]> uuids = new ArrayList<>();
    String query = "SELECT UUID, METRIC_NAME from METRICS_METADATA_UUID where METRIC_NAME like ?";
    try (Connection con = hBaseAccessor.getConnection();
          PreparedStatement stmt = con.prepareStatement(query)) {
      stmt.setString(1, metricPattern);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        byte[] uuid = rs.getBytes("UUID");
        String metricName = rs.getString("METRIC_NAME");

        uuids.add(uuid);
        LOG.debug("Metric name [" + metricName + "] and UUID queried for pattern : " + metricPattern);
      }
    } catch (SQLException throwables) {
      throwables.printStackTrace();
      //TODO
    }
    return uuids;
  }
}