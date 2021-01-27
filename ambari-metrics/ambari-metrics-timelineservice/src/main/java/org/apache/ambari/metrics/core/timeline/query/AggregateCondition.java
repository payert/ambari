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
package org.apache.ambari.metrics.core.timeline.query;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.metrics2.sink.timeline.Precision;

//TODO  Rename it!
public class AggregateCondition extends DefaultCondition {

  private boolean doUpdate = false;

  public AggregateCondition(List<byte[]> uuids, Long startTime, Long endTime) {
    super(uuids, null, null, null, null, startTime, endTime, null, null, true);
    super.setNoLimit();
  }

  public void setDoUpdate(boolean doUpdate) {
    this.doUpdate = doUpdate;
  }

  @Override
  public boolean doUpdate() {
    return this.doUpdate;
  }

  @Override
  public StringBuilder getConditionClause() {
    return super.getConditionClause().append(" GROUP BY UUID");
  }

  @Override
  protected boolean appendUuidClause(StringBuilder sb) {
    boolean appendConjunction = false;

    if (CollectionUtils.isNotEmpty(uuids)) {
      // Put a '(' first
      sb.append("(");

      //IN clause
      // UUID (NOT) IN (?,?,?,?)
      if (CollectionUtils.isNotEmpty(uuids)) {
        sb.append("UUID");
        if (uuidNotCondition) {
          sb.append(" NOT");
        }
        sb.append(" IN (");
        //Append ?,?,?,?
        for (int i = 0; i < uuids.size(); i++) {
          sb.append("?");
          if (i < uuids.size() - 1) {
            sb.append(", ");
          }
        }
        sb.append(")");
      }
      appendConjunction = true;
      sb.append(")");
    }

    return appendConjunction;
  }

  @Override
  public String getOrderByClause(boolean asc) {
    // No order by.
    return "";
  }
}
