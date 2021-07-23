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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.tsdb.fakedb;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class FakeDB implements IDatabase {

    @Override
    public void init() throws TsdbException {

    }

    @Override
    public void cleanup() throws TsdbException {

    }

    @Override
    public void close() throws TsdbException {

    }

    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {

    }

    @Override
    public Status insertOneBatch(Batch batch) {
        return new Status(true);
    }

    @Override
    public Status insertOneSensorBatch(Batch batch) {
        return new Status(true);
    }


    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        return new Status(true, 0);
    }

    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        return new Status(true, null, null);
    }

    @Override
    public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
        return null;
    }

    @Override
    public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
        return null;
    }
}

