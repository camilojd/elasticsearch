/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class StoreStats implements Streamable, ToXContent {

    private long sizeInBytes;

    private ImmutableOpenMap<String, Long> detail = ImmutableOpenMap.of();

    private long throttleTimeInNanos;

    private static ImmutableOpenMap<String, String> descriptions = ImmutableOpenMap.<String, String>builder()
            .fPut("si", "Segment Info")
            .fPut("fnm", "Fields")
            .fPut("fdx", "Field Index")
            .fPut("fdt", "Field Data")
            .fPut("tim", "Term Dictionary")
            .fPut("tip", "Term Index")
            .fPut("doc", "Frequencies")
            .fPut("pos", "Positions")
            .fPut("pay", "Payloads")
            .fPut("nvd", "Norms")
            .fPut("nvm", "Norms")
            .fPut("dvd", "DocValues")
            .fPut("dvm", "DocValues")
            .fPut("tvx", "Term Vector Index")
            .fPut("tvd", "Term Vector Documents")
            .fPut("tvf", "Term Vector Fields")
            .fPut("liv", "Live Documents")
            .build();

    public StoreStats() {

    }

    public StoreStats(long sizeInBytes, ImmutableOpenMap<String, Long> detail, long throttleTimeInNanos) {
        this.sizeInBytes = sizeInBytes;
        this.detail = detail;
        this.throttleTimeInNanos = throttleTimeInNanos;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;

        ImmutableOpenMap.Builder<String, Long> map = ImmutableOpenMap.builder(this.detail);
        for (Iterator<ObjectObjectCursor<String, Long>> it = stats.detail.iterator(); it.hasNext();) {
            ObjectObjectCursor<String, Long> entry = it.next();
            if (map.containsKey(entry.key)) {
                long oldValue = map.get(entry.key);
                map.put(entry.key, oldValue + entry.value);
            } else {
                map.put(entry.key, entry.value);
            }
        }

        this.detail = map.build();
        throttleTimeInNanos += stats.throttleTimeInNanos;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos);
    }

    public TimeValue getThrottleTime() {
        return throttleTime();
    }

    public static StoreStats readStoreStats(StreamInput in) throws IOException {
        StoreStats store = new StoreStats();
        store.readFrom(in);
        return store;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        throttleTimeInNanos = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            int size = in.readVInt();
            ImmutableOpenMap.Builder<String, Long> map = ImmutableOpenMap.builder(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                Long value = in.readLong();
                map.put(key, value);
            }
            detail = map.build();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeVLong(throttleTimeInNanos);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeVInt(detail.size());
            for (Iterator<ObjectObjectCursor<String, Long>> it = detail.iterator(); it.hasNext();) {
                ObjectObjectCursor<String, Long> entry = it.next();
                out.writeString(entry.key);
                out.writeLong(entry.value);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, sizeInBytes);
        builder.timeValueField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.startObject(Fields.DETAIL);
        for (Iterator<ObjectObjectCursor<String, Long>> it = detail.iterator(); it.hasNext();) {
            ObjectObjectCursor<String, Long> entry = it.next();
            builder.startObject(entry.key);
            builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, entry.value);
            builder.field(Fields.DESCRIPTION, descriptions.getOrDefault(entry.key, "Others"));
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString STORE = new XContentBuilderString("store");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
        static final XContentBuilderString THROTTLE_TIME = new XContentBuilderString("throttle_time");
        static final XContentBuilderString THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("throttle_time_in_millis");
        static final XContentBuilderString DETAIL = new XContentBuilderString("detail");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
    }
}
