/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.socket;

import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket. The easiest way to
 * try this out is to open a text server (at port 12345) using the <i>netcat</i> tool via
 *
 * <pre>
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * </pre>
 *
 * <p>and run this example with the hostname and the port as arguments.
 */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final String[] ports;
        final boolean useSlotSharingGroups;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            ports = params.get("ports").split(",");
            useSlotSharingGroups = params.has("use-ssg");
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'SocketWindowWordCount "
                            + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
                            + "and port is the address of the text server");
            System.err.println(
                    "To start a simple text server, run 'netcat -l <port>' and "
                            + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        for (int i = 0; i < ports.length; i++) {
            SlotSharingGroup ssg = SlotSharingGroup.newBuilder(ports[i]).build();
            int port = Integer.valueOf(ports[i]);
            // get input data by connecting to the socket
            SingleOutputStreamOperator<String> text = env.socketTextStream(hostname, port, "\n");
            if (useSlotSharingGroups) {
                text = text.slotSharingGroup(ssg);
            }
            // parse the data, group it, window it, and aggregate the counts
            DataStream<WordWithCount> windowCounts =
                    text.name("port-" + port)
                            .assignTimestampsAndWatermarks(
                                    WatermarkStrategy.<String>forMonotonousTimestamps()
                                            .withTimestampAssigner(
                                                    TimestampAssignerSupplier.<String>of(
                                                            (event, ts) ->
                                                                    Integer.valueOf(
                                                                                    event.split(
                                                                                                    ",")[
                                                                                            0])
                                                                            * 1000)))
                            .flatMap(
                                    (FlatMapFunction<String, WordWithCount>)
                                            (value, out) -> {
                                                String[] parts = value.split(",");
                                                value = parts[1];
                                                String window = parts[0];
                                                for (String word : value.split("\\s")) {
                                                    out.collect(
                                                            new WordWithCount(
                                                                    port, window, word, 1L));
                                                }
                                            },
                                    Types.POJO(WordWithCount.class))
                            .name("flat-map-port-" + port)
                            .keyBy(value -> value.word)
                            .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                            .reduce(
                                    (a, b) ->
                                            new WordWithCount(
                                                    port, a.window, a.word, a.count + b.count))
                            .name("window port " + port)
                            .returns(WordWithCount.class);

            // print the results with a single thread, rather than in parallel
            windowCounts.print().name("print").setParallelism(1);

            if (useSlotSharingGroups) {
                env.registerSlotSharingGroup(ssg);
            }
        }

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /** Data type for words with count. */
    public static class WordWithCount {
        public int port;
        public String window;
        public String word;
        public long count;

        @SuppressWarnings("unused")
        public WordWithCount() {}

        public WordWithCount(int port, String window, String word, long count) {
            this.port = port;
            this.window = window;
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "P" + port + " W" + window + " -- " + word + " : " + count;
        }
    }
}
