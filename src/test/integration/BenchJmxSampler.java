/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** One persistent JMX connection avoids starting JmxTool once per sample. */
public final class BenchJmxSampler {
    private static final String[] CONNECT_METRICS = {
            "source-record-poll-total", "source-record-write-total", "source-record-active-count"
    };
    private static final String[] PRODUCER_METRICS = {
            "record-queue-time-avg", "record-queue-time-max", "request-latency-avg",
            "request-latency-max", "waiting-threads", "record-error-total"
    };

    private BenchJmxSampler() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: BenchJmxSampler <jmx-port> <connector-name>");
        }
        JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + args[0] + "/jmxrmi");
        String connector = args[1];
        String producer = "connector-producer-" + connector + "-0";
        ObjectName connectQuery = new ObjectName("kafka.connect:type=source-task-metrics,*");
        ObjectName producerQuery = new ObjectName("kafka.producer:type=producer-metrics,*");
        System.out.println("epoch_s\tpolled\twritten\tactive\tqueue_avg_ms\tqueue_max_ms"
                + "\trequest_avg_ms\trequest_max_ms\twaiting_threads\terrors");

        boolean warned = false;
        while (true) {
            try (JMXConnector jmx = JMXConnectorFactory.connect(url)) {
                warned = false;
                MBeanServerConnection server = jmx.getMBeanServerConnection();
                while (true) {
                    ObjectName source = find(server.queryNames(connectQuery, null), "connector", connector,
                            "task", "0");
                    if (source != null) {
                        ObjectName writer = find(server.queryNames(producerQuery, null), "client-id", producer,
                                null, null);
                        String[] connect = read(server, source, CONNECT_METRICS);
                        String[] kafka = read(server, writer, PRODUCER_METRICS);
                        StringBuilder line = new StringBuilder(
                                String.format(Locale.ROOT, "%.3f", System.currentTimeMillis() / 1000.0));
                        for (String value : connect) {
                            line.append('\t').append(value);
                        }
                        for (String value : kafka) {
                            line.append('\t').append(value);
                        }
                        System.out.println(line);
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    throw e;
                }
                if (!warned) {
                    System.err.println("Waiting for worker JMX: " + e.getMessage());
                    warned = true;
                }
                Thread.sleep(1000);
            }
        }
    }

    private static ObjectName find(Set<ObjectName> names, String key, String value,
                                   String secondKey, String secondValue) {
        for (ObjectName name : names) {
            if (value.equals(property(name, key))
                    && (secondKey == null || secondValue.equals(property(name, secondKey)))) {
                return name;
            }
        }
        return null;
    }

    private static String property(ObjectName name, String key) {
        String value = name.getKeyProperty(key);
        return value != null && value.startsWith("\"") ? ObjectName.unquote(value) : value;
    }

    private static String[] read(MBeanServerConnection server, ObjectName name, String[] attributes)
            throws Exception {
        String[] values = new String[attributes.length];
        Map<String, Object> found = new HashMap<>();
        if (name != null) {
            AttributeList list = server.getAttributes(name, attributes);
            for (Object item : list) {
                Attribute attribute = (Attribute) item;
                found.put(attribute.getName(), attribute.getValue());
            }
        }
        for (int i = 0; i < attributes.length; i++) {
            Object value = found.get(attributes[i]);
            values[i] = value instanceof Number ? value.toString() : "-";
        }
        return values;
    }
}
