// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.autoscale;

import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.applications.Cluster;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A series of metric snapshots for all nodes in a cluster
 *
 * @author bratseth
 */
public class ClusterTimeseries {

    private final List<Node> clusterNodes;

    final int measurementCount;
    final int measurementCountWithoutStale;
    final int measurementCountWithoutStaleOutOfService;
    final int measurementCountWithoutStaleOutOfServiceUnstable;

    /** The measurements for all nodes in this snapshot */
    private final List<NodeTimeseries> allNodeTimeseries;

    public ClusterTimeseries(Cluster cluster, List<Node> clusterNodes, MetricsDb db, NodeRepository nodeRepository) {
        this.clusterNodes = clusterNodes;
        ClusterSpec clusterSpec = clusterNodes.get(0).allocation().get().membership().cluster();
        var timeseries = db.getNodeTimeseries(nodeRepository.clock().instant().minus(Autoscaler.scalingWindow(clusterSpec)),
                                              clusterNodes.stream().map(Node::hostname).collect(Collectors.toSet()));

        measurementCount = timeseries.stream().mapToInt(m -> m.size()).sum();

        if (cluster.lastScalingEvent().isPresent())
            timeseries = filter(timeseries, snapshot -> snapshot.generation() >= cluster.lastScalingEvent().get().generation());
        measurementCountWithoutStale = timeseries.stream().mapToInt(m -> m.size()).sum();

        timeseries = filter(timeseries, snapshot -> snapshot.inService());
        measurementCountWithoutStaleOutOfService = timeseries.stream().mapToInt(m -> m.size()).sum();

        timeseries = filter(timeseries, snapshot -> snapshot.stable());
        measurementCountWithoutStaleOutOfServiceUnstable = timeseries.stream().mapToInt(m -> m.size()).sum();

        this.allNodeTimeseries = timeseries;
    }

    /**
     * Returns the instant of the oldest metric to consider for each node, or an empty map if metrics from the
     * entire (max) window should be considered.
     */
    private Map<String, Instant> metricStartTimes(Cluster cluster,
                                                  List<Node> clusterNodes,
                                                  List<NodeTimeseries> allNodeTimeseries,
                                                  NodeRepository nodeRepository) {
        if (cluster.lastScalingEvent().isEmpty()) return Map.of();

        var deployment = cluster.lastScalingEvent().get();
        Map<String, Instant> startTimePerHost = new HashMap<>();
        for (Node node : clusterNodes) {
            startTimePerHost.put(node.hostname(), nodeRepository.clock().instant()); // Discard all unless we can prove otherwise
            var nodeTimeseries = allNodeTimeseries.stream().filter(m -> m.hostname().equals(node.hostname())).findAny();
            if (nodeTimeseries.isPresent()) {
                var firstMeasurementOfCorrectGeneration =
                        nodeTimeseries.get().asList().stream()
                                                     .filter(m -> m.generation() >= deployment.generation())
                                                     .findFirst();
                if (firstMeasurementOfCorrectGeneration.isPresent()) {
                    startTimePerHost.put(node.hostname(), firstMeasurementOfCorrectGeneration.get().at());
                }
            }
        }
        return startTimePerHost;
    }

    /** Returns the average number of measurements per node */
    public int measurementsPerNode() {
        int measurementCount = allNodeTimeseries.stream().mapToInt(m -> m.size()).sum();
        return measurementCount / clusterNodes.size();
    }

    /** Returns the number of nodes measured in this */
    public int nodesMeasured() {
        return allNodeTimeseries.size();
    }

    /** Returns the average load of this resource in this */
    public double averageLoad(Resource resource) {
        int measurementCount = allNodeTimeseries.stream().mapToInt(m -> m.size()).sum();
        double measurementSum = allNodeTimeseries.stream().flatMap(m -> m.asList().stream()).mapToDouble(m -> value(resource, m)).sum();
        return measurementSum / measurementCount;
    }

    private double value(Resource resource, MetricSnapshot snapshot) {
        switch (resource) {
            case cpu: return snapshot.cpu();
            case memory: return snapshot.memory();
            case disk: return snapshot.disk();
            default: throw new IllegalArgumentException("Got an unknown resource " + resource);
        }
    }

    private List<NodeTimeseries> filter(List<NodeTimeseries> timeseries, Predicate<MetricSnapshot> filter) {
        return timeseries.stream().map(nodeTimeseries -> nodeTimeseries.filter(filter)).collect(Collectors.toList());
    }

}
