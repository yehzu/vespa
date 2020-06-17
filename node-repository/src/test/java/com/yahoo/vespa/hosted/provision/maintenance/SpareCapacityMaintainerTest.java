// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ClusterMembership;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.DockerImage;
import com.yahoo.config.provision.Environment;
import com.yahoo.config.provision.Flavor;
import com.yahoo.config.provision.NodeFlavors;
import com.yahoo.config.provision.NodeResources;
import com.yahoo.config.provision.NodeType;
import com.yahoo.config.provision.RegionName;
import com.yahoo.config.provision.Zone;
import com.yahoo.test.ManualClock;
import com.yahoo.transaction.NestedTransaction;
import com.yahoo.vespa.curator.mock.MockCurator;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.NodeRepository;
import com.yahoo.vespa.hosted.provision.node.Agent;
import com.yahoo.vespa.hosted.provision.node.IP;
import com.yahoo.vespa.hosted.provision.provisioning.EmptyProvisionServiceProvider;
import com.yahoo.vespa.hosted.provision.provisioning.FlavorConfigBuilder;
import com.yahoo.vespa.hosted.provision.testutils.MockDeployer;
import com.yahoo.vespa.hosted.provision.testutils.MockNameResolver;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * @author bratseth
 */
public class SpareCapacityMaintainerTest {

    @Test
    public void testEmpty() {
        var tester = new SpareCapacityMaintainerTester();
        tester.maintainer.maintain();
        assertEquals(0, tester.deployer.redeployments);
        assertEquals(0, tester.nodeRepository.list().retired().size());
    }

    @Test
    public void testOneSpare() {
        var tester = new SpareCapacityMaintainerTester();
        tester.addHosts(2, new NodeResources(10, 100, 1000, 1));
        tester.addNodes(0, 1, new NodeResources(10, 100, 1000, 1), 0);
        tester.maintainer.maintain();
        assertEquals(0, tester.deployer.redeployments);
        assertEquals(0, tester.nodeRepository.list().retired().size());
        assertEquals(1, tester.metric.values.get("spareHostCapacity"));
    }

    @Test
    public void testTwoSpares() {
        var tester = new SpareCapacityMaintainerTester();
        tester.addHosts(3, new NodeResources(10, 100, 1000, 1));
        tester.addNodes(0, 1, new NodeResources(10, 100, 1000, 1), 0);
        tester.maintainer.maintain();
        assertEquals(0, tester.deployer.redeployments);
        assertEquals(0, tester.nodeRepository.list().retired().size());
        assertEquals(2, tester.metric.values.get("spareHostCapacity"));
    }

    @Test
    public void testNoSpares() {
        var tester = new SpareCapacityMaintainerTester();
        tester.addHosts(2, new NodeResources(10, 100, 1000, 1));
        tester.addNodes(0, 2, new NodeResources(10, 100, 1000, 1), 0);
        tester.maintainer.maintain();
        assertEquals(0, tester.deployer.redeployments);
        assertEquals(0, tester.nodeRepository.list().retired().size());
        assertEquals(0, tester.metric.values.get("spareHostCapacity"));
    }

    @Test
    public void testAllWorksAsSpares() {
        var tester = new SpareCapacityMaintainerTester();
        tester.addHosts(4, new NodeResources(10, 100, 1000, 1));
        tester.addNodes(0, 2, new NodeResources(5, 50, 500, 0.5), 0);
        tester.addNodes(1, 2, new NodeResources(5, 50, 500, 0.5), 2);
        tester.maintainer.maintain();
        assertEquals(0, tester.deployer.redeployments);
        assertEquals(0, tester.nodeRepository.list().retired().size());
        assertEquals(2, tester.metric.values.get("spareHostCapacity"));
    }

    @Test
    public void testMoveIsNeededToHaveSpares() {
        // Moving application id 1 and 2 to the same nodes frees up spares for application 0
        var tester = new SpareCapacityMaintainerTester();
        tester.addHosts(6, new NodeResources(10, 100, 1000, 1));
        tester.addNodes(0, 2, new NodeResources(10, 100, 1000, 1), 0);
        tester.addNodes(1, 2, new NodeResources(5, 50, 500, 0.5), 2);
        tester.addNodes(2, 2, new NodeResources(5, 50, 500, 0.5), 4);
        tester.maintainer.maintain();
        assertEquals(1, tester.deployer.redeployments);
        assertEquals(1, tester.nodeRepository.list().retired().size());
        assertEquals(1, tester.metric.values.get("spareHostCapacity"));

        // Maintaining again is a no-op since the node to move is already retired
        tester.maintainer.maintain();
        assertEquals(1, tester.deployer.redeployments);
        assertEquals(1, tester.nodeRepository.list().retired().size());
        assertEquals(1, tester.metric.values.get("spareHostCapacity"));
    }

    private static class SpareCapacityMaintainerTester {

        NodeRepository nodeRepository;
        MockDeployer deployer;
        TestMetric metric = new TestMetric();
        SpareCapacityMaintainer maintainer;
        private int hostIndex = 0;
        private int nodeIndex = 0;

        private SpareCapacityMaintainerTester() {
            NodeFlavors flavors = new NodeFlavors(new FlavorConfigBuilder().build());
            nodeRepository = new NodeRepository(flavors,
                                                new EmptyProvisionServiceProvider().getHostResourcesCalculator(),
                                                new MockCurator(),
                                                new ManualClock(),
                                                new Zone(Environment.prod, RegionName.from("us-east-3")),
                                                new MockNameResolver().mockAnyLookup(),
                                                DockerImage.fromString("docker-registry.domain.tld:8080/dist/vespa"), true, false);
            deployer = new MockDeployer(nodeRepository);
            maintainer = new SpareCapacityMaintainer(deployer, nodeRepository, metric, Duration.ofMinutes(1));
        }

        private void addHosts(int count, NodeResources resources) {
            List<Node> hosts = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Node host = nodeRepository.createNode("host" + hostIndex,
                                                      "host" + hostIndex + ".yahoo.com",
                                                      ipConfig(hostIndex + nodeIndex, true),
                                                      Optional.empty(),
                                                      new Flavor(resources),
                                                      Optional.empty(),
                                                      NodeType.host);
                hosts.add(host);
                hostIndex++;
            }
            hosts = nodeRepository.addNodes(hosts, Agent.system);
            hosts = nodeRepository.setReady(hosts, Agent.system, "Test");
            var transaction = new NestedTransaction();
            nodeRepository.activate(hosts, transaction);
            transaction.commit();
        }

        private void addNodes(int id, int count, NodeResources resources, int hostOffset) {
            List<Node> nodes = new ArrayList<>();
            ApplicationId application = ApplicationId.from("tenant" + id, "application" + id, "default");
            for (int i = 0; i < count; i++) {
                ClusterMembership membership = ClusterMembership.from(ClusterSpec.specification(ClusterSpec.Type.content, ClusterSpec.Id.from("cluster" + id))
                                                                                 .group(ClusterSpec.Group.from(0))
                                                                                 .vespaVersion("7")
                                                                                 .build(),
                                                                      i);
                Node node = nodeRepository.createNode("node" + nodeIndex,
                                                      "node" + nodeIndex + ".yahoo.com",
                                                      ipConfig(hostIndex + nodeIndex, false),
                                                      Optional.of("host" + ( hostOffset + i) + ".yahoo.com"),
                                                      new Flavor(resources),
                                                      Optional.empty(),
                                                      NodeType.tenant);
                node = node.allocate(application, membership, node.resources(), Instant.now());
                nodes.add(node);
                nodeIndex++;
            }
            nodes = nodeRepository.addNodes(nodes, Agent.system);
            for (int i = 0; i < count; i++) {
                Node node = nodes.get(i);
                ClusterMembership membership = ClusterMembership.from(ClusterSpec.specification(ClusterSpec.Type.content, ClusterSpec.Id.from("cluster" + id))
                                                                                 .group(ClusterSpec.Group.from(0))
                                                                                 .vespaVersion("7")
                                                                                 .build(),
                                                                      i);
                node = node.allocate(application, membership, node.resources(), Instant.now());
                nodes.set(i, node);
            }
            nodes = nodeRepository.reserve(nodes);
            var transaction = new NestedTransaction();
            nodes = nodeRepository.activate(nodes, transaction);
            transaction.commit();
        }

        private IP.Config ipConfig(int id, boolean host) {
            return new IP.Config(Set.of(String.format("%04X::%04X", id, 0)),
                                 host ? IntStream.range(0, 10)
                                                 .mapToObj(n -> String.format("%04X::%04X", id, n))
                                                 .collect(Collectors.toSet())
                                      : Set.of());
        }

        private void dumpState() {
            for (Node host : nodeRepository.list().hosts().asList()) {
                System.out.println("Host " + host.hostname() + " " + host.resources());
                for (Node node : nodeRepository.list().childrenOf(host).asList())
                    System.out.println("    Node " + node.hostname() + " " + node.resources() + " allocation " +node.allocation());
            }
        }

    }

}
