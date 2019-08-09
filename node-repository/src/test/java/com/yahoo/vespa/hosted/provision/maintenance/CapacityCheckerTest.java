package com.yahoo.vespa.hosted.provision.maintenance;

import com.yahoo.config.provision.*;
import com.yahoo.vespa.hosted.provision.Node;
import com.yahoo.vespa.hosted.provision.node.Allocation;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.junit.Assert.*;

/**
 * @author mgimle
 */
public class CapacityCheckerTest {
    private CapacityCheckerTester tester;

    @Before
    public void setup() {
        tester = new CapacityCheckerTester();
    }

    @Test
    public void testWithRealData() throws IOException {
        String path = "./src/test/resources/zookeeper_dump.json";

        tester.cleanRepository();
        tester.restoreNodeRepositoryFromJsonFile(Paths.get(path));
        var failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        assertTrue(tester.nodeRepository.getNodes(NodeType.host).containsAll(failurePath.get().hostsCausingFailure));
    }

    // TODO:: Move this test case to follow the searcher part of the CapacityChecker when it's moved to closed source
    @Test
    public void testNodeAlloc() throws IOException {
        String path = "./src/test/resources/zookeeper_dump.json";
        tester.cleanRepository();
        tester.restoreNodeRepositoryFromJsonFile(Paths.get(path));

        var hostsToRemove = Set.of(
                "-86489022",
                "887692099",
                "-2027478477",
                "1229740191",
                "1867043290",
                "2047882875",
                "1207107937",
                "-1475643347",
                "1444436682",
                "-156463203",
                "-1041720612"
        );

        var removed = tester.nodeRepository.getNodes(NodeType.host).stream()
                .filter(h -> hostsToRemove.contains(h.hostname()))
                .flatMap(host -> tester.nodeRepository.removeRecursively(host, true).stream())
                .collect(Collectors.toList());

        var removedHosts   = removed.stream().filter(n -> n.type() == NodeType.host).collect(Collectors.toList());
        var removedTenants = removed.stream().filter(n -> n.type() == NodeType.tenant).collect(Collectors.toList());

        System.out.println("Removed Hosts   : " + removedHosts.size());
        for (var h : removedHosts) {
            System.out.println(h.hostname());
        }
        System.out.println("Removed Tenants : " + removedTenants.size());

        Set<CapacityChecker.NewNode> newNodes = removedTenants.stream()
                .filter(n -> n.allocation().isPresent())
                .map(n -> new CapacityChecker.NewNode(n.flavor().resources(), n.allocation().orElseThrow()))
                .collect(Collectors.toSet());
        Map<Node, List<Node>> nodeChildren = tester.capacityChecker.nodeChildren;
        Set<CapacityChecker.Host> hosts = tester.nodeRepository.getNodes(NodeType.host).stream()
                .filter(h -> !removedHosts.contains(h))
                .map(h -> new CapacityChecker.Host(
                        h.hostname(),
                        h.flavor().resources(),
                        nodeChildren.get(h).stream()
                                .filter(n -> n.allocation().isPresent())
                                .map(CapacityChecker.NewNode::from)
                                .collect(Collectors.toSet()),
                        newNodes))
                .collect(Collectors.toSet());

        List<Long>  l = new ArrayList<>();
        for (var n : newNodes.stream().map(n -> new CapacityChecker.Flexiblity(n, hosts)).collect(Collectors.toSet())) {
            l.add(Integer.toUnsignedLong(n.possibleHosts.size()));
        }
        System.out.println(l.stream().reduce(1L, (a, b) -> Long.max(a, 1L) * Long.max(b, 1L)));

        System.out.println("%%%%%%%%%%%%%%%%%%%");
        CapacityChecker.Searcher searcher = new CapacityChecker.Searcher(4);
        searcher.search(newNodes, hosts).ifPresentOrElse(r ->
                {
                    String outstr = r.toCompleteInstructions();
                    System.out.println(outstr);
                    for (var node : newNodes) {
                        assertEquals(String.format("Expected %s to show up exactly once.", node.allocation.toString()), 1, StringUtils.countMatches(outstr, node.allocation.toString()));
                    }
                    System.out.println("Simple version;");
                    System.out.println(r.toSimplifiedInstructions());
                },
                () -> System.out.println("Did NOT find a path!"));


        System.out.println("%%%%%%%%%%%%%%%%%%%");

        System.out.println("Hosts   : " + tester.nodeRepository.getNodes(NodeType.host).size());
        System.out.println("Tenants : " + tester.nodeRepository.getNodes(NodeType.tenant).size());
    }

    @Test
    public void testOvercommittedHosts() {
        tester.createNodes(7, 4,
               10, new NodeResources(-1, 10, 100), 10,
                0, new NodeResources(1, 10, 100), 10);
        int overcommittedHosts = tester.capacityChecker.findOvercommittedHosts().size();
        assertEquals(tester.nodeRepository.getNodes(NodeType.host).size(), overcommittedHosts);
    }

    @Test
    public void testEdgeCaseFailurePaths() {
        tester.createNodes(1, 1,
                0, new NodeResources(1, 10, 100), 10,
                0, new NodeResources(1, 10, 100), 10);
        var failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertFalse("Computing worst case host loss with no hosts should return an empty optional.", failurePath.isPresent());

        // Odd edge case that should never be able to occur in prod
        tester.createNodes(1, 10,
                10, new NodeResources(10, 1000, 10000), 100,
                1, new NodeResources(10, 1000, 10000), 100);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        assertTrue("Computing worst case host loss if all hosts have to be removed should result in an non-empty failureReason with empty nodes.",
                failurePath.get().failureReason.tenant.isEmpty() && failurePath.get().failureReason.host.isEmpty());
        assertEquals(tester.nodeRepository.getNodes(NodeType.host).size(), failurePath.get().hostsCausingFailure.size());

        tester.createNodes(3, 30,
                10, new NodeResources(0, 0, 10000), 1000,
                0, new NodeResources(0, 0, 0), 0);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("When there are multiple lacking resources, all failures are multipleReasonFailures",
                    failureReasons.size(), failureReasons.multipleReasonFailures().size());
            assertEquals(0, failureReasons.singularReasonFailures().size());
        } else fail();
    }

    @Test
    public void testIpFailurePaths() {
        tester.createNodes(1, 10,
                10, new NodeResources(10, 1000, 10000), 1,
                10, new NodeResources(10, 1000, 10000), 1);
        var failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("All failures should be due to hosts having a lack of available ip addresses.",
                    failureReasons.singularReasonFailures().insufficientAvailableIps(), failureReasons.size());
        } else fail();

    }

    @Test
    public void testNodeResourceFailurePaths() {
        tester.createNodes(1, 10,
                10, new NodeResources(1, 100, 1000), 100,
                10, new NodeResources(0, 100, 1000), 100);
        var failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("All failures should be due to hosts lacking cpu cores.",
                    failureReasons.singularReasonFailures().insufficientVcpu(), failureReasons.size());
        } else fail();

        tester.createNodes(1, 10,
                10, new NodeResources(10, 1, 1000), 100,
                10, new NodeResources(10, 0, 1000), 100);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("All failures should be due to hosts lacking memory.",
                    failureReasons.singularReasonFailures().insufficientMemoryGb(), failureReasons.size());
        } else fail();

        tester.createNodes(1, 10,
                10, new NodeResources(10, 100, 10), 100,
                10, new NodeResources(10, 100, 0), 100);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("All failures should be due to hosts lacking disk space.",
                    failureReasons.singularReasonFailures().insufficientDiskGb(), failureReasons.size());
        } else fail();

        int emptyHostsWithSlowDisk = 10;
        tester.createNodes(1, 10, List.of(new NodeResources(1, 10, 100)),
                10, new NodeResources(0, 0, 0), 100,
                10, new NodeResources(10, 1000, 10000, NodeResources.DiskSpeed.slow), 100);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("All empty hosts should be invalid due to having incompatible disk speed.",
                    failureReasons.singularReasonFailures().incompatibleDiskSpeed(), emptyHostsWithSlowDisk);
        } else fail();

    }


    @Test
    public void testParentHostPolicyIntegrityFailurePaths() {
        tester.createNodes(1, 1,
                10, new NodeResources(1, 100, 1000), 100,
                10, new NodeResources(10, 1000, 10000), 100);
        var failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertEquals("With only one type of tenant, all failures should be due to violation of the parent host policy.",
                    failureReasons.singularReasonFailures().violatesParentHostPolicy(), failureReasons.size());
        } else fail();

        tester.createNodes(1, 2,
                10, new NodeResources(10, 100, 1000), 1,
                0, new NodeResources(0, 0, 0), 0);
        failurePath = tester.capacityChecker.worstCaseHostLossLeadingToFailure();
        assertTrue(failurePath.isPresent());
        if (failurePath.get().failureReason.tenant.isPresent()) {
            var failureReasons = failurePath.get().failureReason.allocationFailures;
            assertNotEquals("Fewer distinct children than hosts should result in some parent host policy violations.",
                    failureReasons.size(), failureReasons.singularReasonFailures().violatesParentHostPolicy());
            assertNotEquals(0, failureReasons.singularReasonFailures().violatesParentHostPolicy());
        } else fail();
    }
}


