// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.deployment;

import com.yahoo.component.Version;
import com.yahoo.config.application.api.DeploymentSpec;
import com.yahoo.config.provision.AthenzDomain;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.HostName;
import com.yahoo.config.provision.SystemName;
import com.yahoo.config.provision.zone.ZoneApi;
import com.yahoo.config.provision.zone.ZoneId;
import com.yahoo.slime.ArrayTraverser;
import com.yahoo.slime.Inspector;
import com.yahoo.vespa.config.SlimeUtils;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.ConfigChangeActions;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RefeedAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.RestartAction;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.configserverbindings.ServiceInfo;
import com.yahoo.vespa.hosted.controller.api.identifiers.DeploymentId;
import com.yahoo.vespa.hosted.controller.api.integration.LogEntry;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.TesterCloud;
import com.yahoo.vespa.hosted.controller.api.integration.stubs.MockMailer;
import com.yahoo.vespa.hosted.controller.application.ApplicationPackage;
import com.yahoo.vespa.hosted.controller.application.RoutingPolicy;
import com.yahoo.vespa.hosted.controller.integration.ZoneApiMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.yahoo.vespa.hosted.controller.api.integration.LogEntry.Type.debug;
import static com.yahoo.vespa.hosted.controller.api.integration.LogEntry.Type.error;
import static com.yahoo.vespa.hosted.controller.api.integration.LogEntry.Type.info;
import static com.yahoo.vespa.hosted.controller.api.integration.LogEntry.Type.warning;
import static com.yahoo.vespa.hosted.controller.deployment.InternalDeploymentTester.appId;
import static com.yahoo.vespa.hosted.controller.deployment.InternalDeploymentTester.applicationPackage;
import static com.yahoo.vespa.hosted.controller.deployment.InternalDeploymentTester.testerId;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.failed;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.succeeded;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.unfinished;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jonmv
 * @author freva
 */
public class InternalStepRunnerTest {

    private InternalDeploymentTester tester;

    @Before
    public void setup() {
        tester = new InternalDeploymentTester();
    }

    private SystemName system() {
        return tester.tester().controller().system();
    }

    @Test
    public void canRegisterAndRunDirectly() {
        tester.deployNewSubmission();

        tester.deployNewPlatform(new Version("7.1"));
    }

    @Test
    public void canSwitchFromScrewdriverAndBackAgain() {
        // Deploys a default application package with default build number.
        tester.tester().deployCompletely(tester.app(), InternalDeploymentTester.applicationPackage);
        tester.setEndpoints(appId, JobType.productionUsCentral1.zone(system()));
        tester.setEndpoints(appId, JobType.productionUsWest1.zone(system()));
        tester.setEndpoints(appId, JobType.productionUsEast3.zone(system()));

        // Let application have an ongoing upgrade when it switches (but kill the jobs, as the tester assumes they aren't running).
        tester.tester().upgradeSystem(new Version("7.1"));
        tester.tester().buildService().clear();

        tester.deployNewSubmission();
        tester.deployNewSubmission();

        tester.deployNewPlatform(new Version("7.2"));

        tester.jobs().unregister(appId);
        try {
            tester.tester().deployCompletely(tester.app(), InternalDeploymentTester.applicationPackage, BuildJob.defaultBuildNumber + 1);
            throw new IllegalStateException("Component job should get even again with build numbers to produce a change.");
        }
        catch (AssertionError expected) { }
        tester.tester().deployCompletely(tester.app(), InternalDeploymentTester.applicationPackage, BuildJob.defaultBuildNumber + 2);
    }

    @Test
    public void testerHasAthenzIdentity() {
        tester.newRun(JobType.stagingTest);
        tester.runner().run();
        DeploymentSpec spec = tester.configServer().application(InternalDeploymentTester.testerId.id()).get().applicationPackage().deploymentSpec();
        assertEquals("domain", spec.athenzDomain().get().value());
        ZoneId zone = JobType.stagingTest.zone(system());
        assertEquals("service", spec.athenzService(zone.environment(), zone.region()).get().value());
    }

    @Test
    public void refeedRequirementBlocksDeployment() {
        RunId id = tester.newRun(JobType.stagingTest);

        tester.setEndpoints(testerId.id(), JobType.stagingTest.zone(system()));
        tester.runner().run();
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.installInitialReal));

        tester.setEndpoints(appId, JobType.stagingTest.zone(system()));
        tester.configServer().convergeServices(appId, JobType.stagingTest.zone(system()));
        tester.configServer().setConfigChangeActions(new ConfigChangeActions(Collections.emptyList(),
                                                                                      singletonList(new RefeedAction("Refeed",
                                                                                                                        false,
                                                                                                                        "doctype",
                                                                                                                        "cluster",
                                                                                                                        Collections.emptyList(),
                                                                                                                        singletonList("Refeed it!")))));
        tester.runner().run();

        assertEquals(failed, tester.jobs().run(id).get().steps().get(Step.deployReal));
    }

    @Test
    public void restartsServicesAndWaitsForRestartAndReboot() {
        RunId id = tester.newRun(JobType.productionUsCentral1);
        ZoneId zone = id.type().zone(system());
        HostName host = tester.configServer().hostFor(appId, zone);

        tester.setEndpoints(testerId.id(), JobType.productionUsCentral1.zone(system()));
        tester.runner().run();

        tester.configServer().setConfigChangeActions(new ConfigChangeActions(singletonList(new RestartAction("cluster",
                                                                                                             "container",
                                                                                                             "search",
                                                                                                             singletonList(new ServiceInfo("queries",
                                                                                                                                                                   "search",
                                                                                                                                                                   "config",
                                                                                                                                                                   host.value())),
                                                                                                             singletonList("Restart it!"))),
                                                                             Collections.emptyList()));
        tester.runner().run();
        assertEquals(succeeded, tester.jobs().run(id).get().steps().get(Step.deployReal));

        tester.configServer().convergeServices(appId, zone);
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.installReal));

        tester.configServer().nodeRepository().doRestart(new DeploymentId(appId, zone), Optional.of(host));
        tester.configServer().nodeRepository().requestReboot(new DeploymentId(appId, zone), Optional.of(host));
        tester.runner().run();
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.installReal));

        tester.clock().advance(InternalStepRunner.installationTimeout.plus(Duration.ofSeconds(1)));
        tester.runner().run();
        assertEquals(RunStatus.error, tester.jobs().run(id).get().status());
    }

    @Test
    public void waitsForEndpointsAndTimesOut() {
        tester.newRun(JobType.systemTest);

        // Tester fails to show up for staging tests, and the real deployment for system tests.
        tester.setEndpoints(testerId.id(), JobType.systemTest.zone(system()));
        tester.setEndpoints(appId, JobType.stagingTest.zone(system()));

        tester.runner().run();
        tester.configServer().convergeServices(appId, JobType.stagingTest.zone(system()));
        tester.runner().run();
        tester.configServer().convergeServices(appId, JobType.systemTest.zone(system()));
        tester.configServer().convergeServices(testerId.id(), JobType.systemTest.zone(system()));
        tester.configServer().convergeServices(appId, JobType.stagingTest.zone(system()));
        tester.configServer().convergeServices(testerId.id(), JobType.stagingTest.zone(system()));
        tester.runner().run();

        tester.clock().advance(InternalStepRunner.endpointTimeout.plus(Duration.ofSeconds(1)));
        tester.runner().run();
        assertEquals(failed, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installReal));
        assertEquals(failed, tester.jobs().last(appId, JobType.stagingTest).get().steps().get(Step.installTester));
    }

    @Test
    public void installationFailsIfDeploymentExpires() {
        tester.newRun(JobType.systemTest);
        tester.runner().run();
        tester.configServer().convergeServices(appId, JobType.systemTest.zone(system()));
        tester.setEndpoints(appId, JobType.systemTest.zone(system()));
        tester.runner().run();
        assertEquals(succeeded, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installReal));

        tester.applications().deactivate(appId, JobType.systemTest.zone(system()));
        tester.runner().run();
        assertEquals(failed, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installTester));
        assertTrue(tester.jobs().last(appId, JobType.systemTest).get().hasEnded());
        assertTrue(tester.jobs().last(appId, JobType.systemTest).get().hasFailed());
    }

    @Test
    public void startTestsFailsIfDeploymentExpires() {
        tester.newRun(JobType.systemTest);
        tester.runner().run();
        tester.configServer().convergeServices(appId, JobType.systemTest.zone(system()));
        tester.configServer().convergeServices(testerId.id(), JobType.systemTest.zone(system()));
        tester.runner().run();

        tester.applications().deactivate(appId, JobType.systemTest.zone(system()));
        tester.runner().run();
        assertEquals(unfinished, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.startTests));
    }

    @Test
    public void alternativeEndpointsAreDetected() {
        tester.newRun(JobType.systemTest);
        tester.runner().run();;
        tester.configServer().convergeServices(appId, JobType.systemTest.zone(system()));
        tester.configServer().convergeServices(testerId.id(), JobType.systemTest.zone(system()));
        assertEquals(unfinished, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installReal));
        assertEquals(unfinished, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installTester));

        tester.tester().controller().curator().writeRoutingPolicies(appId, Set.of(new RoutingPolicy(appId,
                                                                                                    ClusterSpec.Id.from("default"),
                                                                                                    JobType.systemTest.zone(system()),
                                                                                                    HostName.from("host"),
                                                                                                    Optional.empty(),
                                                                                                    emptySet())));
        tester.tester().controller().curator().writeRoutingPolicies(testerId.id(), Set.of(new RoutingPolicy(testerId.id(),
                                                                                                            ClusterSpec.Id.from("default"),
                                                                                                            JobType.systemTest.zone(system()),
                                                                                                            HostName.from("host"),
                                                                                                            Optional.empty(),
                                                                                                            emptySet())));
        tester.runner().run();;
        assertEquals(succeeded, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installReal));
        assertEquals(succeeded, tester.jobs().last(appId, JobType.systemTest).get().steps().get(Step.installTester));
    }

    @Test
    public void testsFailIfTesterRestarts() {
        RunId id = tester.startSystemTestTests();
        tester.cloud().set(TesterCloud.Status.NOT_STARTED);
        tester.runner().run();
        assertEquals(failed, tester.jobs().run(id).get().steps().get(Step.endTests));
    }

    @Test
    public void testsFailIfTestsFailRemotely() {
        RunId id = tester.startSystemTestTests();
        tester.cloud().add(new LogEntry(123, 321, error, "Failure!"));
        tester.cloud().set(TesterCloud.Status.FAILURE);

        long lastId = tester.jobs().details(id).get().lastId().getAsLong();
        tester.runner().run();
        assertTestLogEntries(id, Step.endTests,
                             new LogEntry(lastId + 1, 321, error, "Failure!"),
                             new LogEntry(lastId + 2, tester.clock().millis(), debug, "Tests failed."));
        assertEquals(failed, tester.jobs().run(id).get().steps().get(Step.endTests));
    }

    @Test
    public void testsFailIfTestsErr() {
        RunId id = tester.startSystemTestTests();
        tester.cloud().add(new LogEntry(0, 123, error, "Error!"));
        tester.cloud().set(TesterCloud.Status.ERROR);

        long lastId = tester.jobs().details(id).get().lastId().getAsLong();
        tester.runner().run();
        assertEquals(failed, tester.jobs().run(id).get().steps().get(Step.endTests));
        assertTestLogEntries(id, Step.endTests,
                             new LogEntry(lastId + 1, 123, error, "Error!"),
                             new LogEntry(lastId + 2, tester.clock().millis(), info, "Tester failed running its tests!"));
    }

    @Test
    public void testsSucceedWhenTheyDoRemotely() {
        RunId id = tester.startSystemTestTests();
        tester.runner().run();
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.endTests));
        assertEquals(URI.create(tester.routing().endpoints(new DeploymentId(testerId.id(), JobType.systemTest.zone(system()))).get(0).endpoint()),
                     tester.cloud().testerUrl());
        Inspector configObject = SlimeUtils.jsonToSlime(tester.cloud().config()).get();
        assertEquals(appId.serializedForm(), configObject.field("application").asString());
        assertEquals(JobType.systemTest.zone(system()).value(), configObject.field("zone").asString());
        assertEquals(system().value(), configObject.field("system").asString());
        assertEquals(1, configObject.field("endpoints").children());
        assertEquals(1, configObject.field("endpoints").field(JobType.systemTest.zone(system()).value()).entries());
        configObject.field("endpoints").field(JobType.systemTest.zone(system()).value()).traverse((ArrayTraverser) (__, endpoint) -> assertEquals(tester.routing().endpoints(new DeploymentId(appId, JobType.systemTest.zone(system()))).get(0).endpoint(), endpoint.asString()));

        long lastId = tester.jobs().details(id).get().lastId().getAsLong();
        tester.cloud().add(new LogEntry(0, 123, info, "Ready!"));
        tester.runner().run();
        assertTestLogEntries(id, Step.endTests,
                             new LogEntry(lastId + 1, 123, info, "Ready!"));

        tester.cloud().add(new LogEntry(1, 1234, info, "Steady!"));
        tester.runner().run();
        assertTestLogEntries(id, Step.endTests,
                             new LogEntry(lastId + 1, 123, info, "Ready!"),
                             new LogEntry(lastId + 2, 1234, info, "Steady!"));

        tester.cloud().add(new LogEntry(12, 12345, info, "Success!"));
        tester.cloud().set(TesterCloud.Status.SUCCESS);
        tester.runner().run();
        assertTestLogEntries(id, Step.endTests,
                             new LogEntry(lastId + 1, 123, info, "Ready!"),
                             new LogEntry(lastId + 2, 1234, info, "Steady!"),
                             new LogEntry(lastId + 3, 12345, info, "Success!"),
                             new LogEntry(lastId + 4, tester.clock().millis(), debug, "Tests completed successfully."));
        assertEquals(succeeded, tester.jobs().run(id).get().steps().get(Step.endTests));
    }

    @Test
    public void deployToDev() {
        ZoneId zone = JobType.devUsEast1.zone(system());
        tester.jobs().deploy(appId, JobType.devUsEast1, Optional.empty(), applicationPackage);
        tester.runner().run();
        RunId id = tester.jobs().last(appId, JobType.devUsEast1).get().id();
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.installReal));

        Version version = new Version("7.8.9");
        Future<?> concurrentDeployment = Executors.newSingleThreadExecutor().submit(() -> {
            tester.jobs().deploy(appId, JobType.devUsEast1, Optional.of(version), applicationPackage);
        });
        while ( ! concurrentDeployment.isDone())
            tester.runner().run();
        assertEquals(id.number() + 1, tester.jobs().last(appId, JobType.devUsEast1).get().id().number());

        ApplicationPackage otherPackage = new ApplicationPackageBuilder().region("us-central-1").build();
        tester.jobs().deploy(appId, JobType.perfUsEast3, Optional.empty(), otherPackage);

        tester.runner().run(); // Job run order determined by JobType enum order per application.
        tester.configServer().convergeServices(appId, zone);
        tester.setEndpoints(appId, zone);
        assertEquals(unfinished, tester.jobs().run(id).get().steps().get(Step.installReal));
        assertEquals(otherPackage.hash(), tester.configServer().application(appId).get().applicationPackage().hash());
        
        tester.configServer().setVersion(appId, zone, version);
        tester.runner().run();
        assertEquals(1, tester.jobs().active().size());
        assertEquals(version, tester.tester().application(appId).deployments().get(zone).version());

        try {
            tester.jobs().deploy(appId, JobType.productionApNortheast1, Optional.empty(), applicationPackage);
            fail("Deployments outside dev should not be allowed.");
        }
        catch (IllegalArgumentException expected) { }
    }

    @Test
    public void notificationIsSent() {
        tester.startSystemTestTests();
        tester.cloud().set(TesterCloud.Status.NOT_STARTED);
        tester.runner().run();
        MockMailer mailer = ((MockMailer) tester.tester().controller().serviceRegistry().mailer());
        assertEquals(1, mailer.inbox("a@b").size());
        assertEquals("Vespa application tenant.application: System test failing due to system error",
                     mailer.inbox("a@b").get(0).subject());
        assertEquals(1, mailer.inbox("b@a").size());
        assertEquals("Vespa application tenant.application: System test failing due to system error",
                     mailer.inbox("b@a").get(0).subject());
    }

    @Test
    public void vespaLogIsCopied() {
        RunId id = tester.startSystemTestTests();
        tester.cloud().set(TesterCloud.Status.ERROR);
        tester.configServer().setLogStream(vespaLog);
        long lastId = tester.jobs().details(id).get().lastId().getAsLong();
        tester.runner().run();
        assertEquals(failed, tester.jobs().run(id).get().steps().get(Step.endTests));
        assertTestLogEntries(id, Step.copyVespaLogs,
                             new LogEntry(lastId + 2, tester.clock().millis(), debug, "Copying Vespa log from nodes of tenant.application in test.us-east-1 ..."),
                             new LogEntry(lastId + 3, 1554970337084L, info,
                                          "17480180-v6-3.ostk.bm2.prod.ne1.yahoo.com\tcontainer\tContainer.com.yahoo.container.jdisc.ConfiguredApplication\n" +
                                          "Switching to the latest deployed set of configurations and components. Application switch number: 2"),
                             new LogEntry(lastId + 4, 1554970337935L, info,
                                          "17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\tcontainer\tstdout\n" +
                                          "ERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)"),
                             new LogEntry(lastId + 5, 1554970337947L, info,
                                          "17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\tcontainer\tstdout\n" +
                                          "ERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)"),
                             new LogEntry(lastId + 6, 1554970337947L, info,
                                          "17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\tcontainer\tstdout\n" +
                                          "ERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)"),
                             new LogEntry(lastId + 7, 1554970337947L, warning,
                                          "17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\tcontainer\tstderr\n" +
                                          "java.lang.NullPointerException\n\tat org.apache.felix.framework.BundleRevisionImpl.calculateContentPath(BundleRevisionImpl.java:438)\n\tat org.apache.felix.framework.BundleRevisionImpl.initializeContentPath(BundleRevisionImpl.java:371)"));
    }

    @Test
    public void certificateTimeoutAbortsJob() {
        tester.tester().controllerTester().zoneRegistry().setSystemName(SystemName.PublicCd);
        tester.tester().controllerTester().zoneRegistry().setZones(ZoneApiMock.fromId("prod.aws-us-east-1c"));
        RunId id = tester.startSystemTestTests();

        tester.clock().advance(InternalStepRunner.certificateTimeout.plus(Duration.ofSeconds(1)));
        tester.runner().run();
        assertEquals(RunStatus.aborted, tester.jobs().run(id).get().status());
    }

    private void assertTestLogEntries(RunId id, Step step, LogEntry... entries) {
        assertEquals(List.of(entries), tester.jobs().details(id).get().get(step));
    }

    private static final String vespaLog = "1554970337.084804\t17480180-v6-3.ostk.bm2.prod.ne1.yahoo.com\t5549/832\tcontainer\tContainer.com.yahoo.container.jdisc.ConfiguredApplication\tinfo\tSwitching to the latest deployed set of configurations and components. Application switch number: 2\n" +
                                           "1554970337.935104\t17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\t5480\tcontainer\tstdout\tinfo\tERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)\n" +
                                           "1554970337.947777\t17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\t5480\tcontainer\tstdout\tinfo\tERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)\n" +
                                           "1554970337.947820\t17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\t5480\tcontainer\tstdout\tinfo\tERROR: Bundle canary-application [71] Unable to get module class path. (java.lang.NullPointerException)\n" +
                                           "1554970337.947844\t17491290-v6-1.ostk.bm2.prod.ne1.yahoo.com\t5480\tcontainer\tstderr\twarning\tjava.lang.NullPointerException\\n\\tat org.apache.felix.framework.BundleRevisionImpl.calculateContentPath(BundleRevisionImpl.java:438)\\n\\tat org.apache.felix.framework.BundleRevisionImpl.initializeContentPath(BundleRevisionImpl.java:371)";

    @Test
    public void generates_correct_services_xml_test() {
        assertFile("test_runner_services.xml-cd", new String(InternalStepRunner.servicesXml(AthenzDomain.from("vespa.vespa.cd"),
                                                                                            true,
                                                                                            Optional.of("d-2-12-75"))));
    }

    private void assertFile(String resourceName, String actualContent) {
        try {
            Path path = Paths.get("src/test/resources/").resolve(resourceName);
            String expectedContent = new String(Files.readAllBytes(path));
            assertEquals(expectedContent, actualContent);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
