// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.maintenance;

import com.yahoo.component.Version;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.athenz.api.OktaAccessToken;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.SourceRevision;
import com.yahoo.vespa.hosted.controller.application.ApplicationPackage;
import com.yahoo.vespa.hosted.controller.deployment.DeploymentTester;
import com.yahoo.vespa.hosted.controller.deployment.JobController;
import com.yahoo.vespa.hosted.controller.deployment.Run;
import com.yahoo.vespa.hosted.controller.deployment.RunStatus;
import com.yahoo.vespa.hosted.controller.deployment.Step;
import com.yahoo.vespa.hosted.controller.deployment.Step.Status;
import com.yahoo.vespa.hosted.controller.deployment.StepRunner;
import com.yahoo.vespa.hosted.controller.deployment.Versions;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType.stagingTest;
import static com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType.systemTest;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.aborted;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.error;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.running;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.testFailure;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.failed;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.succeeded;
import static com.yahoo.vespa.hosted.controller.deployment.Step.Status.unfinished;
import static com.yahoo.vespa.hosted.controller.deployment.Step.copyVespaLogs;
import static com.yahoo.vespa.hosted.controller.deployment.Step.deactivateReal;
import static com.yahoo.vespa.hosted.controller.deployment.Step.deactivateTester;
import static com.yahoo.vespa.hosted.controller.deployment.Step.deployReal;
import static com.yahoo.vespa.hosted.controller.deployment.Step.deployTester;
import static com.yahoo.vespa.hosted.controller.deployment.Step.endTests;
import static com.yahoo.vespa.hosted.controller.deployment.Step.installReal;
import static com.yahoo.vespa.hosted.controller.deployment.Step.installTester;
import static com.yahoo.vespa.hosted.controller.deployment.Step.report;
import static com.yahoo.vespa.hosted.controller.deployment.Step.startTests;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jonmv
 */
public class JobRunnerTest {

    private static final ApplicationPackage applicationPackage = new ApplicationPackage(new byte[0]);
    private static final Versions versions = new Versions(Version.fromString("1.2.3"),
                                                          ApplicationVersion.from(new SourceRevision("repo",
                                                                                                     "branch",
                                                                                                     "bada55"),
                                                                                  321),
                                                          Optional.empty(),
                                                          Optional.empty());

    @Test
    public void multiThreadedExecutionFinishes() {
        DeploymentTester tester = new DeploymentTester();
        JobController jobs = tester.controller().jobController();
        StepRunner stepRunner = (step, id) -> id.type() == stagingTest && step.get() == startTests? Optional.of(error) : Optional.of(running);
        Phaser phaser = new Phaser(1);
        JobRunner runner = new JobRunner(tester.controller(), Duration.ofDays(1), new JobControl(tester.controller().curator()),
                                         phasedExecutor(phaser), stepRunner);

        ApplicationId id = tester.createApplication("real", "tenant", 1, 1L).id();
        jobs.submit(id, versions.targetApplication().source().get(), "a@b", 2, applicationPackage, new byte[0]);

        jobs.start(id, systemTest, versions);
        try {
            jobs.start(id, systemTest, versions);
            fail("Job is already running, so this should not be allowed!");
        }
        catch (IllegalStateException e) { }
        jobs.start(id, stagingTest, versions);

        assertTrue(jobs.last(id, systemTest).get().steps().values().stream().allMatch(unfinished::equals));
        assertFalse(jobs.last(id, systemTest).get().hasEnded());
        assertTrue(jobs.last(id, stagingTest).get().steps().values().stream().allMatch(unfinished::equals));
        assertFalse(jobs.last(id, stagingTest).get().hasEnded());
        runner.maintain();

        phaser.arriveAndAwaitAdvance();
        assertTrue(jobs.last(id, systemTest).get().steps().values().stream().allMatch(succeeded::equals));
        assertTrue(jobs.last(id, stagingTest).get().hasEnded());
        assertTrue(jobs.last(id, stagingTest).get().hasFailed());
    }

    @Test
    public void stepLogic() {
        DeploymentTester tester = new DeploymentTester();
        JobController jobs = tester.controller().jobController();
        Map<Step, RunStatus> outcomes = new EnumMap<>(Step.class);
        JobRunner runner = new JobRunner(tester.controller(), Duration.ofDays(1), new JobControl(tester.controller().curator()),
                                         inThreadExecutor(), mappedRunner(outcomes));

        ApplicationId id = tester.createApplication("real", "tenant", 1, 1L).id();
        jobs.submit(id, versions.targetApplication().source().get(), "a@b", 2, applicationPackage, new byte[0]);
        Supplier<Run> run = () -> jobs.last(id, systemTest).get();

        jobs.start(id, systemTest, versions);
        RunId first = run.get().id();

        Map<Step, Status> steps = run.get().steps();
        runner.maintain();
        assertEquals(steps, run.get().steps());
        assertEquals(List.of(deployTester), run.get().readySteps());

        outcomes.put(deployTester, running);
        runner.maintain();
        assertEquals(List.of(deployReal), run.get().readySteps());

        outcomes.put(deployReal, running);
        runner.maintain();
        assertEquals(List.of(installTester, installReal), run.get().readySteps());

        outcomes.put(installReal, running);
        runner.maintain();
        assertEquals(List.of(installTester), run.get().readySteps());

        outcomes.put(installTester, running);
        runner.maintain();
        assertEquals(List.of(startTests), run.get().readySteps());

        outcomes.put(startTests, running);
        runner.maintain();
        assertEquals(List.of(endTests), run.get().readySteps());

        // Failure ending tests fails the run, but run-always steps continue.
        outcomes.put(endTests, testFailure);
        runner.maintain();
        assertTrue(run.get().hasFailed());
        assertEquals(List.of(copyVespaLogs, deactivateTester), run.get().readySteps());

        outcomes.put(copyVespaLogs, running);
        runner.maintain();
        assertEquals(List.of(deactivateReal, deactivateTester), run.get().readySteps());

        // Abortion does nothing, as the run has already failed.
        jobs.abort(run.get().id());
        runner.maintain();
        assertEquals(List.of(deactivateReal, deactivateTester), run.get().readySteps());

        outcomes.put(deactivateReal, running);
        outcomes.put(deactivateTester, running);
        outcomes.put(report, running);
        runner.maintain();
        assertTrue(run.get().hasFailed());
        assertTrue(run.get().hasEnded());
        assertTrue(run.get().status() == aborted);

        // A new run is attempted.
        jobs.start(id, systemTest, versions);
        assertEquals(first.number() + 1, run.get().id().number());

        // Run fails on tester deployment -- remaining run-always steps succeed, and the run finishes.
        outcomes.put(deployTester, error);
        runner.maintain();
        assertTrue(run.get().hasEnded());
        assertTrue(run.get().hasFailed());
        assertFalse(run.get().status() == aborted);
        assertEquals(failed, run.get().steps().get(deployTester));
        assertEquals(unfinished, run.get().steps().get(installTester));
        assertEquals(succeeded, run.get().steps().get(report));

        assertEquals(2, jobs.runs(id, systemTest).size());

        // Start a third run, then unregister and wait for data to be deleted.
        jobs.start(id, systemTest, versions);
        jobs.unregister(id);
        runner.maintain();
        assertFalse(jobs.last(id, systemTest).isPresent());
        assertTrue(jobs.runs(id, systemTest).isEmpty());
    }

    @Test
    public void locksAndGarbage() throws InterruptedException, BrokenBarrierException {
        DeploymentTester tester = new DeploymentTester();
        JobController jobs = tester.controller().jobController();
        // Hang during tester deployment, until notified.
        CyclicBarrier barrier = new CyclicBarrier(2);
        JobRunner runner = new JobRunner(tester.controller(), Duration.ofDays(1), new JobControl(tester.controller().curator()),
                                         Executors.newFixedThreadPool(32), waitingRunner(barrier));

        ApplicationId id = tester.createApplication("real", "tenant", 1, 1L).id();
        jobs.submit(id, versions.targetApplication().source().get(), "a@b", 2, applicationPackage, new byte[0]);

        RunId runId = new RunId(id, systemTest, 1);
        jobs.start(id, systemTest, versions);
        runner.maintain();
        barrier.await();
        try {
            jobs.locked(id, systemTest, deactivateTester, step -> { });
            fail("deployTester step should still be locked!");
        }
        catch (TimeoutException e) { }

        // Thread is still trying to deploy tester -- delete application, and see all data is garbage collected.
        assertEquals(Collections.singletonList(runId), jobs.active().stream().map(run -> run.id()).collect(Collectors.toList()));
        tester.controllerTester().deleteApplication(id);
        assertEquals(Collections.emptyList(), jobs.active());
        assertEquals(runId, jobs.last(id, systemTest).get().id());

        // Deployment still ongoing, so garbage is not yet collected.
        runner.maintain();
        assertEquals(runId, jobs.last(id, systemTest).get().id());

        // Deployment lets go, deactivation may now run, and trash is thrown out.
        barrier.await();
        runner.maintain();
        assertEquals(Optional.empty(), jobs.last(id, systemTest));
    }

    @Test
    public void historyPruning() {
        DeploymentTester tester = new DeploymentTester();
        JobController jobs = tester.controller().jobController();
        JobRunner runner = new JobRunner(tester.controller(), Duration.ofDays(1), new JobControl(tester.controller().curator()),
                                         inThreadExecutor(), (id, step) -> Optional.of(running));

        ApplicationId id = tester.createApplication("real", "tenant", 1, 1L).id();
        jobs.submit(id, versions.targetApplication().source().get(), "a@b", 2, applicationPackage, new byte[0]);

        for (int i = 0; i < jobs.historyLength(); i++) {
            jobs.start(id, systemTest, versions);
            runner.run();
        }

        assertEquals(256, jobs.runs(id, systemTest).size());
        assertTrue(jobs.details(new RunId(id, systemTest, 1)).isPresent());

        jobs.start(id, systemTest, versions);
        runner.run();

        assertEquals(256, jobs.runs(id, systemTest).size());
        assertEquals(2, jobs.runs(id, systemTest).keySet().iterator().next().number());
        assertFalse(jobs.details(new RunId(id, systemTest, 1)).isPresent());
        assertTrue(jobs.details(new RunId(id, systemTest, 257)).isPresent());
    }

    @Test
    public void timeout() {
        DeploymentTester tester = new DeploymentTester();
        JobController jobs = tester.controller().jobController();
        Map<Step, RunStatus> outcomes = new EnumMap<>(Step.class);
        JobRunner runner = new JobRunner(tester.controller(), Duration.ofDays(1), new JobControl(tester.controller().curator()),
                                         inThreadExecutor(), mappedRunner(outcomes));

        ApplicationId id = tester.createApplication("real", "tenant", 1, 1L).id();
        jobs.submit(id, versions.targetApplication().source().get(), "a@b", 2, applicationPackage, new byte[0]);

        jobs.start(id, systemTest, versions);
        tester.clock().advance(JobRunner.jobTimeout.plus(Duration.ofSeconds(1)));
        runner.run();
        assertTrue(jobs.last(id, systemTest).get().status() == aborted);
    }

    public static ExecutorService inThreadExecutor() {
        return new AbstractExecutorService() {
            AtomicBoolean shutDown = new AtomicBoolean(false);
            @Override public void shutdown() { shutDown.set(true); }
            @Override public List<Runnable> shutdownNow() { shutDown.set(true); return Collections.emptyList(); }
            @Override public boolean isShutdown() { return shutDown.get(); }
            @Override public boolean isTerminated() { return shutDown.get(); }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
            @Override public void execute(Runnable command) { command.run(); }
        };
    }

    private static ExecutorService phasedExecutor(Phaser phaser) {
        return new AbstractExecutorService() {
            ExecutorService delegate = Executors.newFixedThreadPool(32);
            @Override public void shutdown() { delegate.shutdown(); }
            @Override public List<Runnable> shutdownNow() { return delegate.shutdownNow(); }
            @Override public boolean isShutdown() { return delegate.isShutdown(); }
            @Override public boolean isTerminated() { return delegate.isTerminated(); }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException { return delegate.awaitTermination(timeout, unit); }
            @Override public void execute(Runnable command) {
                phaser.register();
                delegate.execute(() -> {
                    command.run();
                    phaser.arriveAndDeregister();
                });
            }
        };
    }

    private static StepRunner mappedRunner(Map<Step, RunStatus> outcomes) {
        return (step, id) -> Optional.ofNullable(outcomes.get(step.get()));
    }

    private static StepRunner waitingRunner(CyclicBarrier barrier) {
        return (step, id) -> {
            try {
                if (step.get() == deployTester) {
                    barrier.await(); // Wake up the main thread, which waits for this step to be locked.
                    barrier.reset();
                    barrier.await(); // Then wait while holding the lock for this step, until the main thread wakes us up.
                }
            }
            catch (InterruptedException | BrokenBarrierException e) {
                throw new AssertionError(e);
            }
            return Optional.of(running);
        };
    }

}
