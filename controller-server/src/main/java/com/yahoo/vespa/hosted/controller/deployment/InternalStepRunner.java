// Copyright 2019 Oath Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.deployment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.yahoo.component.Version;
import com.yahoo.config.application.api.DeploymentSpec;
import com.yahoo.config.application.api.Notifications;
import com.yahoo.config.application.api.Notifications.When;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.AthenzDomain;
import com.yahoo.config.provision.AthenzService;
import com.yahoo.config.provision.ClusterSpec;
import com.yahoo.config.provision.zone.ZoneId;
import com.yahoo.io.IOUtils;
import com.yahoo.log.LogLevel;
import com.yahoo.security.KeyAlgorithm;
import com.yahoo.security.KeyUtils;
import com.yahoo.security.SignatureAlgorithm;
import com.yahoo.security.X509CertificateBuilder;
import com.yahoo.vespa.hosted.controller.Application;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.api.ActivateResult;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.DeployOptions;
import com.yahoo.vespa.hosted.controller.api.identifiers.DeploymentId;
import com.yahoo.vespa.hosted.controller.api.identifiers.Hostname;
import com.yahoo.vespa.hosted.controller.api.integration.LogEntry;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.Node;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.PrepareResponse;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.ApplicationVersion;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.TesterCloud;
import com.yahoo.vespa.hosted.controller.api.integration.organization.DeploymentFailureMails;
import com.yahoo.vespa.hosted.controller.application.ApplicationPackage;
import com.yahoo.vespa.hosted.controller.application.Deployment;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobReport;
import com.yahoo.yolean.Exceptions;

import javax.security.auth.x500.X500Principal;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.yahoo.config.application.api.Notifications.Role.author;
import static com.yahoo.config.application.api.Notifications.When.failing;
import static com.yahoo.config.application.api.Notifications.When.failingCommit;
import static com.yahoo.log.LogLevel.DEBUG;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.ACTIVATION_CONFLICT;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.APPLICATION_LOCK_FAILURE;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.BAD_REQUEST;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.CERTIFICATE_NOT_READY;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.INVALID_APPLICATION_PACKAGE;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.LOAD_BALANCER_NOT_READY;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.OUT_OF_CAPACITY;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException.ErrorCode.PARENT_HOST_NOT_READY;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.Node.State.active;
import static com.yahoo.vespa.hosted.controller.api.integration.configserver.Node.State.reserved;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.aborted;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.deploymentFailed;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.error;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.installationFailed;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.outOfCapacity;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.running;
import static com.yahoo.vespa.hosted.controller.deployment.RunStatus.testFailure;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

/**
 * Runs steps of a deployment job against its provided controller.
 *
 * A dual-purpose logger is set up for each step run here:
 *   1. all messages are logged to a buffer which is stored in an external log storage at the end of execution, and
 *   2. all messages are also logged through the usual logging framework; by default, any messages of level
 *      {@code Level.INFO} or higher end up in the Vespa log, and all messages may be sent there by means of log-control.
 *
 * @author jonmv
 */
public class InternalStepRunner implements StepRunner {

    private static final Logger logger = Logger.getLogger(InternalStepRunner.class.getName());

    static final Duration endpointTimeout = Duration.ofMinutes(15);
    static final Duration installationTimeout = Duration.ofMinutes(150);
    static final Duration certificateTimeout = Duration.ofMinutes(300);

    private final Controller controller;
    private final TestConfigSerializer testConfigSerializer;
    private final DeploymentFailureMails mails;

    public InternalStepRunner(Controller controller) {
        this.controller = controller;
        this.testConfigSerializer = new TestConfigSerializer(controller.system());
        this.mails = new DeploymentFailureMails(controller.zoneRegistry());
    }

    @Override
    public Optional<RunStatus> run(LockedStep step, RunId id) {
        DualLogger logger = new DualLogger(id, step.get());
        try {
            switch (step.get()) {
                case deployTester: return deployTester(id, logger);
                case deployInitialReal: return deployInitialReal(id, logger);
                case installInitialReal: return installInitialReal(id, logger);
                case deployReal: return deployReal(id, logger);
                case installTester: return installTester(id, logger);
                case installReal: return installReal(id, logger);
                case startTests: return startTests(id, logger);
                case endTests: return endTests(id, logger);
                case copyVespaLogs: return copyVespaLogs(id, logger);
                case deactivateReal: return deactivateReal(id, logger);
                case deactivateTester: return deactivateTester(id, logger);
                case report: return report(id, logger);
                default: throw new AssertionError("Unknown step '" + step + "'!");
            }
        }
        catch (UncheckedIOException e) {
            logger.logWithInternalException(INFO, "IO exception running " + id + ": " + Exceptions.toMessageString(e), e);
            return Optional.empty();
        }
        catch (RuntimeException e) {
            logger.log(WARNING, "Unexpected exception running " + id, e);
            if (JobProfile.of(id.type()).alwaysRun().contains(step.get())) {
                logger.log("Will keep trying, as this is a cleanup step.");
                return Optional.empty();
            }
            return Optional.of(error);
        }
    }

    private Optional<RunStatus> deployInitialReal(RunId id, DualLogger logger) {
        Versions versions = controller.jobController().run(id).get().versions();
        logger.log("Deploying platform version " +
                   versions.sourcePlatform().orElse(versions.targetPlatform()) +
                   " and application version " +
                   versions.sourceApplication().orElse(versions.targetApplication()).id() + " ...");
        return deployReal(id, true, versions, logger);
    }

    private Optional<RunStatus> deployReal(RunId id, DualLogger logger) {
        Versions versions = controller.jobController().run(id).get().versions();
        logger.log("Deploying platform version " + versions.targetPlatform() +
                         " and application version " + versions.targetApplication().id() + " ...");
        return deployReal(id, false, versions, logger);
    }

    private Optional<RunStatus> deployReal(RunId id, boolean setTheStage, Versions versions, DualLogger logger) {
        Optional<ApplicationPackage> applicationPackage = id.type().environment().isManuallyDeployed()
                ? Optional.of(new ApplicationPackage(controller.applications().applicationStore()
                                                               .getDev(id.application(), id.type().zone(controller.system()))))
                : Optional.empty();
        Optional<Version> vespaVersion = id.type().environment().isManuallyDeployed()
                ? Optional.of(versions.targetPlatform())
                : Optional.empty();
        return deploy(id.application(),
                      id.type(),
                      () -> controller.applications().deploy(id.application(),
                                                             id.type().zone(controller.system()),
                                                             applicationPackage,
                                                             new DeployOptions(false,
                                                                               vespaVersion,
                                                                               false,
                                                                               setTheStage)),
                      logger);
    }

    private Optional<RunStatus> deployTester(RunId id, DualLogger logger) {
        Version platform = controller.jobController().run(id).get().versions().targetPlatform();
        logger.log("Deploying the tester container on platform " + platform + " ...");
        return deploy(id.tester().id(),
                      id.type(),
                      () -> controller.applications().deployTester(id.tester(),
                                                                   testerPackage(id),
                                                                   id.type().zone(controller.system()),
                                                                   new DeployOptions(true,
                                                                                     Optional.of(platform),
                                                                                     false,
                                                                                     false)),
                      logger);
    }

    private Optional<RunStatus> deploy(ApplicationId id, JobType type, Supplier<ActivateResult> deployment, DualLogger logger) {
        try {
            PrepareResponse prepareResponse = deployment.get().prepareResponse();
            if ( ! prepareResponse.configChangeActions.refeedActions.stream().allMatch(action -> action.allowed)) {
                List<String> messages = new ArrayList<>();
                messages.add("Deploy failed due to non-compatible changes that require re-feed.");
                messages.add("Your options are:");
                messages.add("1. Revert the incompatible changes.");
                messages.add("2. If you think it is safe in your case, you can override this validation, see");
                messages.add("   http://docs.vespa.ai/documentation/reference/validation-overrides.html");
                messages.add("3. Deploy as a new application under a different name.");
                messages.add("Illegal actions:");
                prepareResponse.configChangeActions.refeedActions.stream()
                                                                 .filter(action -> ! action.allowed)
                                                                 .flatMap(action -> action.messages.stream())
                                                                 .forEach(messages::add);
                messages.add("Details:");
                prepareResponse.log.stream()
                                   .map(entry -> entry.message)
                                   .forEach(messages::add);
                logger.log(messages);
                return Optional.of(deploymentFailed);
            }

            if (prepareResponse.configChangeActions.restartActions.isEmpty())
                logger.log("No services requiring restart.");
            else
                prepareResponse.configChangeActions.restartActions.stream()
                                                                  .flatMap(action -> action.services.stream())
                                                                  .map(service -> service.hostName)
                                                                  .sorted().distinct()
                                                                  .map(Hostname::new)
                                                                  .forEach(hostname -> {
                                                                      controller.applications().restart(new DeploymentId(id, type.zone(controller.system())), Optional.of(hostname));
                                                                      logger.log("Restarting services on host " + hostname.id() + ".");
                                                                  });
            logger.log("Deployment successful.");
            if (prepareResponse.message != null)
                logger.log(prepareResponse.message);
            return Optional.of(running);
        }
        catch (ConfigServerException e) {
            if (   e.getErrorCode() == OUT_OF_CAPACITY && type.isTest()
                || e.getErrorCode() == ACTIVATION_CONFLICT
                || e.getErrorCode() == APPLICATION_LOCK_FAILURE
                || e.getErrorCode() == PARENT_HOST_NOT_READY
                || e.getErrorCode() == CERTIFICATE_NOT_READY
                || e.getErrorCode() == LOAD_BALANCER_NOT_READY) {
                logger.log("Will retry, because of '" + e.getErrorCode() + "' deploying:\n" + e.getMessage());
                return Optional.empty();
            }
            if (   e.getErrorCode() == INVALID_APPLICATION_PACKAGE
                || e.getErrorCode() == BAD_REQUEST
                || e.getErrorCode() == OUT_OF_CAPACITY) {
                logger.log("Deployment failed: " + e.getMessage());
                return Optional.of(e.getErrorCode() == OUT_OF_CAPACITY ? outOfCapacity : deploymentFailed);
            }
            throw e;
        }
    }

    private Optional<RunStatus> installInitialReal(RunId id, DualLogger logger) {
        return installReal(id, true, logger);
    }

    private Optional<RunStatus> installReal(RunId id, DualLogger logger) {
        return installReal(id, false, logger);
    }

    private Optional<RunStatus> installReal(RunId id, boolean setTheStage, DualLogger logger) {
        Optional<Deployment> deployment = deployment(id.application(), id.type());
        if ( ! deployment.isPresent()) {
            logger.log(INFO, "Deployment expired before installation was successful.");
            return Optional.of(installationFailed);
        }

        Versions versions = controller.jobController().run(id).get().versions();
        Version platform = setTheStage ? versions.sourcePlatform().orElse(versions.targetPlatform()) : versions.targetPlatform();
        ApplicationVersion application = setTheStage ? versions.sourceApplication().orElse(versions.targetApplication()) : versions.targetApplication();
        logger.log("Checking installation of " + platform + " and " + application.id() + " ...");

        if (   nodesConverged(id.application(), id.type(), platform, logger)
            && servicesConverged(id.application(), id.type(), platform, logger)) {
            if (endpointsAvailable(id.application(), id.type().zone(controller.system()), logger)) {
                logger.log("Installation succeeded!");
                return Optional.of(running);
            }
            else if (timedOut(deployment.get(), endpointTimeout)) {
                logger.log(WARNING, "Endpoints failed to show up within " + endpointTimeout.toMinutes() + " minutes!");
                return Optional.of(error);
            }
        }

        if (timedOut(deployment.get(), installationTimeout)) {
            logger.log(INFO, "Installation failed to complete within " + installationTimeout.toMinutes() + " minutes!");
            return Optional.of(installationFailed);
        }

        logger.log("Installation not yet complete.");
        return Optional.empty();
    }

    private Optional<RunStatus> installTester(RunId id, DualLogger logger) {
        Optional<Deployment> deployment = deployment(id.application(), id.type());
        if ( ! deployment.isPresent()) {
            logger.log(WARNING, "Deployment expired before installation of tester was successful.");
            return Optional.of(error);
        }

        Version platform = controller.jobController().run(id).get().versions().targetPlatform();
        logger.log("Checking installation of tester container ...");
        if (   nodesConverged(id.tester().id(), id.type(), platform, logger)
            && servicesConverged(id.tester().id(), id.type(), platform, logger)) {
            if (endpointsAvailable(id.tester().id(), id.type().zone(controller.system()), logger)) {
                logger.log("Tester container successfully installed!");
                return Optional.of(running);
            }
            else if (timedOut(deployment.get(), endpointTimeout)) {
                logger.log(WARNING, "Tester failed to show up within " + endpointTimeout.toMinutes() + " minutes!");
                return Optional.of(error);
            }
        }

        if (timedOut(deployment.get(), installationTimeout)) {
            logger.log(WARNING, "Installation of tester failed to complete within " + installationTimeout.toMinutes() + " minutes of real deployment!");
            return Optional.of(error);
        }

        logger.log("Installation of tester not yet complete.");
        return Optional.empty();
    }

    private boolean endpointsAvailable(ApplicationId id, ZoneId zoneId, DualLogger logger) {
        logger.log("Attempting to find deployment endpoints ...");
        var endpoints = controller.applications().clusterEndpoints(id, Set.of(zoneId));
        if ( ! endpoints.containsKey(zoneId)) {
            logger.log("Endpoints not yet ready.");
            return false;
        }
        logEndpoints(endpoints, logger);
        return true;
    }

    private void logEndpoints(Map<ZoneId, Map<ClusterSpec.Id, URI>> endpoints, DualLogger logger) {
        List<String> messages = new ArrayList<>();
        messages.add("Found endpoints:");
        endpoints.forEach((zone, uris) -> {
            messages.add("- " + zone);
            uris.forEach((cluster, uri) -> messages.add(" |-- " + uri + " (" + cluster + ")"));
        });
        logger.log(messages);
    }

    private boolean nodesConverged(ApplicationId id, JobType type, Version target, DualLogger logger) {
        List<Node> nodes = controller.serviceRegistry().configServer().nodeRepository().list(type.zone(controller.system()), id, ImmutableSet.of(active, reserved));
        List<String> statuses = nodes.stream()
                .map(node -> String.format("%70s: %-16s%-25s%-32s%s",
                                           node.hostname(),
                                           node.serviceState(),
                                           node.wantedVersion() + (node.currentVersion().equals(node.wantedVersion()) ? "" : " <-- " + node.currentVersion()),
                                           node.restartGeneration() >= node.wantedRestartGeneration() ? ""
                                                   : "restart pending (" + node.wantedRestartGeneration() + " <-- " + node.restartGeneration() + ")",
                                           node.rebootGeneration() >= node.wantedRebootGeneration() ? ""
                                                   : "reboot pending (" + node.wantedRebootGeneration() + " <-- " + node.rebootGeneration() + ")"))
                .collect(Collectors.toList());
        logger.log(statuses);

        return nodes.stream().allMatch(node ->    node.currentVersion().equals(target)
                                               && node.restartGeneration() >= node.wantedRestartGeneration()
                                               && node.rebootGeneration() >= node.wantedRebootGeneration());
    }

    private boolean servicesConverged(ApplicationId id, JobType type, Version platform, DualLogger logger) {
        var convergence = controller.serviceRegistry().configServer().serviceConvergence(new DeploymentId(id, type.zone(controller.system())),
                                                                       Optional.of(platform));
        if (convergence.isEmpty()) {
            logger.log("Config status not currently available -- will retry.");
            return false;
        }
        logger.log("Wanted config generation is " + convergence.get().wantedGeneration());
        List<String> statuses = convergence.get().services().stream()
                .filter(serviceStatus -> serviceStatus.currentGeneration() != convergence.get().wantedGeneration())
                .map(serviceStatus -> String.format("%70s: %11s on port %4d has %s",
                                                    serviceStatus.host().value(),
                                                    serviceStatus.type(),
                                                    serviceStatus.port(),
                                                    serviceStatus.currentGeneration() == -1 ? "not started!" : Long.toString(serviceStatus.currentGeneration())))
                .collect(Collectors.toList());
        logger.log(statuses);
        if (statuses.isEmpty())
            logger.log("All services on wanted config generation.");

        return convergence.get().converged();
    }

    private Optional<RunStatus> startTests(RunId id, DualLogger logger) {
        Optional<Deployment> deployment = deployment(id.application(), id.type());
        if (deployment.isEmpty()) {
            logger.log(INFO, "Deployment expired before tests could start.");
            return Optional.of(aborted);
        }

        Set<ZoneId> zones = controller.jobController().testedZoneAndProductionZones(id.application(), id.type());

        logger.log("Attempting to find endpoints ...");
        var endpoints = controller.applications().clusterEndpoints(id.application(), zones);
        if ( ! endpoints.containsKey(id.type().zone(controller.system())) && timedOut(deployment.get(), endpointTimeout)) {
            logger.log(WARNING, "Endpoints for the deployment to test vanished again, while it was still active!");
            return Optional.of(error);
        }
        logEndpoints(endpoints, logger);

        Optional<URI> testerEndpoint = controller.jobController().testerEndpoint(id);
        if (testerEndpoint.isEmpty() && timedOut(deployment.get(), endpointTimeout)) {
            logger.log(WARNING, "Endpoints for the tester container vanished again, while it was still active!");
            return Optional.of(error);
        }

        if (controller.jobController().cloud().ready(testerEndpoint.get())) {
            logger.log("Starting tests ...");
            controller.jobController().cloud().startTests(testerEndpoint.get(),
                                                          TesterCloud.Suite.of(id.type()),
                                                          testConfigSerializer.configJson(id.application(),
                                                                                          id.type(),
                                                                                          endpoints,
                                                                                          listClusters(id.application(), zones)));
            return Optional.of(running);
        }

        logger.log("Tester container not yet ready.");
        return Optional.empty();
    }

    private Optional<RunStatus> endTests(RunId id, DualLogger logger) {
        if ( ! deployment(id.application(), id.type()).isPresent()) {
            logger.log(INFO, "Deployment expired before tests could complete.");
            return Optional.of(aborted);
        }

        Optional<X509Certificate> testerCertificate = controller.jobController().run(id).get().testerCertificate();
        if (testerCertificate.isPresent()) {
            try {
                testerCertificate.get().checkValidity(Date.from(controller.clock().instant()));
            }
            catch (CertificateExpiredException | CertificateNotYetValidException e) {
                logger.log(INFO, "Tester certificate expired before tests could complete.");
                return Optional.of(aborted);
            }
        };

        Optional<URI> testerEndpoint = controller.jobController().testerEndpoint(id);
        if ( ! testerEndpoint.isPresent()) {
            logger.log("Endpoints for tester not found -- trying again later.");
            return Optional.empty();
        }

        controller.jobController().updateTestLog(id);

        TesterCloud.Status testStatus = controller.jobController().cloud().getStatus(testerEndpoint.get());
        switch (testStatus) {
            case NOT_STARTED:
                throw new IllegalStateException("Tester reports tests not started, even though they should have!");
            case RUNNING:
                return Optional.empty();
            case FAILURE:
                logger.log("Tests failed.");
                return Optional.of(testFailure);
            case ERROR:
                logger.log(INFO, "Tester failed running its tests!");
                return Optional.of(error);
            case SUCCESS:
                logger.log("Tests completed successfully.");
                return Optional.of(running);
            default:
                throw new IllegalStateException("Unknown status '" + testStatus + "'!");
        }
    }

    private Optional<RunStatus> copyVespaLogs(RunId id, DualLogger logger) {
        ZoneId zone = id.type().zone(controller.system());
        if (deployment(id.application(), id.type()).isPresent())
            try {
                logger.log("Copying Vespa log from nodes of " + id.application() + " in " + zone + " ...");
                List<LogEntry> entries = new ArrayList<>();
                String logs = IOUtils.readAll(controller.serviceRegistry().configServer().getLogs(new DeploymentId(id.application(), zone),
                                                                                     Collections.emptyMap()), // Get all logs.
                                              StandardCharsets.UTF_8);
                for (String line : logs.split("\n")) {
                    String[] parts = line.split("\t");
                    if (parts.length != 7) continue;
                    entries.add(new LogEntry(0,
                                             (long) (Double.parseDouble(parts[0]) * 1000),
                                             LogEntry.typeOf(LogLevel.parse(parts[5])),
                                             parts[1] + '\t' + parts[3] + '\t' + parts[4] + '\n' +
                                             parts[6].replaceAll("\\\\n", "\n")
                                                     .replaceAll("\\\\t", "\t")));
                }
                controller.jobController().log(id, Step.copyVespaLogs, entries);
            }
            catch (Exception e) {
                logger.log(INFO, "Failure getting vespa logs for " + id, e);
                return Optional.of(error);
            }
        return Optional.of(running);
    }

    private Optional<RunStatus> deactivateReal(RunId id, DualLogger logger) {
        try {
            logger.log("Deactivating deployment of " + id.application() + " in " + id.type().zone(controller.system()) + " ...");
            controller.applications().deactivate(id.application(), id.type().zone(controller.system()));
            return Optional.of(running);
        }
        catch (RuntimeException e) {
            logger.log(WARNING, "Failed deleting application " + id.application(), e);
            return Optional.of(error);
        }
    }

    private Optional<RunStatus> deactivateTester(RunId id, DualLogger logger) {
        try {
            logger.log("Deactivating tester of " + id.application() + " in " + id.type().zone(controller.system()) + " ...");
            controller.jobController().deactivateTester(id.tester(), id.type());
            return Optional.of(running);
        }
        catch (RuntimeException e) {
            logger.log(WARNING, "Failed deleting tester of " + id.application(), e);
            return Optional.of(error);
        }
    }

    private Optional<RunStatus> report(RunId id, DualLogger logger) {
        try {
            controller.jobController().active(id).ifPresent(run -> {
                JobReport report = JobReport.ofJob(run.id().application(),
                                                   run.id().type(),
                                                   run.id().number(),
                                                   run.hasFailed() ? Optional.of(DeploymentJobs.JobError.unknown) : Optional.empty());
                controller.applications().deploymentTrigger().notifyOfCompletion(report);

                if (run.hasFailed())
                    sendNotification(run, logger);
            });
        }
        catch (IllegalStateException e) {
            logger.log(INFO, "Job '" + id.type() + "' no longer supposed to run?", e);
            return Optional.of(error);
        }
        return Optional.of(running);
    }

    /** Sends a mail with a notification of a failed run, if one should be sent. */
    private void sendNotification(Run run, DualLogger logger) {
        Application application = controller.applications().require(run.id().application());
        Notifications notifications = application.deploymentSpec().notifications();
        boolean newCommit = application.change().application()
                                       .map(run.versions().targetApplication()::equals)
                                       .orElse(false);
        When when = newCommit ? failingCommit : failing;

        List<String> recipients = new ArrayList<>(notifications.emailAddressesFor(when));
        if (notifications.emailRolesFor(when).contains(author))
            run.versions().targetApplication().authorEmail().ifPresent(recipients::add);

        if (recipients.isEmpty())
            return;

        try {
            if (run.status() == outOfCapacity && run.id().type().isProduction())
                controller.serviceRegistry().mailer().send(mails.outOfCapacity(run.id(), recipients));
            if (run.status() == deploymentFailed)
                controller.serviceRegistry().mailer().send(mails.deploymentFailure(run.id(), recipients));
            if (run.status() == installationFailed)
                controller.serviceRegistry().mailer().send(mails.installationFailure(run.id(), recipients));
            if (run.status() == testFailure)
                controller.serviceRegistry().mailer().send(mails.testFailure(run.id(), recipients));
            if (run.status() == error)
                controller.serviceRegistry().mailer().send(mails.systemError(run.id(), recipients));
        }
        catch (RuntimeException e) {
            logger.log(INFO, "Exception trying to send mail for " + run.id(), e);
        }
    }

    /** Returns the deployment of the real application in the zone of the given job, if it exists. */
    private Optional<Deployment> deployment(ApplicationId id, JobType type) {
        return Optional.ofNullable(application(id).deployments().get(type.zone(controller.system())));
    }

    /** Returns the real application with the given id. */
    private Application application(ApplicationId id) {
        controller.applications().lockOrThrow(id, __ -> { }); // Memory fence.
        return controller.applications().require(id);
    }

    /**
     * Returns whether the time since deployment is more than the zone deployment expiry, or the given timeout.
     *
     * We time out the job before the deployment expires, for zone where deployments are not persistent,
     * to be able to collect the Vespa log from the deployment. Thus, the lower of the zone's deployment expiry,
     * and the given default installation timeout, minus one minute, is used as a timeout threshold.
     */
    private boolean timedOut(Deployment deployment, Duration defaultTimeout) {
        Duration timeout = controller.zoneRegistry().getDeploymentTimeToLive(deployment.zone())
                                     .filter(zoneTimeout -> zoneTimeout.compareTo(defaultTimeout) < 0)
                                     .orElse(defaultTimeout);
        return deployment.at().isBefore(controller.clock().instant().minus(timeout.minus(Duration.ofMinutes(1))));
    }

    /** Returns the application package for the tester application, assembled from a generated config, fat-jar and services.xml. */
    private ApplicationPackage testerPackage(RunId id) {
        ApplicationVersion version = controller.jobController().run(id).get().versions().targetApplication();
        DeploymentSpec spec = controller.applications().require(id.application()).deploymentSpec();

        byte[] servicesXml = servicesXml(controller.zoneRegistry().accessControlDomain(),
                                         spec.athenzDomain().isPresent(),
                                         testerFlavorFor(id, spec));
        byte[] testPackage = controller.applications().applicationStore().get(id.tester(), version);

        ZoneId zone = id.type().zone(controller.system());
        byte[] deploymentXml = deploymentXml(spec.athenzDomain(), spec.athenzService(zone.environment(), zone.region()));

        if (controller.system().isPublic()) {
            KeyPair keyPair = KeyUtils.generateKeypair(KeyAlgorithm.EC, 256);
            X500Principal subject = new X500Principal("CN=" + id.tester().id().toFullString() + "." + id.type() + "." + id.number());
            X509Certificate certificate = X509CertificateBuilder.fromKeypair(keyPair, subject,
                                                                             Instant.now(), Instant.now().plus(certificateTimeout),
                                                                             SignatureAlgorithm.SHA512_WITH_ECDSA, BigInteger.valueOf(1))
                                                                .build();
            controller.jobController().storeTesterCertificate(id, certificate);
        }

        try (ZipBuilder zipBuilder = new ZipBuilder(testPackage.length + servicesXml.length + 1000)) {
            zipBuilder.add(testPackage);
            zipBuilder.add("services.xml", servicesXml);
            zipBuilder.add("deployment.xml", deploymentXml);
            zipBuilder.close();
            return new ApplicationPackage(zipBuilder.toByteArray());
        }
    }

    private static Optional<String> testerFlavorFor(RunId id, DeploymentSpec spec) {
        for (DeploymentSpec.Step step : spec.steps())
            if (step.deploysTo(id.type().environment()))
                return step.zones().get(0).testerFlavor();

        throw new IllegalStateException("No step deploys to the zone this run is for!");
    }

    /** Returns all content clusters in all current deployments of the given real application. */
    private Map<ZoneId, List<String>> listClusters(ApplicationId id, Iterable<ZoneId> zones) {
        ImmutableMap.Builder<ZoneId, List<String>> clusters = ImmutableMap.builder();
        for (ZoneId zone : zones)
            clusters.put(zone, ImmutableList.copyOf(controller.serviceRegistry().configServer().getContentClusters(new DeploymentId(id, zone))));
        return clusters.build();
    }

    /** Returns the generated services.xml content for the tester application. */
    static byte[] servicesXml(AthenzDomain domain, boolean useAthenzCredentials, Optional<String> testerFlavor) {
        String flavor = testerFlavor.orElse("d-1-4-50");
        int memoryGb = Integer.parseInt(flavor.split("-")[2]); // Memory available in tester container.
        int jdiscMemoryPercentage = (int) Math.ceil(200.0 / memoryGb); // 2Gb memory for tester application (excessive?).
        int testMemoryMb = 512 * (memoryGb - 2); // Memory allocated to Surefire running tests. ≥25% left for other stuff.

        String servicesXml =
                "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<services xmlns:deploy='vespa' version='1.0'>\n" +
                "    <container version='1.0' id='tester'>\n" +
                "\n" +
                "        <component id=\"com.yahoo.vespa.hosted.testrunner.TestRunner\" bundle=\"vespa-testrunner-components\">\n" +
                "            <config name=\"com.yahoo.vespa.hosted.testrunner.test-runner\">\n" +
                "                <artifactsPath>artifacts</artifactsPath>\n" +
                "                <surefireMemoryMb>" + testMemoryMb + "</surefireMemoryMb>\n" +
                "                <useAthenzCredentials>" + useAthenzCredentials + "</useAthenzCredentials>\n" +
                "            </config>\n" +
                "        </component>\n" +
                "\n" +
                "        <handler id=\"com.yahoo.vespa.hosted.testrunner.TestRunnerHandler\" bundle=\"vespa-testrunner-components\">\n" +
                "            <binding>http://*/tester/v1/*</binding>\n" +
                "        </handler>\n" +
                "\n" +
                "        <http>\n" +
                "            <server id='default' port='4080'/>\n" +
                "            <filtering>\n" +
                "                <access-control domain='" + domain.value() + "'>\n" + // Set up dummy access control to pass validation :/
                "                    <exclude>\n" +
                "                        <binding>http://*/tester/v1/*</binding>\n" +
                "                    </exclude>\n" +
                "                </access-control>\n" +
                "                <request-chain id=\"testrunner-api\">\n" +
                "                    <filter id='authz-filter' class='com.yahoo.jdisc.http.filter.security.athenz.AthenzAuthorizationFilter' bundle=\"jdisc-security-filters\">\n" +
                "                        <config name=\"jdisc.http.filter.security.athenz.athenz-authorization-filter\">\n" +
                "                            <credentialsToVerify>TOKEN_ONLY</credentialsToVerify>\n" +
                "                            <roleTokenHeaderName>Yahoo-Role-Auth</roleTokenHeaderName>\n" +
                "                        </config>\n" +
                "                        <component id=\"com.yahoo.jdisc.http.filter.security.athenz.StaticRequestResourceMapper\" bundle=\"jdisc-security-filters\">\n" +
                "                            <config name=\"jdisc.http.filter.security.athenz.static-request-resource-mapper\">\n" +
                "                                <resourceName>" + domain.value() + ":tester-application</resourceName>\n" +
                "                                <action>deploy</action>\n" +
                "                            </config>\n" +
                "                        </component>\n" +
                "                    </filter>\n" +
                "                </request-chain>\n" +
                "            </filtering>\n" +
                "        </http>\n" +
                "\n" +
                "        <nodes count=\"1\" flavor=\"" + flavor + "\" allocated-memory=\"" + jdiscMemoryPercentage + "%\" />\n" +
                "    </container>\n" +
                "</services>\n";

        return servicesXml.getBytes(StandardCharsets.UTF_8);
    }

    /** Returns a dummy deployment xml which sets up the service identity for the tester, if present. */
    private static byte[] deploymentXml(Optional<AthenzDomain> athenzDomain, Optional<AthenzService> athenzService) {
        String deploymentSpec =
                "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<deployment version=\"1.0\" " +
                athenzDomain.map(domain -> "athenz-domain=\"" + domain.value() + "\" ").orElse("") +
                athenzService.map(service -> "athenz-service=\"" + service.value() + "\" ").orElse("")
                + "/>";
        return deploymentSpec.getBytes(StandardCharsets.UTF_8);
    }

    /** Logger which logs to a {@link JobController}, as well as to the parent class' {@link Logger}. */
    private class DualLogger {

        private final RunId id;
        private final Step step;

        private DualLogger(RunId id, Step step) {
            this.id = id;
            this.step = step;
        }

        private void log(String... messages) {
            log(List.of(messages));
        }

        private void log(List<String> messages) {
            controller.jobController().log(id, step, DEBUG, messages);
        }

        private void log(Level level, String message) {
            log(level, message, null);
        }

        // Print stack trace in our logs, but don't expose it to end users
        private void logWithInternalException(Level level, String message, Throwable thrown) {
            logger.log(level, id + " at " + step + ": " + message, thrown);
            controller.jobController().log(id, step, level, message);
        }

        private void log(Level level, String message, Throwable thrown) {
            logger.log(level, id + " at " + step + ": " + message, thrown);

            if (thrown != null) {
                ByteArrayOutputStream traceBuffer = new ByteArrayOutputStream();
                thrown.printStackTrace(new PrintStream(traceBuffer));
                message += "\n" + traceBuffer;
            }
            controller.jobController().log(id, step, level, message);
        }

    }

}
