package com.yahoo.vespa.hosted.controller.deployment;

import com.google.common.collect.ImmutableMap;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType;
import com.yahoo.vespa.hosted.controller.api.integration.deployment.RunId;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;

import static com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType.stagingTest;
import static com.yahoo.vespa.hosted.controller.api.integration.deployment.JobType.systemTest;
import static com.yahoo.vespa.hosted.controller.deployment.Step.report;
import static java.time.Instant.now;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author jonmv
 */
public class BadgesTest {

    private static final ApplicationId id = ApplicationId.from("tenant", "application", "default");
    private static final Run success = new Run(new RunId(id, systemTest, 3), ImmutableMap.of(report, Step.Status.succeeded),
                                               null, null, Optional.of(now()), RunStatus.success, 0, Optional.empty());

    private static final Run running = new Run(new RunId(id, systemTest, 4), ImmutableMap.of(report, Step.Status.succeeded),
                                               null, null, Optional.empty(), RunStatus.running, 0, Optional.empty());

    private static final Run failure = new Run(new RunId(id, JobType.stagingTest, 2), ImmutableMap.of(report, Step.Status.succeeded),
                                               null, null, Optional.of(now()), RunStatus.testFailure, 0, Optional.empty());

    @Test
    public void test() {
        Badges badges = new Badges(URI.create("https://badges.tld/api/"));

        assertEquals(URI.create("https://badges.tld/api/tenant.application;" + Badges.dark),
                     badges.historic(id, Optional.empty(), Collections.emptyList()));

        assertEquals(URI.create("https://badges.tld/api/tenant.application;" + Badges.dark +
                                "/" + systemTest.jobName() + ";" + Badges.blue +
                                "/%20;" + Badges.purple + ";s%7B" + Badges.white + "%7D"),
                     badges.historic(id, Optional.of(success), Collections.singletonList(running)));

        assertEquals(URI.create("https://badges.tld/api/tenant.application;" + Badges.dark +
                                "/" + systemTest.jobName() + ";" + Badges.blue +
                                "/%20;" + Badges.blue + ";s%7B" + Badges.white + "%7D" +
                                "/%20;" + Badges.purple + ";s%7B" + Badges.white + "%7D"),
                     badges.historic(id, Optional.of(success), List.of(success, running)));

        assertEquals(URI.create("https://badges.tld/api/tenant.application;" + Badges.dark +
                                "/" + systemTest.jobName() + ";" + Badges.purple +
                                "/" + stagingTest.jobName() + ";" + Badges.red),
                     badges.overview(id, List.of(running, failure)));
    }

}
