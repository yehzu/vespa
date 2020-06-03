// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.handler.threadpool;

import com.google.inject.Inject;
import com.yahoo.component.AbstractComponent;
import com.yahoo.concurrent.ThreadFactoryFactory;
import com.yahoo.container.handler.ThreadpoolConfig;
import com.yahoo.container.protect.ProcessTerminator;
import com.yahoo.jdisc.Metric;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * A configurable thread pool. This provides the worker threads used for normal request processing.
 *
 * @author Steinar Knutsen
 * @author baldersheim
 * @author bratseth
 * @author bjorncs
 */
public class ContainerThreadPool extends AbstractComponent implements AutoCloseable {

    private final ExecutorServiceWrapper threadpool;

    @Inject
    public ContainerThreadPool(ThreadpoolConfig config, Metric metric) {
        this(config, metric, new ProcessTerminator());
    }

    public ContainerThreadPool(ThreadpoolConfig threadpoolConfig, Metric metric, ProcessTerminator processTerminator) {
        int maxNumThreads = computeThreadPoolSize(threadpoolConfig.maxthreads());
        WorkerCompletionTimingThreadPoolExecutor executor =
                new WorkerCompletionTimingThreadPoolExecutor(maxNumThreads, maxNumThreads,
                        0L, TimeUnit.SECONDS,
                        createQ(threadpoolConfig.queueSize(), maxNumThreads),
                        ThreadFactoryFactory.getThreadFactory("threadpool"),
                        metric);
        // Prestart needed, if not all threads will be created by the fist N tasks and hence they might also
        // get the dreaded thread locals initialized even if they will never run.
        // That counters what we we want to achieve with the Q that will prefer thread locality.
        executor.prestartAllCoreThreads();
        threadpool = new ExecutorServiceWrapper(executor, metric, processTerminator,
                threadpoolConfig.maxThreadExecutionTimeSeconds() * 1000L);
    }

    public Executor executor() { return threadpool; }
    @Override public void deconstruct() { closeInternal(); }
    @Override public void close() { closeInternal(); }

    /**
     * Shutdown the thread pool, give a grace period of 1 second before forcibly
     * shutting down all worker threads.
     */
    private void closeInternal() {
        boolean terminated;

        super.deconstruct();
        threadpool.shutdown();
        try {
            terminated = threadpool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!terminated) {
            threadpool.shutdownNow();
        }
    }

    private static BlockingQueue<Runnable> createQ(int queueSize, int maxThreads) {
        return (queueSize == 0)
                ? new SynchronousQueue<>(false)
                : (queueSize < 0)
                ? new ArrayBlockingQueue<>(maxThreads*4)
                : new ArrayBlockingQueue<>(queueSize);
    }

    private static int computeThreadPoolSize(int maxNumThreads) {
        return (maxNumThreads <= 0)
                ? Runtime.getRuntime().availableProcessors() * 4
                : maxNumThreads;
    }
}