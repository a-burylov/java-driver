/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A helper class to debounce events received by the Control Connection.
 *
 * This class accumulates received events and starts a new timer each time a new
 * event is received, cancelling all pending timers.
 *
 * The events are only delivered:
 * - if the current timer times out, in which case the events are delivered asynchronously;
 * - if the queue is full, in which case the events are delivered synchronously.
 */
class EventDebouncer<T> {

    private static final Logger logger = LoggerFactory.getLogger(EventDebouncer.class);

    private final String name;

    private final AtomicReference<DeliveryAttempt> pendingDelivery = new AtomicReference<DeliveryAttempt>(null);

    private final ReentrantLock eventsLock = new ReentrantLock();

    private final CountDownLatch started = new CountDownLatch(1);

    private final ScheduledExecutorService executor;

    private final DeliveryCallback<T> callback;

    private final long delayMs;

    private final BlockingQueue<T> events;

    private volatile boolean stopped = false;

    EventDebouncer(String name, ScheduledExecutorService executor, DeliveryCallback<T> callback, long delayMs, int maxPendingEvents) {
        this.name = name;
        this.executor = executor;
        this.callback = callback;
        this.delayMs = delayMs;
        this.events = new ArrayBlockingQueue<T>(maxPendingEvents);
    }

    void start() {
        logger.trace("Starting {} debouncer...", name);
        started.countDown();
        logger.trace("{} debouncer started", name);
        eventsLock.lock();
        try {
            if (!events.isEmpty()) {
                logger.trace("{} debouncer: events were accumulated before the debouncer started: scheduling delivery now", name);
                scheduleNewDelivery();
            }
        } finally {
            eventsLock.unlock();
        }
    }

    void stop() {
        logger.trace("Stopping {} debouncer...", name);
        stopped = true;
        while (true) {
            DeliveryAttempt previous = cancelPendingDelivery();
            if (pendingDelivery.compareAndSet(previous, null)) {
                break;
            }
        }
        logger.trace("{} debouncer stopped", name);
    }

    void eventReceived(T event) {
        if(stopped) {
            logger.trace("{} debouncer is stopped, rejecting event: {}", name, event);
            return;
        }
        checkNotNull(event);
        logger.trace("{} debouncer: event received {}", name, event);
        enqueueEvent(event);
        scheduleNewDelivery();
    }

    private void enqueueEvent(T event) {
        eventsLock.lock();
        try {
            while (!events.offer(event)) {
                logger.trace("{} debouncer: event queue is full, blocking the caller and attempting to deliver immediately", name);
                try {
                    // if the queue is full and the debouncer is not ready,
                    // we have no other choice than to block and wait
                    started.await();
                    // this can race with a scheduled delivery,
                    // but in the worse case the number of delivered events will be zero
                    // which is harmless
                    deliverEvents();
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while attempting to deliver an event");
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            eventsLock.unlock();
        }
    }

    private DeliveryAttempt cancelPendingDelivery() {
        DeliveryAttempt previous = pendingDelivery.get();
        if (previous != null)
            previous.cancel();
        return previous;
    }

    private void scheduleNewDelivery() {
        while (isRunning()) {
            DeliveryAttempt previous = cancelPendingDelivery();
            DeliveryAttempt next = new DeliveryAttempt();
            if (pendingDelivery.compareAndSet(previous, next)) {
                next.schedule();
                break;
            }
        }
    }

    private boolean isRunning() {
        return started.getCount() == 0 && !stopped;
    }

    private void deliverEvents() throws InterruptedException {
        if(stopped) {
            return;
        }
        final List<T> events = new ArrayList<T>();
        int drained;
        eventsLock.lock();
        try {
            drained = this.events.drainTo(events);
        } finally {
            eventsLock.unlock();
        }
        if (drained > 0) {
            logger.trace("{} debouncer: delivering {} events", name, drained);
            callback.deliver(events);
        } else {
            logger.trace("{} debouncer: no events to deliver", name);
        }
    }

    class DeliveryAttempt extends ExceptionCatchingRunnable {

        volatile ScheduledFuture<?> deliveryFuture;

        void cancel() {
            if (deliveryFuture != null && !deliveryFuture.isCancelled())
                deliveryFuture.cancel(true);
        }

        void schedule() {
            if (!stopped)
                deliveryFuture = executor.schedule(this, delayMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void runMayThrow() throws Exception {
            deliverEvents();
        }

    }

    interface DeliveryCallback<T> {

        /**
         * Deliver the given list of events.
         * The given list is a private copy and any modification made to it
         * has no side-effect; it is also guaranteed not to be null nor empty.
         *
         * @param events the events to deliver
         */
        void deliver(List<T> events);

    }
}
