/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.oxia.v2.testing;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.NotificationContinuityRegistration;
import io.oxia.client.api.NotificationContinuitySnapshot;
import io.oxia.client.api.NotificationContinuityState;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Minimal dynamic O1 client fake for listener registration and close ordering. */
public final class FakeO1ContinuityClient {
    private final List<String> lifecycleEvents;
    private final AsyncOxiaClient proxy;
    private NotificationContinuitySnapshot current;
    private Consumer<NotificationContinuitySnapshot> listener;
    private boolean registrationClosed;
    private boolean clientClosed;
    private boolean callbackBeforeRegistrationReturns;
    private int unsupportedOperationCount;

    public FakeO1ContinuityClient(long generation, NotificationContinuityState state) {
        this(generation, state, new ArrayList<>());
    }

    public FakeO1ContinuityClient(long generation, NotificationContinuityState state, List<String> lifecycleEvents) {
        this.lifecycleEvents = lifecycleEvents;
        current = snapshot(generation, state);
        proxy = (AsyncOxiaClient) Proxy.newProxyInstance(
                AsyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {AsyncOxiaClient.class},
                (ignored, method, arguments) -> {
                    if (method.getName().equals("notificationContinuity")) {
                        @SuppressWarnings("unchecked")
                        Consumer<NotificationContinuitySnapshot> requested =
                                (Consumer<NotificationContinuitySnapshot>) arguments[0];
                        listener = requested;
                        lifecycleEvents.add("registration-open");
                        if (callbackBeforeRegistrationReturns) {
                            requested.accept(current);
                        }
                        return registration();
                    }
                    if (method.getName().equals("close")) {
                        clientClosed = true;
                        lifecycleEvents.add("client-close");
                        return null;
                    }
                    if (method.getName().equals("toString")) {
                        return "FakeO1ContinuityClient";
                    }
                    unsupportedOperationCount++;
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    public AsyncOxiaClient client() {
        return proxy;
    }

    public void callbackBeforeRegistrationReturns() {
        callbackBeforeRegistrationReturns = true;
    }

    public void emit(long generation, NotificationContinuityState state) {
        current = snapshot(generation, state);
        if (listener != null && !registrationClosed) {
            listener.accept(current);
        }
    }

    public boolean registrationClosed() {
        return registrationClosed;
    }

    public boolean clientClosed() {
        return clientClosed;
    }

    public int unsupportedOperationCount() {
        return unsupportedOperationCount;
    }

    public List<String> lifecycleEvents() {
        return List.copyOf(lifecycleEvents);
    }

    private NotificationContinuityRegistration registration() {
        return new NotificationContinuityRegistration() {
            @Override
            public NotificationContinuitySnapshot current() {
                return current;
            }

            @Override
            public void close() {
                if (!registrationClosed) {
                    registrationClosed = true;
                    lifecycleEvents.add("registration-close");
                }
            }
        };
    }

    private static NotificationContinuitySnapshot snapshot(long generation, NotificationContinuityState state) {
        CompletableFuture<Void> ready = new CompletableFuture<>();
        if (state == NotificationContinuityState.READY) {
            ready.complete(null);
        } else if (state == NotificationContinuityState.CLOSED) {
            ready.completeExceptionally(new IllegalStateException("closed"));
        }
        return new NotificationContinuitySnapshot(generation, state, ready);
    }
}
