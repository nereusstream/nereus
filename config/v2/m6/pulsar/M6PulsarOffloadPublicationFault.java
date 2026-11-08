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

import com.nereusstream.pulsar.broker.PulsarP1OxiaMetadataStoreProvider;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.NereusOwnershipMetadataStore;

/** Test-only wrapper around the real qualified Oxia store. Only the original applied publication reply is lost. */
public final class M6PulsarOffloadPublicationFault implements MetadataStoreProvider {
    private static final String TARGET = "/managed-ledgers/"
            + TopicName.get("persistent://public/m6-native/handoff").getPersistenceNamingEncoding();

    @Override
    public String urlScheme() {
        return "oxia-nereus-m6-offload-fault";
    }

    @Override
    public MetadataStore create(String url, MetadataStoreConfig config, boolean watch) throws MetadataStoreException {
        var actual = new PulsarP1OxiaMetadataStoreProvider()
                .create("oxia-nereus" + url.substring(urlScheme().length()), config, watch);
        var cut = Path.of(System.getProperty("nereus.m6.publishFaultDirectory"));
        var count = new AtomicInteger();
        return (MetadataStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {NereusOwnershipMetadataStore.class}, (proxy, method, args) -> {
                    boolean publication = method.getName().equals("put") && args.length == 3
                            && TARGET.equals(args[0]) && Files.isRegularFile(cut.resolve("armed"))
                            && new MetaStoreImpl(null, null).parseManagedLedgerInfo((byte[]) args[1])
                                    .getLedgerInfosList().stream().anyMatch(ledger -> ledger.hasOffloadContext()
                                            && ledger.getOffloadContext().isComplete());
                    Object result;
                    try {
                        result = method.invoke(actual, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (!publication) {
                        return result;
                    }
                    if (count.incrementAndGet() != 1) {
                        Files.writeString(cut.resolve("repeated"), "unexpected second native publication");
                        throw new IllegalStateException("Native offload publication was redispatched");
                    }
                    @SuppressWarnings("unchecked")
                    var original = (CompletableFuture<Stat>) result;
                    return original.thenCompose(stat -> CompletableFuture.runAsync(() -> {
                        try {
                            Files.writeString(cut.resolve("applied"), Long.toString(stat.getVersion()),
                                    StandardOpenOption.CREATE_NEW);
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                            while (!Files.isRegularFile(cut.resolve("release"))) {
                                if (System.nanoTime() >= deadline) {
                                    throw new IllegalStateException("Test did not release the original publication callback");
                                }
                                Thread.sleep(20);
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new java.util.concurrent.CompletionException(interrupted);
                        } catch (java.io.IOException failure) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                    }).thenCompose(unused -> CompletableFuture.failedFuture(
                            new MetadataStoreException("fixture lost the applied native publication response"))));
                });
    }
}
