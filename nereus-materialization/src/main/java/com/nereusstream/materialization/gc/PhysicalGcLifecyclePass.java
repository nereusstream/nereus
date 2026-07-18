/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Runs one complete metadata-first physical-GC lifecycle pass without owning its components. */
public final class PhysicalGcLifecyclePass {
    private final Supplier<CompletableFuture<PhysicalObjectRootScanResult>> roots;
    private final Supplier<CompletableFuture<StreamRegistrationRetirementScanResult>> registrations;
    private final Supplier<CompletableFuture<ObjectInventoryScanResult>> inventory;

    public PhysicalGcLifecyclePass(
            PhysicalObjectRootScanner roots,
            Supplier<PhysicalObjectRootVisitor> visitors,
            StreamRegistrationRetirementScanner registrations,
            ObjectInventoryScanner inventory) {
        PhysicalObjectRootScanner exactRoots = Objects.requireNonNull(roots, "roots");
        Supplier<PhysicalObjectRootVisitor> exactVisitors = Objects.requireNonNull(
                visitors, "visitors");
        this.roots = () -> {
            PhysicalObjectRootVisitor visitor = Objects.requireNonNull(
                    exactVisitors.get(), "physical-root visitor factory returned null");
            return exactRoots.scan(visitor);
        };
        this.registrations = Objects.requireNonNull(
                registrations, "registrations")::scan;
        this.inventory = Objects.requireNonNull(inventory, "inventory")::scan;
    }

    PhysicalGcLifecyclePass(
            Supplier<CompletableFuture<PhysicalObjectRootScanResult>> roots,
            Supplier<CompletableFuture<StreamRegistrationRetirementScanResult>> registrations,
            Supplier<CompletableFuture<ObjectInventoryScanResult>> inventory) {
        this.roots = Objects.requireNonNull(roots, "roots");
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
    }

    /**
     * Recovery metadata is always consumed before registration retirement, and object listing is always last.
     * A failed stage stops the pass; the lifecycle service retries from shard zero on the next pass.
     */
    public CompletableFuture<PhysicalGcLifecyclePassResult> scan() {
        final CompletableFuture<PhysicalObjectRootScanResult> rootPass;
        try {
            rootPass = Objects.requireNonNull(
                    roots.get(), "physical-root pass returned null");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return rootPass.thenCompose(rootResult ->
                require(registrations, "registration-retirement pass").thenCompose(registrationResult ->
                        require(inventory, "object-inventory pass").thenApply(inventoryResult ->
                                new PhysicalGcLifecyclePassResult(
                                        rootResult,
                                        registrationResult,
                                        inventoryResult))));
    }

    private static <T> CompletableFuture<T> require(
            Supplier<CompletableFuture<T>> operation, String stage) {
        try {
            return Objects.requireNonNull(operation.get(), stage + " returned null");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
