/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Exact outcome counts from one complete pass over every registered product object prefix. */
public record ObjectInventoryScanResult(
        long familiesScanned,
        long pagesScanned,
        long objectsListed,
        long alreadyRooted,
        long wouldRegister,
        long rootsRegistered,
        long rootsConverged,
        long ageBlocked,
        long malformedKeys,
        long staleListings,
        long headMismatches,
        long rootConflicts) {
    public ObjectInventoryScanResult {
        if (familiesScanned < 0
                || pagesScanned < 0
                || objectsListed < 0
                || alreadyRooted < 0
                || wouldRegister < 0
                || rootsRegistered < 0
                || rootsConverged < 0
                || ageBlocked < 0
                || malformedKeys < 0
                || staleListings < 0
                || headMismatches < 0
                || rootConflicts < 0) {
            throw new IllegalArgumentException("object inventory counts must be non-negative");
        }
        long classified = Math.addExact(
                Math.addExact(
                        Math.addExact(alreadyRooted, wouldRegister),
                        Math.addExact(rootsRegistered, rootsConverged)),
                Math.addExact(
                        Math.addExact(ageBlocked, malformedKeys),
                        Math.addExact(
                                staleListings,
                                Math.addExact(headMismatches, rootConflicts))));
        if (classified != objectsListed) {
            throw new IllegalArgumentException(
                    "every listed object must have exactly one inventory outcome");
        }
    }
}
