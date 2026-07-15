/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

/** Ephemeral authoritative subject revalidated at every F4 mutation boundary. */
public sealed interface GenerationActivationSubject
        permits LiveProjectionSubject, DomainValidatedDeletionSubject {
}
