/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.admin;

import java.nio.file.Path;

public final class NereusAdminMain {

    public static void main(String[] rawArgs) {
        int exitCode = run(rawArgs).code();
        if (exitCode != AdminExitCode.SUCCESS.code()) {
            System.exit(exitCode);
        }
    }

    static AdminExitCode run(String[] rawArgs) {
        CommandLineArguments args;
        try {
            args = CommandLineArguments.parse(rawArgs);
        } catch (IllegalArgumentException e) {
            System.err.println("invalid arguments: " + e.getMessage());
            return AdminExitCode.INVALID_ARGUMENT;
        }

        AdminConfiguration config;
        try {
            Path configPath = args.config();
            config = AdminConfiguration.load(configPath);
        } catch (RuntimeException e) {
            System.err.println("invalid configuration: " + e.getMessage());
            return AdminExitCode.CONFIGURATION_ERROR;
        }

        try {
            return switch (args.command()) {
                case "bookkeeper" -> dispatchBookKeeper(config, args);
                case "object-store" -> dispatchObjectStore(config, args);
                default -> throw new IllegalStateException(
                        "parser admitted an unsupported command: " + args.command());
            };
        } catch (RuntimeException e) {
            System.err.println("unexpected error: "
                    + AdminFailureClassifier.safeMessage(e));
            return AdminExitCode.INTERNAL_ERROR;
        }
    }

    private static AdminExitCode dispatchBookKeeper(
            AdminConfiguration config, CommandLineArguments args) {
        return switch (args.subcommand()) {
            case "namespace" -> dispatchBookKeeperNamespace(config, args);
            case "activation" -> BookKeeperActivationReadCommand.execute(config, args);
            default -> throw new IllegalStateException(
                    "parser admitted an unsupported BookKeeper command");
        };
    }

    private static AdminExitCode dispatchBookKeeperNamespace(
            AdminConfiguration config, CommandLineArguments args) {
        return switch (args.action().orElseThrow()) {
            case "ensure" -> BookKeeperNamespaceCommand.ensure(config, args);
            case "verify" -> BookKeeperNamespaceCommand.verify(config, args);
            default -> throw new IllegalStateException(
                    "parser admitted an unsupported namespace action");
        };
    }

    private static AdminExitCode dispatchObjectStore(
            AdminConfiguration config, CommandLineArguments args) {
        return switch (args.subcommand()) {
            case "verify" -> ObjectStoreContractCommand.verify(config, args);
            case "contract" -> ObjectStoreContractCommand.contract(config, args);
            case "persistence" -> dispatchObjectStorePersistence(config, args);
            default -> throw new IllegalStateException(
                    "parser admitted an unsupported object-store command");
        };
    }

    private static AdminExitCode dispatchObjectStorePersistence(
            AdminConfiguration config, CommandLineArguments args) {
        return switch (args.action().orElseThrow()) {
            case "create" -> ObjectStoreContractCommand.persistenceCreate(config, args);
            case "verify" -> ObjectStoreContractCommand.persistenceVerify(config, args);
            case "cleanup" -> ObjectStoreContractCommand.persistenceCleanup(config, args);
            default -> throw new IllegalStateException(
                    "parser admitted an unsupported object-store persistence action");
        };
    }
}
