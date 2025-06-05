# Structured Cooperation Implementation Guide

## Introduction

This document describes a concrete implementation of the Structured Cooperation Protocol as defined in the RFC. It serves as a bridge between the abstract concepts in the RFC and a specific implementation approach. While this document avoids implementation details tied to specific technologies (such as Quarkus or reactive programming), it provides enough detail for someone to reimplement the system without seeing the original code.

## System Architecture

### Data Model

The implementation uses two primary database tables:

1. **messages**: Stores the basic signal data
   - `id`: Unique identifier (UUIDv7)
   - `topic`: The channel to which the signal belongs
   - `payload`: The signal content (stored as JSON)
   - `created_at`: Timestamp when the signal was created

2. **message_event**: Tracks the lifecycle of signals and operation runs
   - `id`: Unique identifier for the event (UUIDv7)
   - `message_id`: Reference to the signal this event relates to
   - `type`: Type of event (see Event Types section)
   - `coroutine_name`: Name of the operation
   - `coroutine_identifier`: Unique instance ID of the operation run
   - `step`: The step in the operation
   - `cooperation_lineage`: Array of UUIDs representing the nested hierarchy of cooperation scopes
   - `created_at`: Timestamp when the event was created

### Event Types

The implementation uses the following event types to track the lifecycle of signals and operation runs:

1. **EMITTED**: Created when a signal is published to a channel
2. **SEEN**: Created when an operation run first sees a signal
3. **SUSPENDED**: Created when an operation run completes a step and suspends
4. **COMMITTED**: Created when an operation run successfully completes all steps
5. **ROLLING_BACK**: Created when an operation run begins rolling back
6. **ROLLBACK_EMITTED**: Created for each signal emitted in a step that needs to be rolled back
7. **ROLLED_BACK**: Created when an operation run successfully completes rollback
8. **ROLLBACK_FAILED**: Created when an operation run fails during rollback

## Core Components

### Message Queue

The message queue component provides the following functionality:

1. **Signal Emission**: Allows operations to emit signals to channels
2. **Signal Subscription**: Allows operations to subscribe to channels
3. **Structured Cooperation**: Implements the structured cooperation protocol

### Coroutines

The implementation uses a coroutine-like abstraction to represent operations:

1. **Coroutine**: Represents an operation with multiple steps
   - Contains a list of steps to execute
   - Has a unique identifier
   - Has a cooperation hierarchy strategy

2. **Coroutine Identifier**: Uniquely identifies an operation
   - Consists of a name (identifying the operation) and an instance ID (identifying a specific operation run)

3. **Transactional Step**: Represents a discrete unit of execution within an operation
   - Has a name
   - Has an invoke method that executes the step
   - Has an optional rollback method that undoes the effects of the step
   - Has an optional handleChildFailures method that handles failures in child scopes

4. **Continuation**: Represents the state of a suspended operation run
   - Can be resumed with the result of the last step
   - Returns a continuation result (Suspend, Success, or Failure)

5. **Cooperation Scope**: Provides context for an operation run
   - Contains the cooperation lineage (the path from the root operation run)
   - Contains the continuation identifier (step name and coroutine identifier)
   - Tracks emitted signals

6. **Suspension Point**: Represents the point at which a coroutine is suspended
   - BeforeFirstStep: The coroutine is about to execute its first step
   - BetweenSteps: The coroutine has completed one step and is about to execute the next
   - AfterLastStep: The coroutine has completed its last step

### Cooperation Hierarchy Strategies

The implementation supports different strategies for determining when child operation runs have completed:

1. **Registry-based**: Uses a predefined registry of operations that should process specific signal types on specific channels
   - Maintains a mapping of channels to operation names
   - Generates SQL that checks if all expected operations have seen the signals emitted in the current step
   - Ensures that no signals are left unprocessed by any expected operation

2. **Time-based**: Waits for a specified time period and then waits for all operation runs that have started during that period to complete
   - Generates SQL that checks if a specified time has elapsed since the SEEN event was created
   - After the time has elapsed, ensures that all operation runs that have started processing the signal have completed

The strategies are implemented as SQL generators that produce conditions to be included in the query that determines when a suspended operation run can resume.

## Protocol Implementation

### Signal Emission and Processing

1. When a signal is emitted, an EMITTED event is created in the message_event table
2. The signal is stored in the messages table
3. Subscribers are notified of the new signal via a notification mechanism
4. When a subscriber receives a notification, it creates a SEEN event in the message_event table
5. The SEEN event extends the cooperation lineage by appending a new UUID, creating a child cooperation scope
6. Subscribers race to lock the SEEN event; those that find it already locked skip processing

### Operation Run Execution

1. An operation run begins when it receives a signal
2. The operation run executes its first step
3. If the step emits signals, they are recorded with the current cooperation lineage
4. After the step completes, the operation run emits a SUSPENDED event and exits
5. The operation run doesn't resume until all signals emitted before the SUSPENDED event are processed by all their corresponding handlers
6. Once all child signals are processed, the event loop picks up the signal again
7. The operation run checks if there were any errors in child scopes
8. If no errors, it continues with the next step, repeating the process
9. If all steps complete successfully, it emits a COMMITTED event

### Error Handling and Rollback

1. If an operation run fails during execution, it emits a ROLLING_BACK event
2. The rollback process consists of two main phases for each step:
   a. Rolling back child signals: For each signal emitted in the step, a ROLLBACK_EMITTED event is created
   b. Executing the step's rollback handler: The compensating action for the step is executed

3. The rollback process in detail:
   a. When an operation run fails, it marks itself as ROLLING_BACK
   b. It then loads the last SUSPENDED event (or the first step if there is none)
   c. For each signal emitted in that step, it creates a ROLLBACK_EMITTED event
   d. It suspends until all child handlers have processed their ROLLBACK_EMITTED events
   e. Once all child handlers have completed rollback, it executes the rollback handler for the step
   f. It then moves to the previous step and repeats the process
   g. If it reaches the beginning, it emits a ROLLED_BACK event
   h. If it fails during rollback, it emits a ROLLBACK_FAILED event

4. Child failure handling:
   a. When a child operation run fails, the parent operation run receives a CooperationScopeRolledBackException or CooperationScopeRollbackFailedException
   b. The parent can handle this exception in its handleChildFailures method
   c. If the parent doesn't handle the exception, it will also roll back
   d. If the parent handles the exception, it can continue with its next step

5. Special cases:
   a. If a step fails before emitting any signals, the transaction is rolled back automatically and no ROLLBACK_EMITTED events are created
   b. If a step has no child signals, the rollback process continues immediately without waiting
   c. If a rollback handler fails, the operation run is marked as ROLLBACK_FAILED and the failure is propagated to the parent

## Implementation Details

### Cooperation Lineage

The cooperation lineage is implemented as an array of UUIDs:

1. The root operation run has an empty lineage
2. When a SEEN event is created, a new UUID is appended to the lineage
3. This creates a hierarchical relationship between operation runs
4. The lineage is used to determine which operation runs are children of which parent operation runs

### Locking Mechanism

To prevent multiple instances of the same operation from processing the same signal:

1. Operation runs race to lock the SEEN record:
   a. The implementation uses a database-level locking mechanism (SELECT FOR UPDATE SKIP LOCKED)
   b. This allows multiple instances of the same operation to compete for the lock without blocking
   c. Only one instance will successfully acquire the lock; others will skip processing

2. The locking process in detail:
   a. When a signal is ready to be processed, the event loop identifies it
   b. Multiple operation run instances may attempt to process the same signal
   c. Each instance executes a query that attempts to lock the SEEN record
   d. The query includes FOR UPDATE SKIP LOCKED, which means it will skip records that are already locked
   e. The first instance to execute the query will acquire the lock
   f. Other instances will receive an empty result set, indicating the record is already locked
   g. The instance that acquired the lock will process the signal
   h. When processing is complete (or suspended), the lock is released

3. This locking mechanism ensures that:
   a. Each signal is processed by exactly one instance of each operation
   b. No two instances of the same operation process the same signal simultaneously
   c. If an instance fails while processing a signal, another instance can pick it up later
   d. The system can scale horizontally with multiple instances of the same operation

### Event Loop

The implementation uses an event loop to process signals:

1. The event loop periodically checks for signals that are ready to be processed
2. A signal is ready to be processed if:
   a. It has an EMITTED event but no SEEN event for the operation, or
   b. It has a SEEN event for the operation, doesn't have a COMMITTED, ROLLED_BACK, or ROLLBACK_FAILED event, and if it has a SUSPENDED event, all signals emitted in that step have been processed by all their corresponding handlers (as determined by the Cooperation Hierarchy Strategy)

3. The event loop query process:
   a. Finds all signals with EMITTED events that don't have a corresponding SEEN event for the operation
   b. Finds all SEEN events for the operation that haven't been committed or rolled back
   c. For each SEEN event, finds the most recent SUSPENDED event
   d. Identifies all signals emitted in the suspended step
   e. Matches each emitted signal to its SEEN events
   f. Ensures each SEEN event has a terminal event (COMMITTED, ROLLED_BACK, or ROLLBACK_FAILED)
   g. Applies the Cooperation Hierarchy Strategy to determine if all expected handlers have processed the signals
   h. Returns the IDs of signals that are ready to be processed

4. When a signal is ready to be processed:
   a. The operation run is resumed with the result of the last step
   b. If the last step was successful, the operation run continues with the next step
   c. If the last step failed, the operation run executes the handleChildFailures method
   d. If a child operation run failed, the parent operation run receives an exception and can decide whether to continue or roll back

### Transactional Semantics

The implementation provides transactional semantics:

1. Each step executes within a database transaction
2. Signals emitted within a step are part of the same transaction
3. If a transaction fails, signals emitted during that step are not propagated
4. Rollback steps also execute within transactions

### Optimizations

The implementation includes several optimizations:

1. **Immediate Continuation**: If a SUSPENDED event is emitted but there are no signals emitted in the step, the operation run immediately continues on the same instance
2. **Lock Skipping**: Operation runs skip signals that are already being processed by another instance of the same operation
3. **Batch Processing**: The event loop processes multiple signals in a batch to reduce overhead

## Conclusion

This implementation of the Structured Cooperation Protocol provides a robust foundation for building distributed systems with structured concurrency-like semantics. By tracking the lifecycle of signals and operation runs through events in a database, it ensures that parent operation runs only complete when all child operation runs have completed, and provides clear semantics for error propagation and resource management.
