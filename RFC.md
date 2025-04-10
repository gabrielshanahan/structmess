# RFC: Structured Cooperation Protocol for Distributed Systems

## Abstract

This document specifies a protocol for implementing structured concurrency-like behavior across distributed systems, which we call "structured cooperation." This protocol enables operation runs in distributed systems to maintain parent-child relationships similar to structured concurrency, ensuring that parent operation runs only complete when all child operation runs they initiate have completed, and providing clear semantics for error propagation.

## Status of This Document

This document is a draft specification.

## Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## Definitions

The following terms are used throughout this document with specific meanings:

- **Operation**: A predefined sequence of steps that performs a specific task. An operation is a static definition of behavior.

- **Operation Run**: A specific execution instance of an operation. When we refer to parent-child relationships or hierarchies, we are referring to relationships between operation runs, not between operations themselves.

- **Step**: A discrete unit of execution within an operation. Each step SHOULD be wrapped in a transaction and may optionally define a compensating action (for rollback) and an action for handling child failures.

- **Signal**: A unit of data transmitted between components in the system. A signal carries a payload and is sent to a channel. Examples of signals are messages in message brokers, or events in event streaming systems.

- **Channel**: A named conduit through which signals are transmitted. Components can publish signals to channels and subscribe to receive signals from channels.

- **Cooperation Scope**: A context that defines the hierarchical relationship between operation runs. Each operation run executes within a cooperation scope.

- **Cooperation Lineage**: The path from the root operation run to the current operation run in the hierarchy.

- **Compensating Action**: An action that undoes the effects of a step when rollback is required.

## 1. Introduction

Structured concurrency is a programming paradigm that ensures child tasks complete before their parent tasks. This creates a clear hierarchy of tasks and simplifies reasoning, error handling, and resource management. While structured concurrency is well-established for in-process concurrency, applying these principles to distributed systems presents unique challenges.

This specification defines a protocol for extending structured concurrency principles to distributed systems, which we call "structured cooperation."

## 2. Core Concepts

### 2.1 Cooperation Hierarchy

A mechanism to establish and track parent-child relationships between operation runs in a distributed system MUST be implemented. This mechanism:

1. MUST uniquely identify each operation run in the system
2. MUST establish a hierarchical relationship between operation runs
3. MUST track the lineage of operation runs (the path from the root operation run to the current operation run)
4. MUST allow determining which operation runs were initiated by which parent operation runs

### 2.2 Signal-Based Communication

The protocol is based on signal passing between components:

1. Components MUST be able to emit signals to channels
2. Components MUST be able to subscribe to channels to receive signals
3. The protocol MUST support emitting signals during an operation run. Emitting a signal in this fashion MUST cause it to be associated with that operation run's hierarchy
4. The protocol MUST support emitting signals outside the hierarchy, which will not establish parent-child relationships. The protocol MAY also support this style of emission from within an operation run.

## 3. Protocol Semantics

### 3.1 Operation Run Lifecycle

Each operation run in the system MUST progress through a well-defined lifecycle:

1. **Initiation**: An operation run begins when a component receives a signal
2. **Processing**: The operation run executes its steps sequentially, with each step being a discrete unit of work
3. **Suspension**: After each step completes, the operation run MUST suspend execution if signals were emitted during that step
4. **Resumption**: A suspended operation run MUST resume when its dependencies are satisfied (i.e., when all child operation runs have completed)
5. **Completion**: An operation run ends with either success or failure
6. **Rollback**: In case of failure, the operation run MUST roll back its effects through compensating actions

### 3.2 Hierarchical Execution

The protocol MUST enforce the following hierarchical execution rules:

1. When an operation run emits a signal within its cooperation scope, it establishes a parent-child relationship with any operation runs that process that signal
2. A parent operation run MUST NOT proceed to its next step until a subset of child operation runs initiated from the current step is completed. The subset is specified by the Cooperation Hierarchy Strategy (see below)
3. A parent operation run MUST NOT complete until all its child operation runs have completed
4. The completion status of child operation runs MUST be propagated to their parent operation runs

### 3.3 Suspension and Resumption

The protocol MUST provide a mechanism for operation runs to suspend and resume:

1. After completing a step that emits signals, an operation run MUST suspend
2. A suspended operation run MUST NOT resume until all child operation runs initiated in the previous step have completed
3. The system MUST provide a way to determine when all relevant child operation runs (as given by the Cooperation Hierarchy Strategy) have completed
4. When all child operation runs have completed, the parent operation run MUST be resumed

### 3.4 Error Handling and Rollback

The protocol MUST provide error handling and rollback capabilities:

1. When a child of an operation run fails, the operation run MUST have the opportunity to recover from the failure
2. If an operation run cannot recover from a failure (either its own or of one or more of its children), then:
   1. if it is not yet being rolled back, the operation run MUST be rolled back, and the failure(s) MUST be propagated to its parent operation run
   2. if it is already being rolled back, the operation run MUST be marked as failed, and the failure(s) MUST be propagated to its parent operation run
3. When an operation run decides to roll back, it MUST do so by rolling back its individual steps in reverse order of their original execution
4. When a step is rolled back, it MUST do so in two consecutive substeps—first, by rolling back all child operation runs emitted in that step, then, by executing the compensating action for that step
5. The system MUST track the rollback status of operation runs. 

## 4. Cooperation Hierarchy Strategies

The protocol MUST support different strategies for determining when all relevant child operation runs have completed:

1. **Registry-based**: Using a predefined registry of components that should process specific signal types on specific channels
2. **Time-based**: Waiting for a specified time period and then waiting for all operation runs that have started during that period to complete
3. **Historical**: Using historical data to determine which components have processed similar signals in the past

Implementations MAY support one or more of these strategies or define additional strategies.

## 5. Transactional Semantics

The protocol MUST provide transactional semantics:

1. Each step of an operation run MUST execute within a transaction
2. Signals emitted within a step MUST be part of the same transaction
3. If a transaction fails before commiting, signals emitted during that step MUST NOT be propagated into the system
4. Compensating actions (rollback steps) MUST have transactional semantics

## 6. Implementation Considerations

### 6.1 Deadlock Prevention

Implementations MUST take care to prevent deadlocks:

1. The system SHOULD detect and resolve circular dependencies between operation runs
2. Timeouts SHOULD be implemented for operation runs that do not complete within expected time frames
3. The system SHOULD provide mechanisms to cancel operation runs that cannot complete

### 6.2 Performance

Implementations SHOULD optimize for performance:

1. The system SHOULD minimize overhead for tracking operation run hierarchies
2. The system SHOULD efficiently determine when child operation runs have completed
3. The system MAY implement optimizations for operation runs that do not emit signals

### 6.3 Monitoring and Debugging

Implementations SHOULD provide facilities for monitoring and debugging:

1. The system SHOULD track the status of all operation runs
2. The system SHOULD provide a way to visualize operation run hierarchies
3. The system SHOULD log significant events in the lifecycle of operation runs

## 7. Security Considerations

Implementations MUST consider security implications:

1. The system MUST ensure that operation runs cannot forge their position in the hierarchy
2. The system SHOULD provide mechanisms to limit the depth of operation run hierarchies
3. The system SHOULD implement rate limiting to prevent resource exhaustion attacks

## 8. Conclusion

This structured cooperation protocol enables distributed systems to maintain the benefits of structured concurrency while operating across process and machine boundaries. It provides clear semantics for signal processing, error propagation, and resource management in distributed environments.

Implementations of this protocol MAY use various persistence mechanisms, communication channels, and programming models, as long as they adhere to the semantics defined in this specification.
