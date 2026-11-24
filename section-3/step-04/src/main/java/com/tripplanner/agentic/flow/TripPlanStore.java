package com.tripplanner.agentic.flow;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripplanner.model.PlanningRequest;
import com.tripplanner.model.TripApproval;
import com.tripplanner.model.TripError;
import com.tripplanner.model.TripPlanStatus;
import com.tripplanner.model.TripRequest;
import io.serverlessworkflow.impl.lifecycle.WorkflowExecutionListener;
import io.serverlessworkflow.impl.lifecycle.WorkflowFailedEvent;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

/*
 * TripPlanStore is an application-level, in-memory store for tracking trip-planning
 * requests and their outcomes. It is NOT the workflow engine's state store.
 *
 * There are two separate types of state in this application:
 *
 * 1. Workflow execution state, managed internally by Quarkus Flow. This includes
 *    workflow instance IDs, execution progress, context and paused workflows.
 * 2. Application-level trip state, managed by this class. This includes the original
 *    trip request, generated plan, approval decisions and current trip status.
 *
 * In Step 04, both are in-memory. Quarkus Flow uses its default in-memory state
 * management because no persistence extension is configured. This class maintains
 * its own state using the following fields:
 *
 * - requests: maps a request ID to its latest TripPlanStatus.
 * - instances: maps a workflow instance ID to its corresponding request ID.
 * - decisions: stores approval/rejection decisions submitted for pending workflows.
 * - latestRequestId: identifies the most recently registered trip.
 *
 * The store is shared by several application components:
 *
 * - TripPlannerResource registers new trips, waits for planning results and retrieves
 *   trip statuses for HTTP requests.
 * - TripApprovalResource records customer approval/rejection decisions before
 *   publishing them to Kafka.
 * - TripPlannerFlow binds request IDs to workflow instance IDs, retrieves trip
 *   information and validates decisions before resuming workflows.
 * - The Kafka consumer (flow-out-consumer) receives workflow outcome events,
 *   updates the corresponding trip status and wakes up HTTP request threads
 *   waiting for the result.
 * - The workflow execution listener handles workflow failures that may occur
 *   without a normal outcome event.
 *
 * Thread safety:
 *
 * This class is @ApplicationScoped, meaning its state is shared across concurrent
 * HTTP request threads, workflow execution threads, and Kafka consumer threads.
 * Since the underlying HashMaps are not thread-safe, synchronized methods protect
 * concurrent access and ensure that related updates across multiple maps and fields
 * remain consistent and visible to other threads.
 *
 * awaitPlan() uses timedWait() to suspend an HTTP request thread while waiting for
 * the trip to move beyond the initial "planning" state. Waiting releases the object's
 * monitor, allowing other threads to update the store.
 *
 * After updating a trip's status (e.g. when a workflow outcome is received or a
 * submission/workflow failure occurs), the store calls notifyAll() to wake up all
 * threads waiting on the same monitor. Each awakened thread reacquires the monitor
 * and checks whether its own request has reached the expected state. notifyAll()
 * is used instead of notify() because multiple requests may be waiting concurrently,
 * and notify() could wake a thread waiting for a different trip.
 *
 * This implementation is not durable: restarting the application loses both the
 * application-level trip data and the in-memory workflow execution state.
 *
 * Step 05 introduces database persistence for both independently:
 * - Quarkus Flow uses its JPA persistence extension to persist workflow execution state.
 * - TripPlanStore uses Hibernate ORM to persist application-level trip data.
 *
 * In Step 05, awaitPlan() uses database polling instead of wait()/notifyAll().
 */
@ApplicationScoped
public class TripPlanStore implements WorkflowExecutionListener {
    private static final Logger LOG = Logger.getLogger(TripPlanStore.class);
    private final Map<String, TripPlanStatus> requests = new HashMap<>();
    private final Map<String, String> instances = new HashMap<>();
    private final Map<String, TripApproval> decisions = new HashMap<>();
    private String latestRequestId;

    @Inject
    ObjectMapper objectMapper;

    public synchronized PlanningRequest register(TripRequest request) {
        String requestId = UUID.randomUUID().toString();
        requests.put(requestId, new TripPlanStatus(requestId, null, request, "planning", null, null, null, null));
        latestRequestId = requestId;
        return new PlanningRequest(requestId, request);
    }

    // Bind the request to the actual Flow instance before any model work or result publication.
    public synchronized TripPlanStatus bind(PlanningRequest input, String instanceId) {
        TripPlanStatus status = requests.get(input.requestId());
        if (status == null || !"planning".equals(status.status()) || status.instanceId() != null
                || !Objects.equals(input.request(), status.request())) {
            throw new IllegalStateException("Unknown or duplicate planning request");
        }
        TripPlanStatus bound = new TripPlanStatus(status.requestId(), instanceId, status.request(),
                "planning", null, null, null, null);
        requests.put(input.requestId(), bound);
        instances.put(instanceId, input.requestId());
        return bound;
    }

    @Incoming("flow-out-consumer")
    public CompletionStage<Void> consume(Message<String> message) {
        CloudEventMetadata<?> event = message.getMetadata(CloudEventMetadata.class).orElse(null);
        if (event == null) return message.ack();
        String instanceId = event.<String>getExtension("flowinstanceid").orElse(null);
        String state = switch (event.getType()) {
            case "com.tripplanner.trip.approval.requested" -> "awaiting_approval";
            case "com.tripplanner.booking.finalized" -> "confirmed";
            case "com.tripplanner.trip.rejected" -> "rejected";
            case "com.tripplanner.trip.failed" -> "failed";
            default -> null;
        };
        if (instanceId == null || state == null) return message.ack();
        try {
            accept(instanceId, state, objectMapper.readValue(message.getPayload(), TripPlanStatus.class));
        } catch (JsonProcessingException e) {
            LOG.warn("Ignoring malformed trip outcome event");
        }
        return message.ack();
    }

    private synchronized void accept(String instanceId, String state, TripPlanStatus result) {
        if (result == null || !instanceId.equals(result.instanceId()) || !state.equals(result.status())) return;
        TripPlanStatus previous = byInstanceId(instanceId);
        if (previous == null || !previous.requestId().equals(result.requestId())
                || !previous.request().equals(result.request())) return;
        boolean planning = "planning".equals(previous.status());
        if (planning && !state.equals("awaiting_approval") && !state.equals("failed")) return;
        if (!planning && (!"decision_submitted".equals(previous.status()) || state.equals("awaiting_approval"))) return;
        if (state.equals("awaiting_approval") && result.plan() == null) return;
        if (state.equals("confirmed") && result.confirmation() == null) return;
        // Finalization cannot replace or discard the plan the customer reviewed.
        requests.put(previous.requestId(), new TripPlanStatus(previous.requestId(), instanceId, previous.request(), state,
                planning ? result.plan() : previous.plan(), result.confirmation(), result.error(), result.message()));
        if (!planning) decisions.remove(instanceId);
        notifyAll();
    }

    public synchronized TripPlanStatus submitDecision(TripApproval approval) {
        String instanceId = approval.instanceId();
        TripPlanStatus previous = byInstanceId(instanceId);
        if (previous == null) throw error(404, "unknown_trip", "The requested trip was not found.");
        if (!"awaiting_approval".equals(previous.status())) {
            throw error(409, "decision_not_pending", "This trip is not awaiting a decision.");
        }
        TripPlanStatus submitted = previous.outcome("decision_submitted", previous.plan(), null);
        requests.put(previous.requestId(), submitted);
        decisions.put(instanceId, approval);
        return submitted;
    }

    public synchronized boolean matchesDecision(String instanceId, TripApproval approval) {
        TripPlanStatus status = byInstanceId(instanceId);
        return status != null && "decision_submitted".equals(status.status())
                && approval != null && approval.equals(decisions.get(instanceId));
    }

    public synchronized void submissionFailed(String requestId, Throwable failure) {
        TripPlanStatus previous = requests.get(requestId);
        if (previous == null || !("planning".equals(previous.status()) || "awaiting_approval".equals(previous.status())
                || "decision_submitted".equals(previous.status()))) return;
        requests.put(requestId, previous.failed(failure));
        decisions.remove(previous.instanceId());
        notifyAll();
    }

    /*
     * Handles terminal workflow failures as a fallback to the normal outcome-publishing mechanism.
     *
     * There are two failure scenarios:
     *
     * 1. Business-level failure (normal handling):
     *    A task such as plan() or resolveDecision() catches an exception and returns a
     *    TripPlanStatus with status "failed". The workflow continues and routes this result
     *    to publishFailure, which uses emitJson() to publish a com.tripplanner.trip.failed
     *    event to Kafka. The flow-out-consumer receives the event and updates TripPlanStore.
     *
     * 2. Terminal workflow failure (fallback handling):
     *    The workflow engine itself fails before it can successfully publish its outcome.
     *    For example, publishing the com.tripplanner.trip.failed event from publishFailure
     *    (or another outcome such as publishConfirmation) may fail. In this case, the normal
     *    Kafka-based notification might never reach the application. Flow notifies the
     *    WorkflowExecutionListener through onWorkflowFailed(), allowing the store to record
     *    the failure directly.
     *
     * In both cases, the request is marked as failed. Any plan already generated and
     * reviewed by the customer is preserved rather than discarded. The first scenario
     * is a failure of the trip-planning or booking operation, while the second is a
     * failure of the workflow execution itself. The latter is why WorkflowExecutionListener
     * is needed.
     */
    @Override
    public synchronized void onWorkflowFailed(WorkflowFailedEvent event) {
        // Publishing the domain outcome can itself fail. Only a terminal engine failure takes this fallback path.
        TripPlanStatus previous = byInstanceId(event.workflowContext().instanceData().id());
        if (previous != null) {
            submissionFailed(previous.requestId(), event.cause());
        }
    }

    public synchronized TripPlanStatus awaitPlan(String requestId, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            TripPlanStatus status = requests.get(requestId);
            if (status != null && !"planning".equals(status.status())) return status;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return null;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
    }

    public synchronized TripPlanStatus byRequestId(String requestId) {
        return requests.get(requestId);
    }

    public synchronized TripPlanStatus byInstanceId(String instanceId) {
        return requests.get(instances.get(instanceId));
    }

    public synchronized TripPlanStatus latest() {
        return requests.get(latestRequestId);
    }

    private static WebApplicationException error(int status, String code, String message) {
        return new WebApplicationException(Response.status(status).entity(new TripError(code, message)).build());
    }
}
