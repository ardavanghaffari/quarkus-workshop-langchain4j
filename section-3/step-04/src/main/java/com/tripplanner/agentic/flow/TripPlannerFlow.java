package com.tripplanner.agentic.flow;

import static io.quarkiverse.flow.dsl.FlowDSL.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tripplanner.model.PlanningRequest;
import com.tripplanner.model.TripApproval;
import com.tripplanner.model.TripPlanStatus;
import io.quarkiverse.flow.Flow;
import io.cloudevents.CloudEvent;
import io.serverlessworkflow.impl.WorkflowContextData;
import io.serverlessworkflow.api.types.FlowDirectiveEnum;
import io.serverlessworkflow.api.types.Workflow;
import io.quarkiverse.flow.dsl.FlowWorkflowBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.io.IOException;

/*
 * Searching the code, we only find references to 'flow-in-producer' and 'flow-out-consumer'. Two
 * channels on the application side. The former for sending plan requests and human decisions to the
 * flow and the latter for receiving the results from the flow. We find, however, no references to
 * 'flow-in' and 'flow-out'. The channels through which the flow interacts with Kafka and the REST
 * API. That is because Quarkus Flow provides the Kafka-to-workflow messaging integration automatically.
 * `quarkus.flow.messaging.defaults-enabled=true` in application.properties enables the default messaging
 * bridge provided by the quarkus-flow-messaging extension. Quarkus Flow messaging bridge automatically
 *
 * 1. Consumes incoming events from flow-in and forwards them to Flow engine
 * 2. Publishes workflow events through flow-out to Kafka
 *
 * There is no explicit @Incoming("flow-in") in this class. Instead, the messaging extension handles
 * that integration. The workflow declares which event types it wants to react to, namely:
 *
 * .schedule(on(one("com.tripplanner.trip.requested").first()))
 *
 * When an event of type com.tripplanner.trip.requested arrives, start a new workflow instance.
 * Similarly, the flow waits for an event of type com.tripplanner.trip.approval.done that matches
 * this workflow instance using:
 *
 * listen("waitApproval",
 *   toOne(consumed("com.tripplanner.trip.approval.done")
 *       .envelope(this::matchesDecision)))
 *
 * On the publishing side, there is also no explicit kafka producer; instead the flow uses:
 *
 * emitJson(
 *   "requestApproval",
 *   "com.tripplanner.trip.approval.requested",
 *   TripPlanStatus.class
 * )
 *
 * And later, when the decision arrives, it emits one of publishConfirmation, publishRejection or
 * publishFailure. emitJson() is a Flow DSL operation. It creates and emits a CloudEvent from the
 * task's result. The messaging bridge takes care of publishing these events to the configured
 * flow-out channel, which sends them to Kafka.
 *
 * Channels on the application side are explicit in the code because they belong to the application's
 * own Reactive Messaging implementation. For example:
 *
 * @Inject
 * @Channel("flow-in-producer")
 * Emitter<PlanningRequest> flowInProducer;
 *
 * Is an ordinary MicroProfile Reactive Messaging producer. The application explicitly sends messages
 * through it. On the flow side, the quarkus-flow-messaging extension abstracts the equivalent Kafka
 * plumbing from the workflow implementation. 'flow-in' and 'flow-out' connection to the Kafka topics
 * is configured in application.properties, and their integration with the workflow engine is handled
 * by the extension.
 */

@ApplicationScoped
public class TripPlannerFlow extends Flow {

    @Inject
    TripPlannerFlowAdapter adapter;

    @Inject
    TripPlanStore store;

    @Inject
    ObjectMapper objectMapper;

    @Override
    public Workflow descriptor() {
        return FlowWorkflowBuilder.workflow("trip-planner-flow")
                .schedule(on(one("com.tripplanner.trip.requested").first()))
                .tasks(
                        withInstanceId("planTrip", this::plan, PlanningRequest.class),
                        switchWhenOrElse((TripPlanStatus result) -> "failed".equals(result.status()),
                                "publishFailure", "requestApproval", TripPlanStatus.class),
                        emitJson("requestApproval", "com.tripplanner.trip.approval.requested", TripPlanStatus.class),
                        listen("waitApproval",
                                toOne(consumed("com.tripplanner.trip.approval.done")
                                        .envelope(this::matchesDecision))),
                        withInstanceId("resolveDecision", this::resolveDecision, TripApproval.class),
                        switchCase(
                                caseOf((TripPlanStatus result) -> "confirmed".equals(result.status()), TripPlanStatus.class)
                                        .then("publishConfirmation"),
                                caseOf((TripPlanStatus result) -> "rejected".equals(result.status()), TripPlanStatus.class)
                                        .then("publishRejection"),
                                caseDefault("publishFailure")),
                        emitJson("publishConfirmation", "com.tripplanner.booking.finalized", TripPlanStatus.class)
                                .then(FlowDirectiveEnum.END),
                        emitJson("publishRejection", "com.tripplanner.trip.rejected", TripPlanStatus.class)
                                .then(FlowDirectiveEnum.END),
                        emitJson("publishFailure", "com.tripplanner.trip.failed", TripPlanStatus.class))
                .build();
    }

    private TripPlanStatus plan(String instanceId, PlanningRequest input) {
        TripPlanStatus status = store.bind(input, instanceId);
        try {
            return status.outcome("awaiting_approval", Objects.requireNonNull(adapter.planFromRequest(status.request())), null);
        } catch (Exception failure) {
            return status.failed(failure);
        }
    }

    private TripPlanStatus resolveDecision(String instanceId, TripApproval approval) {
        TripPlanStatus status = store.byInstanceId(instanceId);
        try {
            if (!store.matchesDecision(instanceId, approval)) {
                throw new IllegalStateException("Decision does not match the pending workflow");
            }
            if ("rejected".equals(approval.status())) return status.outcome("rejected", status.plan(), null);
            if (!"approved".equals(approval.status())) throw new IllegalArgumentException("Invalid decision");
            return status.outcome("confirmed", status.plan(), Objects.requireNonNull(adapter.finalizeBooking(approval)));
        } catch (Exception failure) {
            return status.failed(failure);
        }
    }

    /*
     * Note that listen() above is run for every workflow instance. Each workflow instance has
     * its own context. There could be e.g., 10 instances, only one of them matches and is resumed.
     */
    private boolean matchesDecision(CloudEvent event, WorkflowContextData context) {
        // One envelope predicate keeps both checks: dataAs() would replace an existing envelope predicate.
        String instanceId = context.instanceData().id();
        if (!instanceId.equals(event.getExtension("flowinstanceid")) || event.getData() == null) return false;
        try {
            return store.matchesDecision(instanceId, objectMapper.readValue(event.getData().toBytes(), TripApproval.class));
        } catch (IOException malformed) {
            return false;
        }
    }
}
