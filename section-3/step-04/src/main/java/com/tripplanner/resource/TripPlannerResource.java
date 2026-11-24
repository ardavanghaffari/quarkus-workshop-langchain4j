package com.tripplanner.resource;

import com.tripplanner.agentic.flow.TripPlanStore;
import com.tripplanner.model.PlanningRequest;
import com.tripplanner.model.TripError;
import com.tripplanner.model.TripPlanStatus;
import com.tripplanner.model.TripRequest;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Path("/trip")
@Produces(MediaType.APPLICATION_JSON)
public class TripPlannerResource {
    @ConfigProperty(name = "trip.planning.timeout", defaultValue = "PT120S")
    Duration planTimeout;

    @Inject
    TripPlanStore tripPlanStore;

    /*
     * This is a SmallRye Reactive Messaging Emitter. It allows the REST resource to publish
     * messages to the flow-in-producer channel, which is configured to send messages to the
     * Kafka topic flow-in.
     */
    @Channel("flow-in-producer")
    Emitter<PlanningRequest> flowInProducer;

    @POST
    @Path("/plan")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response planTrip(TripRequest request) {
        Response invalid = validatePlanRequest(request);
        if (invalid != null) return invalid;
        PlanningRequest input = tripPlanStore.register(request);
        OutgoingCloudEventMetadata<PlanningRequest> metadata = OutgoingCloudEventMetadata.<PlanningRequest>builder()
                .withId(input.requestId())
                .withSource(URI.create("api:/trip/plan"))
                .withType("com.tripplanner.trip.requested")
                .withDataContentType("application/json")
                .build();
        /*
         * Handle failures that occur when submitting the trip planning request to Kafka. There are
         * two different ways the submission can fail, and the code handles both. Note that
         * flowInProducer.send() is asynchronous so it doesn't wait until Kafka has successfully
         * received the message.
         *
         * Reactive Messaging uses two acknowledgements:
         * - ACK (acknowledgement): The message was successfully processed.
         * - NACK (negative acknowledgement): Something went wrong while processing or sending the message.
         *
         * The Nack callback is invoked in case of failure which updates the trip's status in
         * TripPlanStore and returns a completed CompletableFuture. The store is updated because
         * register() has already created a trip entry with status planning. If publishing the request
         * fails, no workflow will start and the trip remains stuck in planning. The Nack callback
         * returns a CompletableFuture because it is expected to return a CompletionStage<Void>. This
         * allows the messaging framework to know when the failure handling itself has completed.
         * Here, submissionFailed() is synchronous, so there's no asynchronous work to wait for.
         * Returning an already completed future is enough.
         *
         * This was so far an asynchronous failure scenario in which the message is accepted by the
         * emitter, but subsequently fails to be sent. There is also a synchronous failure in which
         * send() itself throws an exception. The emitter might be unable to accept a message because
         * of an immediate problem, such as a closed emitter. In this case, send() throws a RuntimeException,
         * and the catch block handles it.
         */
        try {
            flowInProducer.send(Message.of(input, Metadata.of(metadata)).withNack(failure -> {
                tripPlanStore.submissionFailed(input.requestId(), failure);
                return CompletableFuture.completedFuture(null);
            }));
        } catch (RuntimeException failure) {
            tripPlanStore.submissionFailed(input.requestId(), failure);
        }

        /*
         * awaitPlan() is a synchronized method. This thread acquires the TripPlanStore monitor if
         * available and calls awaitPlan(). There, wait() is called which releases the monitor and
         * puts this thread in the waiting state. If anything goes wrong with submitting the planning
         * request to the flow, submissionFailed() calls notifyAll() which makes this thread eligible
         * to wake up and acquire the lock.
         */
        try {
            TripPlanStatus result = tripPlanStore.awaitPlan(input.requestId(), planTimeout);
            if (result == null) {
                TripPlanStatus pending = tripPlanStore.byRequestId(input.requestId());
                return Response.status(504).entity(pending.withError("planning_timeout",
                        "Planning is taking longer than expected. The workflow may still complete; check its status before retrying.")).build();
            }
            int code = "failed".equals(result.status()) ? new TripError(result.error(), result.message()).httpStatus() : 200;
            return Response.status(code).entity(result).build();
        } catch (InterruptedException interrupted) {
            /*
             * Occurs when this thread is waiting inside tripPlanStore.awaitPlan() and another
             * thread (or the runtime) interrupts it. There are several possible reasons.
             * 1. The HTTP request is canceled. For example, the client cancels the HTTP request.
             *    A reverse proxy or load balancer terminates the connection. Or the HTTP client
             *    times out and abandons the request.
             * 2. Application or server shutdown. For example, Quarkus is shutting down.
             * 3. Thread pool or executor management. The HTTP request is executed on a worker
             *    thread managed by the runtime. If that thread is interrupted by some infrastructure
             *    or application code while waiting, wait() throws InterruptedException.
             */
            Thread.currentThread().interrupt(); // sets the interrupted status (interrupt flag) of the current thread to true.
            /*
             * Every Java thread has an interrupted status, which is essentially a boolean flag.
             * true: An interruption has been requested.
             * false: No interruption has been requested (or the flag has been cleared)
             *
             * What happens to a thread in general when it receives an InterruptedException depends
             * on what it's currently doing. In this case, it is waiting. It stops waiting when
             * interrupted and wakes up immediately. The JVM automatically resets the thread's
             * interrupt status flag back to false. The blocking (wait in this case) method throws
             * an InterruptedException.
             *
             * Why do we restore the interrupt flag here? Because this thread can now return to its
             * executor with its interrupted status preserved. This is important because this REST
             * method that has caught the InterruptedException might not be the last method in the
             * call stack that needs to know about the interruption. This method is effectively
             * saying I caught the interruption, but I don't want to silently discard the fact that
             * it happened. This is called preserving the interrupt status. Whether anything else
             * happens with this interruption depends on higher level code checking the interrupt
             * status. Restoring the flag is the correct defensive practice because the REST resource
             * handles the exception but doesn't own the lifecycle of the HTTP worker thread.
             * The general Java convention is:
             *  - If you can propagate InterruptedException, propagate it.
             *  - If you cannot propagate it (as in this REST resource), restore the interrupt flag.
             */
            return Response.status(503).entity(new TripError("wait_interrupted",
                    "The wait was interrupted. The workflow may still complete; check its status before retrying.")).build();
        }
    }

    @GET
    @Path("/plan/status")
    public Response planStatus(@QueryParam("instanceId") String instanceId, @QueryParam("requestId") String requestId) {
        if ((instanceId == null || instanceId.isBlank()) && (requestId == null || requestId.isBlank())) {
            return Response.status(400).entity(new TripError("invalid_request", "instanceId or requestId is required.")).build();
        }
        TripPlanStatus status = instanceId != null && !instanceId.isBlank()
                ? tripPlanStore.byInstanceId(instanceId) : tripPlanStore.byRequestId(requestId);
        if (status == null) return Response.status(404).entity(new TripError("unknown_trip", "The requested trip was not found.")).build();
        return Response.ok(status).build();
    }

    @GET
    @Path("/plan/latest")
    public Response latestPlan() {
        TripPlanStatus status = tripPlanStore.latest();
        return status == null ? Response.noContent().build() : Response.ok(status).build();
    }

    private Response validatePlanRequest(TripRequest request) {
        if (request == null)
            return bad("Trip details are required.");
        if (request.destination() == null || request.destination().isBlank())
            return bad("Destination is required.");
        return null;
    }

    private static Response bad(String message) {
        return Response.status(400).entity(new TripError("invalid_request", message)).build();
    }
}
