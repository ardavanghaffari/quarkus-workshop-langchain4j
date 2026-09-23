package com.carmanagement.agentic.agents;

import com.carmanagement.model.ApprovalProposal;
import com.carmanagement.service.ApprovalService;
import dev.langchain4j.agentic.declarative.HumanInTheLoop;
import io.quarkus.arc.Arc;
import io.quarkus.logging.Log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface HumanApprovalAgent {

    @HumanInTheLoop(outputKey = "approvalDecision", description = "Coordinates human approval for high-value vehicle dispositions using the requestHumanApproval tool")
    static String reviewDispositionProposal(
            String carMake,
            String carModel,
            Integer carYear,
            Integer carNumber,
            String carValue,
            String dispositionProposal,
            String dispositionReason,
            String carCondition,
            String feedback
    ) {

        Log.infof("🛑 HITL Tool: Creating approval proposal for car %d - %s %s %s",
                carNumber, carYear, carMake, carModel);
        Log.info("⏸️  WORKFLOW PAUSED - Waiting for human approval decision via UI");

        // CDI dependency injection. This Agent is not a Bean. We would have injected the
        // ApprovalService otherwise.
        ApprovalService approvalService = Arc.container().instance(ApprovalService.class).get();

        try {
            // Create proposal and get CompletableFuture that completes when human decides
            CompletableFuture<ApprovalProposal> approvalFuture =
                    approvalService.createProposalAndWaitForDecision(
                            carNumber, carMake, carModel, carYear, carValue,
                            dispositionProposal, dispositionReason, carCondition, feedback
                    );

            // BLOCK HERE until human makes decision (with 5 minute timeout)
            // Note that this agent's execution waits on this future, meaning a thread is hanging
            // for 5 minutes until human input. There is also the SuspendedResponse approach:
            // Avoids blocking a thread while waiting for input. The workflow state waits instead.
            // Checkpoints the agentic state, throws AgenticSystemSuspendedException and releases
            // the calling thread. Two things get checkpointed, the AgenticScope and the planner
            // state. The former contains the state shared by the agents, agent invocations and
            // their results. The latter records where the workflow was when it got suspended.
            // The current LangChain4j implementation stores all this in an AgenticScopeStore which
            // can be backed by a database. The crucial point is that the JVM thread isn't the state
            // of the workflow anymore. The database/store contains enough information to reconstruct
            // it. When input arrives, we send `completePendingResponse()` and resume the workflow.
            ApprovalProposal result = approvalFuture.get(5, TimeUnit.MINUTES);

            Log.infof("▶️  WORKFLOW RESUMED - Human decision received: %s", result.decision);

            // Format response for the agent
            return String.format("""
                Human Decision: %s
                Reason: %s
                Approved By: %s
                Decision Time: %s
                """,
                    result.decision,
                    result.approvalReason != null ? result.approvalReason : "No reason provided",
                    result.approvedBy != null ? result.approvedBy : "Unknown",
                    result.decidedAt != null ? result.decidedAt.toString() : "Unknown"
            );

        } catch (TimeoutException e) {
            Log.error("⏱️  TIMEOUT: No human decision received within 5 minutes, defaulting to REJECTED");
            return """
                Human Decision: REJECTED
                Reason: Timeout - No human decision received within 5 minutes. Defaulting to rejection for safety.
                Approved By: System (Timeout)
                """;
        } catch (Exception e) {
            Log.errorf(e, "❌ ERROR: Failed to get human approval for car %d", carNumber);
            return String.format("""
                Human Decision: REJECTED
                Reason: Error occurred while waiting for human approval: %s
                Approved By: System (Error)
                """, e.getMessage());
        }
    }
}
