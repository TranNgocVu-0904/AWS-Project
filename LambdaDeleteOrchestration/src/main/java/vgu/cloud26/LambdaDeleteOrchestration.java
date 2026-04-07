package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*;
import org.json.JSONObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.*;
import software.amazon.awssdk.services.lambda.model.*;

import java.util.Collections;
import java.util.Map;

public class LambdaDeleteOrchestration
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // --- Configuration: Child Function Names ---
    private static final String DELETE_S3_ORIGINAL = "LambdaDeleteObject";
    private static final String DELETE_S3_RESIZED  = "LambdaDeleteResized"; // Must match your Resize deletion function name
    private static final String DELETE_DB_FUNCTION_NAME = "LambdaDeletePhoto";
    
    // Name of the Token Checker function
    private static final String CHECKER_FUNCTION_NAME = "LambdaTokenChecker";

    // Initialize Lambda Client once for performance
    private final LambdaClient lambdaClient = LambdaClient.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        
        LambdaLogger logger = context.getLogger();
        Map<String, String> jsonHeader = Collections.singletonMap("Content-Type", "application/json");

        try {
            String bodyStr = request.getBody();
            if (bodyStr == null || bodyStr.isEmpty()) throw new Exception("Request body is empty");

            // Handle Nested JSON (Sometimes Frontend sends stringified JSON inside "body")
            JSONObject outer = new JSONObject(bodyStr);
            JSONObject body = (outer.has("body") && outer.get("body") instanceof String) 
                              ? new JSONObject(outer.getString("body")) : outer;

            // ===== 1. WARM-UP LOGIC (Anti-Cold Start) =====
            boolean isWarmup = body.optBoolean("warmup", false);
            if (isWarmup) {
                logger.log("Orchestrator: Chain Warm-up triggered.");
                JSONObject childWarmupBody = new JSONObject().put("warmup", true);
                
                // Wrap in "body" structure for API Gateway event simulation
                String childEvent = new JSONObject().put("body", childWarmupBody.toString()).toString();
                SdkBytes warmPayload = SdkBytes.fromUtf8String(childEvent);

                // Fire-and-Forget: Wake up all child functions asynchronously
                fireAndForget(DELETE_S3_ORIGINAL, warmPayload);
                fireAndForget(DELETE_S3_RESIZED, warmPayload);
                fireAndForget(DELETE_DB_FUNCTION_NAME, warmPayload);

                return createResponse(200, "{\"message\": \"Delete system warmed up OK\"}");
            }

            // ===== 2. GET DATA FROM REQUEST =====
            String s3Key = body.optString("s3Key", ""); // e.g., "user@test.com/myphoto.jpg"
            String email = body.optString("email", "");
            String token = body.optString("token", "");

            if (s3Key.isEmpty()) return createResponse(400, "{\"message\":\"Missing s3Key\"}");
            if (email.isEmpty() || token.isEmpty()) return createResponse(401, "{\"message\":\"Missing email or token\"}");

            logger.log("Request delete for user: " + email + " | Key: " + s3Key);

            // ===== 3. SECURITY CHECK 1: VERIFY TOKEN =====
            // Call LambdaTokenChecker to ensure the user is logged in
            if (!isTokenValid(email, token, logger)) {
                logger.log("Token verification failed for: " + email);
                return createResponse(401, "{\"message\":\"Unauthorized: Invalid Token\"}");
            }

            // ===== 4. SECURITY CHECK 2: OWNERSHIP CHECK =====
            // CRITICAL: Ensure the user is only deleting their OWN files.
            // We check if the file path starts with the user's email folder.
            if (!s3Key.startsWith(email + "/")) {
                logger.log("SECURITY ALERT: User " + email + " tried to delete file: " + s3Key);
                return createResponse(403, "{\"message\":\"Forbidden: You can only delete your own files\"}");
            }

            // ===== 5. PREPARE PAYLOAD FOR CHILDREN =====
            // Package the data to send to child functions
            JSONObject childData = new JSONObject();
            childData.put("s3Key", s3Key);
            childData.put("email", email); 

            // Simulate API Gateway Event structure for child functions
            String childEventStr = new JSONObject()
                    .put("body", childData.toString())
                    .toString();
            SdkBytes payloadBytes = SdkBytes.fromUtf8String(childEventStr);

            // ===== 6. EXECUTE DELETE PIPELINE =====
            
            // STEP 1: DELETE ORIGINAL IMAGE (From Main Bucket)
            InvokeResponse resOrig = lambdaClient.invoke(InvokeRequest.builder().functionName(DELETE_S3_ORIGINAL).payload(payloadBytes).build());
            JSONObject outOrig = parseChildResponse(resOrig);

            // STEP 2: DELETE RESIZED IMAGE (From Thumbnail Bucket)
            InvokeResponse resResized = lambdaClient.invoke(InvokeRequest.builder().functionName(DELETE_S3_RESIZED).payload(payloadBytes).build());
            JSONObject outResized = parseChildResponse(resResized);

            // STEP 3: DELETE DB RECORD (From RDS)
            InvokeResponse resDb = lambdaClient.invoke(InvokeRequest.builder().functionName(DELETE_DB_FUNCTION_NAME).payload(payloadBytes).build());
            JSONObject outDb = parseChildResponse(resDb);

            // ===== 7. SUMMARY & RESPONSE =====
            // Consolidate results into a final report
            JSONObject finalJson = new JSONObject();
            String summary = "Results:\n"
                + "Original: " + outOrig.optString("status") + "\n"
                + "Resized: " + outResized.optString("status") + "\n"
                + "DB: " + outDb.optString("status");

            finalJson.put("message", summary);
            finalJson.put("deleteOriginal", outOrig);
            finalJson.put("deleteResized", outResized);
            finalJson.put("deleteDb", outDb);
            
            // Determine overall success (Original S3 + DB are the most critical)
            boolean isSuccess = "SUCCESS".equals(outOrig.optString("status")) && "SUCCESS".equals(outDb.optString("status"));
            finalJson.put("overallStatus", isSuccess ? "SUCCESS" : "PARTIAL_ERROR");

            return createResponse(200, finalJson.toString());

        } catch (Exception ex) {
            logger.log("Error in Delete Orchestrator: " + ex.toString());
            return createResponse(500, "{\"message\": \"Error in Orchestrator: " + ex.getMessage() + "\"}");
        }
    }

    // --- HELPER 1: CALL TOKEN CHECKER FUNCTION ---
    private boolean isTokenValid(String email, String token, LambdaLogger logger) {
        try {
            JSONObject checkBody = new JSONObject();
            checkBody.put("email", email);
            checkBody.put("token", token);
            
            String payloadStr = new JSONObject().put("body", checkBody.toString()).toString();

            InvokeResponse response = lambdaClient.invoke(InvokeRequest.builder()
                    .functionName(CHECKER_FUNCTION_NAME)
                    .payload(SdkBytes.fromUtf8String(payloadStr))
                    .build());

            int statusCode = response.statusCode(); // AWS Lambda Invocation Status
            
            // Parse the actual application response from the Checker
            String responsePayload = response.payload().asUtf8String();
            JSONObject jsonResp = new JSONObject(responsePayload);
            int functionStatus = jsonResp.optInt("statusCode", 500); // Checker Logic Status

            // Return true only if invocation worked AND checker returned 200 OK
            return (statusCode == 200 && functionStatus == 200);

        } catch (Exception e) {
            logger.log("Token check failed due to exception: " + e.getMessage());
            return false;
        }
    }

    // --- HELPER 2: PARSE CHILD FUNCTION RESPONSES ---
    private JSONObject parseChildResponse(InvokeResponse response) {
        JSONObject result = new JSONObject();
        try {
            String payload = response.payload().asUtf8String();
            JSONObject jsonResp = new JSONObject(payload);
            int statusCode = jsonResp.optInt("statusCode", 500);
            
            // Extract the inner message body
            JSONObject body = new JSONObject(jsonResp.optString("body", "{}"));
            
            result.put("statusCode", statusCode);
            result.put("status", statusCode == 200 ? "SUCCESS" : "ERROR");
            result.put("message", body.optString("message", ""));
        } catch (Exception e) {
            result.put("statusCode", 500);
            result.put("status", "ERROR");
            result.put("message", "Parse error: " + e.getMessage());
        }
        return result;
    }

    // --- HELPER 3: CREATE STANDARD RESPONSE ---
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        response.setBody(body);
        response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        return response;
    }

    // --- HELPER 4: ASYNC INVOCATION (Warm-up) ---
    private void fireAndForget(String functionName, SdkBytes payload) {
        try {
            lambdaClient.invoke(InvokeRequest.builder()
                    .functionName(functionName)
                    .invocationType(InvocationType.EVENT) // Asynchronous
                    .payload(payload)
                    .build());
        } catch (Exception e) {
            // Ignore errors during warm-up
        }
    }
}