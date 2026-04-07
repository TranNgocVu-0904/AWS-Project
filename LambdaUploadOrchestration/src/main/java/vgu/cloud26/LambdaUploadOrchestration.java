package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.util.Collections;

import org.json.JSONObject;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaUploadOrchestration
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final String CHECKER_FUNCTION_NAME = "LambdaTokenChecker";
    private final String UPLOAD_FUNCTION_NAME  = "LambdaUploadObject";
    private final String INSERT_FUNCTION_NAME  = "LambdaInsertPhoto";
    private final String RESIZE_FUNCTION_NAME  = "LambdaResizerDirect";

    // --- Optimization: Initialize Lambda Client once ---
    // (Reusing the client across invocations improves performance)
    private final LambdaClient lambdaClient = LambdaClient.builder()
                                .region(Region.AP_SOUTHEAST_2)
                                .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

        LambdaLogger logger = context.getLogger();

        try {
            // 1. Parse Input Body
            String bodyStr = request.getBody();

            if (bodyStr == null || bodyStr.isEmpty()) {
                return createResponse(400, "{\"message\":\"Empty request body\"}");
            }

            JSONObject body = new JSONObject(bodyStr);

            // ==========================================
            // 0. WARM-UP LOGIC (Anti-Cold Start)
            // ==========================================
            // If the request contains "warmup": true, we wake up all child functions asynchronously.
            boolean isWarmup = body.optBoolean("warmup", false);
            if (isWarmup) {
                logger.log("Orchestrator: Chain Warm-up triggered.");
                
                // Prepare a dummy payload for children
                JSONObject childWarmupBody = new JSONObject().put("warmup", true);
                String childEvent = new JSONObject().put("body", childWarmupBody.toString()).toString();
                SdkBytes warmPayload = SdkBytes.fromUtf8String(childEvent);

                // Fire-and-Forget: Warm-up functions
                fireAndForget(INSERT_FUNCTION_NAME, warmPayload);
                fireAndForget(UPLOAD_FUNCTION_NAME, warmPayload);
                fireAndForget(RESIZE_FUNCTION_NAME, warmPayload);

                return createResponse(200, "{\"message\": \"Orchestrator and children warmed up!\"}");
            }

            // ==========================================
            // 1. INPUT VALIDATION & TOKEN CHECK
            // ==========================================
            String email       = body.optString("email", "");
            String token       = body.optString("token", "");
            String content     = body.getString("content"); // Base64 Image
            String key         = body.getString("key");     // Filename     
            String description = body.optString("description", "");

            if (email.isEmpty() || token.isEmpty()) {
                return createResponse(401, "{\"message\":\"Email and Token are required\"}");
            }

            // Call Helper Lambda to verify token
            if (!isTokenValid(email, token, logger)) {
                logger.log("Authentication failed for email: " + email);
                return createResponse(401, "{\"message\":\"Unauthorized: Invalid Token\"}");
            }

            // ==========================================
            // 2. CALL LAMBDA INSERT (Database Step)
            // ==========================================
            // Prepare payload for Insert Function
            JSONObject insertInnerBody = new JSONObject();
            insertInnerBody.put("description", description);
            insertInnerBody.put("s3Key", key);
            insertInnerBody.put("email", email); // Pass email for DB ownership

            // Wrap in "body" to simulate API Gateway Event structure
            JSONObject insertEvent = new JSONObject();
            insertEvent.put("body", insertInnerBody.toString());

            // Synchronous Call (Wait for DB to finish)
            InvokeResponse insertRes = invokeLambdaSync(INSERT_FUNCTION_NAME, insertEvent.toString());
            String insertPayloadStr = insertRes.payload().asUtf8String();
            
            // Parse DB Response
            JSONObject insertOuterJson = new JSONObject(insertPayloadStr);
            int innerInsertStatus = insertOuterJson.optInt("statusCode", 200);
            String insertBodyStr  = insertOuterJson.optString("body", "{}");

            // Stop pipeline if DB fails
            if (innerInsertStatus != 200) {
                String errorMsg = extractMessage(insertBodyStr);
                return createResponse(innerInsertStatus, new JSONObject().put("message", "Insert Failed: " + errorMsg).toString());
            }

            JSONObject insertJson = new JSONObject(insertBodyStr); 

            // ==========================================
            // 3. CALL LAMBDA UPLOAD (S3 Storage Step)
            // ==========================================
            JSONObject uploadInnerBody = new JSONObject();
            uploadInnerBody.put("content", content);
            uploadInnerBody.put("key", key);
            uploadInnerBody.put("email", email); // Pass email for Folder creation (email/hash.jpg)

            JSONObject uploadEvent = new JSONObject();
            uploadEvent.put("body", uploadInnerBody.toString());

            // Synchronous Call (Wait for Upload to finish)
            InvokeResponse uploadRes = invokeLambdaSync(UPLOAD_FUNCTION_NAME, uploadEvent.toString());
            String uploadPayloadStr = uploadRes.payload().asUtf8String();

            JSONObject uploadOuterJson = new JSONObject(uploadPayloadStr);
            int innerUploadStatus = uploadOuterJson.optInt("statusCode", 200);
            String uploadBodyStr  = uploadOuterJson.optString("body", "{}");

            // Stop pipeline if Upload fails
            if (innerUploadStatus != 200) {
                String errorMsg = extractMessage(uploadBodyStr);
                return createResponse(innerUploadStatus, new JSONObject().put("message", "Upload Failed: " + errorMsg).toString());
            }

            JSONObject uploadJson = new JSONObject(uploadBodyStr);
            String s3Key = uploadJson.getString("key"); 
            String hashKeyFromUpload = uploadJson.optString("hashKey", "");

            // ==========================================
            // 4. CALL LAMBDA RESIZE (Thumbnail Step)
            // ==========================================
            JSONObject resizePayload = new JSONObject();
            resizePayload.put("content", content); 
            resizePayload.put("key", key);       
            resizePayload.put("email", email);     

            JSONObject resizeEvent = new JSONObject();
            resizeEvent.put("body", resizePayload.toString());

            // Synchronous Call (Wait for Resizer)
            InvokeResponse resizeRes = invokeLambdaSync(RESIZE_FUNCTION_NAME, resizeEvent.toString());
            String resizeRaw = resizeRes.payload().asUtf8String();

            JSONObject resizeOuterJson = new JSONObject(resizeRaw);
            int innerResizeStatus = resizeOuterJson.optInt("statusCode", 200);
            String resizeBodyStr  = resizeOuterJson.optString("body", "{}");

            if (innerResizeStatus != 200) {
                String errorMsg = extractMessage(resizeBodyStr);
                return createResponse(innerResizeStatus, new JSONObject().put("message", "Resize Failed: " + errorMsg).toString());
            }

            JSONObject resizeJson = new JSONObject(resizeBodyStr);

            // ==========================================
            // 5. FINAL SUCCESS RESPONSE
            // ==========================================
            // Consolidate reports from all steps into one JSON
            JSONObject finalJson = new JSONObject();
            finalJson.put("message", "Upload image successfully");
            finalJson.put("email", email);

            // Insert Report
            JSONObject insertResult = new JSONObject();
            insertResult.put("status", "Success");
            finalJson.put("step1_insert", insertResult);

            // Upload Report
            JSONObject uploadResult = new JSONObject();
            uploadResult.put("status", "Success");
            uploadResult.put("s3Key", s3Key);
            uploadResult.put("hashKey", hashKeyFromUpload);
            finalJson.put("step2_upload", uploadResult);

            // Resize Report
            JSONObject resizeResult = new JSONObject();
            resizeResult.put("status", "Success");
            resizeResult.put("resizedKey", resizeJson.optString("resizedKey", ""));
            finalJson.put("step3_resize", resizeResult);

            return createResponse(200, finalJson.toString());

        } catch (Exception ex) {
            logger.log("Error in Orchestrator: " + ex.toString());
            return createResponse(500, "{\"message\":\"Error in Orchestrator: " + ex.getMessage() + "\"}");
        }
    }

    // ==========================================
    // HELPER METHODS
    // ==========================================

    /**
     * Helper to validate user token via LambdaTokenChecker
     */
    private boolean isTokenValid(String email, String token, LambdaLogger logger) {
        try {
            JSONObject actualData = new JSONObject();
            actualData.put("email", email);
            actualData.put("token", token);

            // Wrap in "body" so the Checker (which expects API Gateway event) can read it
            JSONObject proxyPayload = new JSONObject();
            proxyPayload.put("body", actualData.toString());

            InvokeResponse response = invokeLambdaSync(CHECKER_FUNCTION_NAME, proxyPayload.toString());
            String responseStr = response.payload().asUtf8String();
            
            logger.log("Checker Response: " + responseStr);

            JSONObject fullRes = new JSONObject(responseStr);
            if (fullRes.has("body")) {
                JSONObject bodyRes = new JSONObject(fullRes.getString("body"));
                
                return bodyRes.optBoolean("valid", false);
            }

            return false;
        } catch (Exception e) {
            logger.log("Authentication call error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Synchronous Invocation (RequestResponse): Waits for the child function to return.
     */
    private InvokeResponse invokeLambdaSync(String functionName, String payloadString) {
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(SdkBytes.fromUtf8String(payloadString))
                .build(); // Default InvocationType is RequestResponse (Sync)
        return lambdaClient.invoke(request);
    }

    /**
     * Asynchronous Invocation (Event): Does not wait. Used for Warmup.
     */
    private void fireAndForget(String functionName, SdkBytes payload) {
        try {
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .invocationType(software.amazon.awssdk.services.lambda.model.InvocationType.EVENT)
                    .payload(payload)
                    .build();
            lambdaClient.invoke(request);
        } catch (Exception e) {
            // Log errors but do not crash the main code
        }
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        response.setBody(body);
        response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        response.setIsBase64Encoded(false);
        return response;
    }

    // Safely extract "message" from a JSON string for error reporting
    private String extractMessage(String jsonBody) {
        try {
            JSONObject json = new JSONObject(jsonBody);
            return json.optString("message", "Unknown error");
        } catch (Exception e) {
            return jsonBody;
        }
    }
}