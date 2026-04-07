package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import org.json.JSONObject;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Collections;
import java.util.Map;

public class LambdaDeleteObject 
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // HARDCODED Bucket Name for Original Images
    private final String BUCKET_NAME = "public-cloud1";

    // Initialize S3 Client (Best practice: Create once outside the handler)
    private final S3Client s3Client = S3Client.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {

        LambdaLogger logger = context.getLogger();
        
        try {
            String requestBody = event.getBody();
            if (requestBody == null || requestBody.isEmpty()) {
                return createResponse(400, "Request body is empty");
            }

            // 1. ROBUST INPUT PARSING
            // The input might come directly from API Gateway OR via the Orchestrator.
            // Sometimes it arrives as a nested JSON string (body inside body), so we handle both cases.
            JSONObject outer = new JSONObject(requestBody);
            
            JSONObject bodyJSON;
            if (outer.has("body") && outer.get("body") instanceof String) {
                // Case: Input is double-encoded JSON (common when invoking from another Lambda)
                bodyJSON = new JSONObject(outer.getString("body"));
            } 
            else {
                // Case: Input is standard JSON
                bodyJSON = outer;
            }

            // 2. EXTRACT S3 KEY
            // "s3Key" is the standard parameter name sent by the Orchestrator.
            // Example value: "user@test.com/a1b2c3d4.jpg"
            String objectKey = bodyJSON.optString("s3Key", null);

            // Fallback (Optional): Check for "key" parameter if "s3Key" is missing
            // This maintains backward compatibility with older versions or direct calls.
            if (objectKey == null) {
                objectKey = bodyJSON.optString("key", null);
            }

            if (objectKey == null || objectKey.isEmpty()) {
                return createResponse(400, "Missing 's3Key' parameter in request");
            }

            logger.log("Deleting object from bucket '" + BUCKET_NAME + "': " + objectKey);

            // 3. EXECUTE S3 DELETE
            // Note: 'objectKey' must be the FULL PATH (e.g., "folder/file.ext").
            // S3 treats the full path as the unique Key ID.
            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(objectKey)
                    .build();

            s3Client.deleteObject(deleteReq);

            logger.log("Deleted successfully: " + objectKey);
            
            return createResponse(200, "Deleted object: " + objectKey);

        } catch (S3Exception e) {
            // Specific handling for AWS S3 errors (e.g., Bucket not found, Permissions)
            logger.log("S3 Error: " + e.awsErrorDetails().errorMessage());
            return createResponse(500, "S3 Error: " + e.awsErrorDetails().errorMessage());

        } catch (Exception e) {
            // General error handling
            logger.log("General Error: " + e.toString());
            return createResponse(500, "Error: " + e.getMessage());
        }
    }

    // --- HELPER: CONSTRUCT RESPONSE ---
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String message) {
        JSONObject responseBody = new JSONObject();
        responseBody.put("message", message);
        
        // Add status to body for easier parsing by the Orchestrator
        responseBody.put("status", statusCode == 200 ? "SUCCESS" : "ERROR");

        Map<String, String> headers = Collections.singletonMap("Content-Type", "application/json");

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(responseBody.toString());
    }
}