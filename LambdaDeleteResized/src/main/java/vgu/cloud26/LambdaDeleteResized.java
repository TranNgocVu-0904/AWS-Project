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

public class LambdaDeleteResized 
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // Target Bucket for Thumbnails
    private final String BUCKET_RESIZED = "resized-public-cloud1";

    // Initialize S3 Client once
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

            // 1. INPUT PARSING
            // Handle potentially nested JSON structure (Orchestrator vs Direct Invocation)
            JSONObject outer = new JSONObject(requestBody);
            JSONObject bodyJSON;
            if (outer.has("body") && outer.get("body") instanceof String) {
                bodyJSON = new JSONObject(outer.getString("body"));
            } else {
                bodyJSON = outer;
            }

            // 2. EXTRACT ORIGINAL KEY
            // We receive the original file path (e.g., "user@test.com/hash.jpg")
            String originalKey = bodyJSON.optString("s3Key", null);
            if (originalKey == null) {
                originalKey = bodyJSON.optString("key", null);
            }

            if (originalKey == null || originalKey.isEmpty()) {
                return createResponse(400, "Missing 's3Key' parameter");
            }

            // ==================================================================
            // 3. TARGET KEY RECONSTRUCTION LOGIC
            // The file structure for resized images is: "folder/resized-filename.ext"
            // We need to transform the input: "ngocvu@gmail.com/anh.png"
            // INTO the target: "ngocvu@gmail.com/resized-anh.png"
            // ==================================================================
            
            String targetKey;
            int lastSlashIndex = originalKey.lastIndexOf('/');

            if (lastSlashIndex != -1) {
                // CASE A: File is inside a folder (Standard case)
                // 1. Extract the folder path (e.g., "ngocvu@gmail.com/")
                String folderPath = originalKey.substring(0, lastSlashIndex + 1);
                
                // 2. Extract the file name (e.g., "anh.png")
                String fileName = originalKey.substring(lastSlashIndex + 1);
                
                // 3. Combine: Folder + Prefix "resized-" + Filename
                targetKey = folderPath + "resized-" + fileName;
            } else {
                // CASE B: File is at the root (No folder)
                targetKey = "resized-" + originalKey;
            }
            
            logger.log("Deleting resized object. Original: " + originalKey + " -> Target: " + targetKey);

            // 4. EXECUTE DELETE ON RESIZED BUCKET
            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                    .bucket(BUCKET_RESIZED)
                    .key(targetKey) // Deleting the reconstructed key
                    .build();

            s3Client.deleteObject(deleteReq);

            return createResponse(200, "Deleted resized object: " + targetKey);

        } catch (S3Exception e) {
            logger.log("S3 Error: " + e.awsErrorDetails().errorMessage());
            return createResponse(500, "S3 Error: " + e.awsErrorDetails().errorMessage());
            
        } catch (Exception e) {
            logger.log("General Error: " + e.toString());
            return createResponse(500, "Error: " + e.getMessage());
        }
    }

    // --- HELPER: RESPONSE GENERATION ---
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String message) {
        JSONObject responseBody = new JSONObject();
        responseBody.put("message", message);
        
        // Add status flag for Orchestrator parsing
        responseBody.put("status", statusCode == 200 ? "SUCCESS" : "ERROR");

        Map<String, String> headers = Collections.singletonMap("Content-Type", "application/json");
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(responseBody.toString());
    }
}