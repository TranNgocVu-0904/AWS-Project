package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class LambdaGetThumbnailObject implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // --- Constants ---
    // The bucket where resized images are stored (Target bucket)
    private static final String RESIZED_BUCKET = "resized-public-cloud1";
    // Name of the Lambda function used to verify tokens
    private static final String CHECKER_FUNCTION_NAME = "LambdaTokenChecker";

    // Initialize S3 Client
    private final S3Client s3Client = S3Client.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();
        
        String originalKey = "";
        try {
            // 1. Parse the incoming JSON Request
            JSONObject bodyJSON = new JSONObject(request.getBody());
            originalKey = bodyJSON.getString("key"); // Expecting format: "user@test.com/hash.jpg"
            String email = bodyJSON.optString("email", "");
            String token = bodyJSON.optString("token", "");
            
            // 2. Security Check: Validate inputs and verify the user's token
            if (email.isEmpty() || token.isEmpty() || !isTokenValid(email, token, logger)) {
                return createResponse(401, "{\"message\":\"Unauthorized\"}", "application/json");
            }
        } catch (Exception e) {
            return createResponse(400, "Invalid JSON", "text/plain");
        }

        // --- KEY TRANSFORMATION LOGIC ---
        // We need to convert the original S3 Key to the Resized S3 Key.
        // Input Example:  "user@test.com/abc12345.jpg"
        // Target Example: "user@test.com/resized-abc12345.jpg"
        
        String resizedKey = originalKey; // Default to original if no folder structure
        int lastSlashIndex = originalKey.lastIndexOf('/');
        
        if (lastSlashIndex >= 0) {
            // Split folder and filename
            String folder = originalKey.substring(0, lastSlashIndex + 1); // Extract "user@test.com/"
            String fileName = originalKey.substring(lastSlashIndex + 1);  // Extract "abc12345.jpg"
            
            // Construct the new key
            resizedKey = folder + "resized-" + fileName;
        } else {
            // Case: File is at the root level (no folder)
            resizedKey = "resized-" + originalKey;
        }

        // --- MIME TYPE DETECTION ---
        // Determine the Content-Type header based on file extension
        String mimeType = "application/octet-stream"; // Default binary type
        int lastDotIndex = resizedKey.lastIndexOf('.');

        if (lastDotIndex > 0) {
            String ext = resizedKey.substring(lastDotIndex + 1).toLowerCase();
            if (ext.equals("png")) 
                mimeType = "image/png";
            else if (ext.equals("jpg") || ext.equals("jpeg")) 
                mimeType = "image/jpeg";
        }

        try {
            // 3. Request the object from the S3 Resized Bucket
            GetObjectRequest s3Request = GetObjectRequest.builder()
                    .bucket(RESIZED_BUCKET)
                    .key(resizedKey) // Use the "resized-" key
                    .build();

            // 4. Read and Encode the Image
            try (ResponseInputStream<GetObjectResponse> s3Response = s3Client.getObject(s3Request)) {
                // Read bytes from S3 stream
                byte[] buffer = s3Response.readAllBytes();
                
                // Convert bytes to Base64 String
                // (Required because API Gateway/Lambda pass binary data as Base64 text)
                String encodedString = Base64.getEncoder().encodeToString(buffer);
                
                // 5. Build the Success Response
                APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
                response.setStatusCode(200);
                response.setBody(encodedString);
                response.withIsBase64Encoded(true); // Tell API Gateway this is binary data
                response.setHeaders(Collections.singletonMap("Content-Type", mimeType));
                return response;
            }
        } catch (Exception e) {
            logger.log("Thumbnail Error: " + e.getMessage());
            return createResponse(404, "Thumbnail not found", "text/plain");
        }
    }

    // --- HELPER METHODS ---

    /**
     * Creates a standard API Gateway Response object.
     */
    private APIGatewayProxyResponseEvent createResponse(int code, String body, String type) {
        APIGatewayProxyResponseEvent res = new APIGatewayProxyResponseEvent();
        res.setStatusCode(code);
        res.setBody(body);
        res.setHeaders(Collections.singletonMap("Content-Type", type));
        return res;
    }
    
    /**
     * Invokes another Lambda function (LambdaTokenChecker) to validate the session token.
     */
    private boolean isTokenValid(String email, String token, LambdaLogger logger) {
        try (LambdaClient lambdaClient = LambdaClient.builder().region(Region.AP_SOUTHEAST_2).build()) {
            // Prepare payload for the checker function
            JSONObject actualData = new JSONObject();
            actualData.put("email", email);
            actualData.put("token", token);
            
            JSONObject proxyPayload = new JSONObject();
            proxyPayload.put("body", actualData.toString()); 
            
            // Call the Lambda function
            InvokeRequest invokeRequest = InvokeRequest.builder()
                    .functionName(CHECKER_FUNCTION_NAME)
                    .payload(SdkBytes.fromUtf8String(proxyPayload.toString()))
                    .build();
            InvokeResponse response = lambdaClient.invoke(invokeRequest);
            
            // Parse the response
            String responseStr = response.payload().asUtf8String();
            JSONObject fullRes = new JSONObject(responseStr);
            if (fullRes.has("body")) {
                JSONObject bodyRes = new JSONObject(fullRes.getString("body"));
                return bodyRes.optBoolean("valid", false);
            }
            return false;
        } catch (Exception e) { return false; }
    }
}