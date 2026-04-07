package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Collections;
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
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class LambdaGetObject implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // Name of the helper Lambda function for token verification
    private static final String CHECKER_FUNCTION_NAME = "LambdaTokenChecker";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();
        
        // 1. Parse Input Data
        String requestBody = request.getBody();
        JSONObject bodyJSON = new JSONObject(requestBody);
        String key = bodyJSON.getString("key"); // The file path in S3 (e.g., "user/image.jpg")

        String email = bodyJSON.optString("email", "");
        String token = bodyJSON.optString("token", "");

        // 2. Security Check: Validate Token
        if (!isTokenValid(email, token, logger)) {
            APIGatewayProxyResponseEvent errorRes = new APIGatewayProxyResponseEvent();
            errorRes.setStatusCode(401);
            errorRes.setBody("{\"message\":\"Unauthorized: Invalid Token\"}");
            errorRes.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            return errorRes;
        }

        // 3. Initialize S3 Configuration
        String bucketName = "public-cloud1";
        S3Client s3Client = S3Client.builder()
                .region(Region.AP_SOUTHEAST_2)
                .build();

        // 4. List Objects Logic (Preserved as requested)
        // Instead of getting the object directly, we list all objects to check existence and size first.
        ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build();

        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        
        int maxSize = 10 * 1024 * 1024; // 10 MB Limit
        Boolean found = false;
        Boolean validSize = false;
        String mimeType = "application/octet-stream"; // Default binary type

        // 5. Iterate through S3 Objects to find the target key
        for (S3Object object : objects) {
            if (object.key().equals(key)) {
                found = true;
                
                // Check file size
                int objectSize = Math.toIntExact(object.size());
                if (objectSize < maxSize){
                    validSize = true;
                }
                
                // --- MIME TYPE DETECTION ---
                // Detect file type based on extension (png, jpg, html)
                int lastDotIndex = key.lastIndexOf('.');
                if (lastDotIndex > 0) {
                    String ext = key.substring(lastDotIndex + 1).toLowerCase();
                    
                    if (ext.equals("png")) {
                        mimeType = "image/png";
                    } else if (ext.equals("jpg") || ext.equals("jpeg")) {
                        mimeType = "image/jpeg";
                    } else if (ext.equals("html") || ext.equals("htm")) {
                        mimeType = "text/html";
                    }
                }
                break; // Stop loop once found
            }
        }

        String encodedString = "";
        
        // 6. Download and Encode (Only if found and size is valid)
        if (found && validSize) {
            GetObjectRequest s3Request = GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build();
            
            try (ResponseInputStream<GetObjectResponse> s3Response = s3Client.getObject(s3Request)) {
                // Read binary data
                byte[] buffer = s3Response.readAllBytes();
                // Convert to Base64 (Required for transferring binary data via JSON/API Gateway)
                encodedString = Base64.getEncoder().encodeToString(buffer);
            } catch (IOException ex) {
                context.getLogger().log("IOException: " + ex);
            }
        } 
        else {
            // Handle Error: File not found or File too large
            APIGatewayProxyResponseEvent errorRes = new APIGatewayProxyResponseEvent();
            errorRes.setStatusCode(404);
            errorRes.setBody("{\"message\":\"File not found or too large\"}");
            errorRes.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
            return errorRes;
        }

        // 7. Construct Success Response
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(200);
        response.setBody(encodedString);
        response.withIsBase64Encoded(true); // Signal that body is Base64 encoded
        response.setHeaders(Collections.singletonMap("Content-Type", mimeType));
        return response;
    }

    /**
     * Helper method to call the LambdaTokenChecker function
     */
    private boolean isTokenValid(String email, String token, LambdaLogger logger) {
        if (email.isEmpty() || token.isEmpty()) return false;
        
        try (LambdaClient lambdaClient = LambdaClient.builder().region(Region.AP_SOUTHEAST_2).build()) {
            
            // Prepare payload
            JSONObject actualData = new JSONObject();
            actualData.put("email", email);
            actualData.put("token", token);

            JSONObject proxyPayload = new JSONObject();
            proxyPayload.put("body", actualData.toString()); 

            // Invoke Checker Lambda
            InvokeRequest invokeRequest = InvokeRequest.builder()
                    .functionName(CHECKER_FUNCTION_NAME)
                    .payload(SdkBytes.fromUtf8String(proxyPayload.toString()))
                    .build();

            InvokeResponse response = lambdaClient.invoke(invokeRequest);
            String responseStr = response.payload().asUtf8String();
            
            // Parse result
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
}