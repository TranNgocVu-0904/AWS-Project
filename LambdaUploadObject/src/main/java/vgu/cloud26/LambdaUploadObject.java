package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;

import org.json.JSONObject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class LambdaUploadObject implements
        RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {

        String bucketName = "public-cloud1";
        String requestBody = event.getBody();

        // 1. Parse JSON Input
        JSONObject bodyJSON = new JSONObject(requestBody);
        
        // The Base64 encoded string of the file content
        String content = bodyJSON.getString("content");
        
        // The original file name (e.g., "my_photo.jpg")
        String originalKey = bodyJSON.getString("key");
        
        // The user's email (Crucial for creating user-specific folders)
        String email = bodyJSON.optString("email", "unknown_user");

        // 2. GENERATE NEW S3 KEY (Hashing logic)
        // Transformation: "my_photo.jpg" -> "user@test.com/<hash_value>.jpg"
        // This ensures unique filenames and organizes files by user folder.
        String finalS3Key = generateHashedKey(email, originalKey);

        context.getLogger().log("Uploading: " + originalKey + " -> To: " + finalS3Key);

        // 3. Upload to S3 using the New Key
        // Decode the Base64 content back to raw bytes
        byte[] objBytes = Base64.getDecoder().decode(content.getBytes());
        
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(finalS3Key)
                .build();

        S3Client s3Client = S3Client.builder()
                .region(Region.AP_SOUTHEAST_2)
                .build();
        
        // Execute the upload
        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(objBytes));

        // 4. BUILD JSON RESPONSE
        // We must return the 'finalS3Key' so the Orchestrator knows where the file is.
        JSONObject respJson = new JSONObject();
        respJson.put("message", "Object uploaded successfully");
        
        // Return the actual path on S3 (used for displaying the image)
        respJson.put("key", finalS3Key); 
        
        // Return the original name (useful for UI display if needed)
        respJson.put("originalName", originalKey);

        // Return the hash of the full path (legacy support or verification)
        respJson.put("hashKey", sha256Hex(finalS3Key));

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(200);
        response.setBody(respJson.toString());
        response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        response.withIsBase64Encoded(false);
        return response;
    }

    // ====== FILE NAMING LOGIC (HASH + FOLDER) ======
    /**
     * Constructs the S3 path: "email/hashed_filename.extension"
     */
    private String generateHashedKey(String email, String originalFileName) {
        // 1. Find the last dot to separate name and extension
        int lastDotIndex = originalFileName.lastIndexOf('.');
        
        String namePart;
        String extensionPart;

        if (lastDotIndex == -1) {
            namePart = originalFileName;
            extensionPart = ""; 
        } 
        else {
            namePart = originalFileName.substring(0, lastDotIndex);
            extensionPart = originalFileName.substring(lastDotIndex); // e.g., ".jpg"
        }

        // 2. Hash the filename part (e.g., "my_photo" -> "a1b2c3...")
        String hashedName = sha256Hex(namePart); 
        
        // Note: If you want completely random names (UUID), swap the line above with:
        // String hashedName = java.util.UUID.randomUUID().toString();

        // 3. Combine: email + / + hash + extension
        return email + "/" + hashedName + extensionPart;
    }

    // ====== HELPER: SHA-256 HASHING ======
    /**
     * Computes SHA-256 hash of a string and returns it as a Hex string.
     */
    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));  // Convert byte to 2-digit hex
            }
            return sb.toString(); 
            
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }
    }
}