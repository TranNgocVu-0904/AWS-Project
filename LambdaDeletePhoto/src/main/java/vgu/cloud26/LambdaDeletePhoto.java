package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.json.JSONObject;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

public class LambdaDeletePhoto implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // === MySQL RDS Configuration ===
    private static final String RDS_INSTANCE_HOSTNAME = "objectdatabase.c3mo42ucag2n.ap-southeast-2.rds.amazonaws.com";
    private static final int RDS_INSTANCE_PORT = 3306;
    private static final String DB_USER = "cloud26";
    private static final String JDBC_URL = "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/Cloud26";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();

        try {
            String requestBody = request.getBody();
            if (requestBody == null || requestBody.isEmpty()) {
                throw new Exception("Request body is empty");
            }
            
            logger.log("LambdaDeletePhoto Input: " + requestBody);

            // Parse Input: Handle potential double-stringified JSON from API Gateway/Orchestrator
            JSONObject outer = new JSONObject(requestBody);
            JSONObject bodyJson = (outer.has("body") && outer.get("body") instanceof String) 
                                  ? new JSONObject(outer.getString("body")) : outer;

            // 1. EXTRACT PARAMETERS
            // fullPath example: "user@gmail.com/255a985...38b3.png"
            // This is the physical path on S3 passed down from the Orchestrator
            String fullPath = bodyJson.optString("s3Key", null); 
            String email = bodyJson.optString("email", null);
        
            if (fullPath == null || fullPath.isEmpty()) {
                return createResponse(400, "Missing s3Key");
            }
            if (email == null || email.isEmpty()) {
                return createResponse(400, "Missing email");
            }

            // 2. EXTRACT HASH KEY FROM S3 PATH (CRITICAL LOGIC)
            // Instead of calculating the hash again, we parse it from the filename.
            // Why? Because we stored the Hash in the 'HashKey' column in the DB.
            
            // Step A: Remove the folder path (email prefix)
            // "email/hash.png" -> "hash.png"
            String fileName = fullPath;
            if (fullPath.contains("/")) {
                fileName = fullPath.substring(fullPath.lastIndexOf("/") + 1);
            }

            // Step B: Remove the file extension to get the raw Hash
            // "hash.png" -> "hash"
            String hashKeyFromS3;
            
            int lastDotIndex = fileName.lastIndexOf('.');
            if (lastDotIndex != -1) {
                hashKeyFromS3 = fileName.substring(0, lastDotIndex);
            } 
            else {
                // Fallback for files without extensions
                hashKeyFromS3 = fileName;
            }
            
            logger.log("Extracted HashKey from path: " + hashKeyFromS3 + " | Email: " + email);

            // 3. EXECUTE DELETE IN DATABASE
            Class.forName("com.mysql.cj.jdbc.Driver");
            int rowsDeleted;
            
            // Connect using IAM Auth (secure, no static password)
            try (Connection conn = DriverManager.getConnection(JDBC_URL, setMySqlConnectionProperties())) {
                
                // Delete the record where the stored HashKey matches our extracted hash
                // AND ensure the email matches (security check)
                String sql = "DELETE FROM Photos WHERE HashKey = ? AND Email = ?";
                
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, hashKeyFromS3); // The extracted hash string
                    ps.setString(2, email);
                    
                    rowsDeleted = ps.executeUpdate();
                }
            }

            // 4. RETURN RESULT
            JSONObject result = new JSONObject();
            if (rowsDeleted > 0) {
                result.put("message", "Deleted from Photos successfully");
                result.put("status", "SUCCESS");
            } 
            else {
                // If 0 rows deleted, it means the Hash didn't exist OR the user doesn't own it
                result.put("message", "No record found (HashKey mismatch or Wrong Owner)");
                result.put("status", "NOT_FOUND");
                logger.log("Warning: DB Delete failed. Extracted Hash: " + hashKeyFromS3);
            }
            result.put("rowsDeleted", rowsDeleted);
            result.put("extractedHash", hashKeyFromS3);

            return createResponse(200, result.toString());

        } catch (Exception e) {
            logger.log("Error in LambdaDeletePhoto: " + e.toString());
            return createResponse(500, new JSONObject().put("message", "Error: " + e.getMessage()).toString());
        }
    }

    // ... Helper functions ...

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        // Ensure body is always JSON
        response.setBody(body.startsWith("{") ? body : new JSONObject().put("message", body).toString());
        response.setHeaders(java.util.Collections.singletonMap("Content-Type", "application/json"));
        return response;
    }

    /**
     * Helper to set up MySQL connection properties with IAM Token.
     */
    private static Properties setMySqlConnectionProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("user", DB_USER);
        props.setProperty("password", generateAuthToken());
        return props;
    }

    /**
     * Generates a temporary RDS IAM Auth Token.
     */
    private static String generateAuthToken() {
        RdsUtilities rdsUtilities = RdsUtilities.builder().build();
        return rdsUtilities.generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(RDS_INSTANCE_HOSTNAME)
                .port(RDS_INSTANCE_PORT)
                .username(DB_USER)
                .region(Region.AP_SOUTHEAST_2)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build());
    }
}