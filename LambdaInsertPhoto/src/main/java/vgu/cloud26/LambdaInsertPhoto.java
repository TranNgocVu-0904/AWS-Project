package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Properties;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.json.JSONObject;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

public class LambdaInsertPhoto implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // === MySQL RDS Configuration ===
    private static final String RDS_INSTANCE_HOSTNAME = "objectdatabase.c3mo42ucag2n.ap-southeast-2.rds.amazonaws.com";
    private static final int RDS_INSTANCE_PORT = 3306;
    private static final String DB_USER = "cloud26"; // Ensure this user has AWSAuthenticationPlugin enabled in MySQL
    private static final String JDBC_URL = "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/Cloud26";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {  
        LambdaLogger logger = context.getLogger();

        try {
            String requestBody = request.getBody();
            JSONObject bodyJson = new JSONObject(requestBody);

            // 1. PARSE INPUT DATA
            String description = bodyJson.optString("description", "");
            String originalKey = bodyJson.getString("s3Key");   // e.g., "cat.jpg"
            String email       = bodyJson.optString("email", "unknown_user");

            // 2. HASH LOGIC (Consistency with Upload Logic)
            // We need to calculate the hash here to store it in the 'HashKey' column
            // and to reconstruct the physical path.
            
            // Separating Name and Extension
            int lastDotIndex = originalKey.lastIndexOf('.');
            String namePart;
            String extensionPart;

            if (lastDotIndex == -1) {
                namePart = originalKey;
                extensionPart = ""; 
            } else {
                namePart = originalKey.substring(0, lastDotIndex);
                extensionPart = originalKey.substring(lastDotIndex); // .jpg
            }

            // Calculate SHA-256 Hash of the filename part
            String hashValue = sha256Hex(namePart); 

            // Construct the ACTUAL S3 path to return to the Orchestrator
            // The Orchestrator needs this so it can tell the Resizer where the file actually lives.
            // Format: "email/hash.jpg"
            String physicalS3Path = email + "/" + hashValue + extensionPart;

            logger.log("Inserting DB: Original=" + originalKey + " | Hash=" + hashValue);

            // 3. INSERT INTO DATABASE
            int rowsInserted = 0;
            // Load MySQL Driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Connect using IAM Authentication properties
            try (Connection conn = DriverManager.getConnection(JDBC_URL, setMySqlConnectionProperties())) {
                
                // Table Structure: ID | Description | S3Key | HashKey | Email
                String sql = "INSERT INTO Photos (Description, S3Key, HashKey, Email) VALUES (?, ?, ?, ?)";
                
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, description);
                    
                    // --- IMPORTANT ---
                    // As per requirement: Store the ORIGINAL name ("cat.jpg") in the S3Key column
                    ps.setString(2, originalKey); 
                    
                    // Store the calculated Hash
                    ps.setString(3, hashValue);
                    
                    // Store the Email (owner)
                    ps.setString(4, email);
                    
                    rowsInserted = ps.executeUpdate();
                }
            }

            // 4. RETURN RESULT
            JSONObject result = new JSONObject();
            result.put("message", "Inserted into Photos successfully");
            result.put("rowsInserted", rowsInserted);
            
            // Return DB info for debugging
            result.put("dbS3Key", originalKey); 
            result.put("dbHashKey", hashValue);
            
            // Return the physical path
            result.put("physicalS3Key", physicalS3Path); 

            return createResponse(200, result.toString());

        } catch (SQLIntegrityConstraintViolationException dupEx) {
            // Handle Duplicate Entries (e.g. if HashKey is UNIQUE in DB)
            logger.log("Duplicate HashKey: " + dupEx.toString());
            return createResponse(400, new JSONObject().put("message", "Duplicate file: This image already exists.").toString());

        } catch (Exception ex) {
            logger.log("Error: " + ex.toString());
            return createResponse(500, new JSONObject().put("message", ex.getMessage()).toString());
        }
    }

    // ====== HELPER: HASHING (SHA-256) ======
    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not supported", e);
        }
    }

    // ====== HELPER: RESPONSE GENERATION ======
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        response.setBody(body);
        response.setHeaders(java.util.Collections.singletonMap("Content-Type", "application/json"));
        return response;
    }

    // ====== HELPER: IAM DB AUTHENTICATION ======
    /**
     * Sets up the connection properties including the IAM Token as the password.
     */
    private static Properties setMySqlConnectionProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "true"); // IAM Auth requires SSL
        props.setProperty("user", DB_USER);
        props.setProperty("password", generateAuthToken()); // The "password" is dynamic
        return props;
    }

    /**
     * Generates a temporary authentication token using AWS SDK.
     * This token is valid for 15 minutes.
     */
    private static String generateAuthToken() {
        RdsUtilities rdsUtilities = RdsUtilities.builder().build();
        return rdsUtilities.generateAuthenticationToken(GenerateAuthenticationTokenRequest.builder()
                .hostname(RDS_INSTANCE_HOSTNAME)
                .port(RDS_INSTANCE_PORT)
                .username(DB_USER)
                .region(Region.AP_SOUTHEAST_2)
                .credentialsProvider(DefaultCredentialsProvider.create()) // Uses Lambda's IAM Role
                .build());
    }
}