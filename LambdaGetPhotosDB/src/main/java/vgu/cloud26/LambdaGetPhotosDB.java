package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.Collections;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.core.SdkBytes;

public class LambdaGetPhotosDB implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    
    // --- Configuration Constants for RDS & Lambda ---
    private static final String RDS_INSTANCE_HOSTNAME = "objectdatabase.c3mo42ucag2n.ap-southeast-2.rds.amazonaws.com";
    private static final int RDS_INSTANCE_PORT = 3306;
    private static final String DB_USER = "cloud26";
    private static final String JDBC_URL = "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/Cloud26";
    private static final String CHECKER_FUNCTION_NAME = "LambdaTokenChecker";

    // Initialize Lambda Client (reused across invocations for performance)
    private static final LambdaClient lambdaClient = LambdaClient.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        LambdaLogger logger = context.getLogger();
        JSONArray items = new JSONArray(); // Array to hold the list of photos

        try {
            // 1. Parse the incoming JSON body
            String bodyStr = request.getBody();
            if (bodyStr == null) 
                return createResponse(400, "{\"message\":\"Empty request body\"}");

            JSONObject requestBody = new JSONObject(bodyStr);

            // 2. Extract User Credentials
            String email = requestBody.optString("email", "");
            String clientToken = requestBody.optString("token", "");
            
            // 3. Security Check: Validate inputs and verify token via Helper Lambda
            if (email.isEmpty() || clientToken.isEmpty() || !isTokenValid(email, clientToken, logger)) {
                 return createResponse(401, "{\"message\":\"Unauthorized\"}");
            }

            // 4. Connect to RDS Database using IAM Authentication
            Class.forName("com.mysql.cj.jdbc.Driver");
            try (Connection mySQLClient = DriverManager.getConnection(JDBC_URL, setMySqlConnectionProperties())) {
                
                // 5. Execute SQL Query safely using PreparedStatement
                PreparedStatement st = mySQLClient.prepareStatement("SELECT * FROM Photos WHERE Email = ?");
                st.setString(1, email); // Bind email to prevent SQL Injection
                
                ResultSet rs = st.executeQuery();

                // 6. Iterate through the result set
                while (rs.next()) {
                    JSONObject item = new JSONObject();
                    item.put("ID", rs.getInt("ID"));
                    item.put("Description", rs.getString("Description"));
                    
                    // --- PATH RECONSTRUCTION LOGIC ---
                    String rawName = rs.getString("S3Key"); // Original filename (e.g., "cat.jpg")
                    String hash = rs.getString("HashKey");  // Content hash (e.g., "abc123hash")
                    String ownerEmail = rs.getString("Email");

                    // a. Extract file extension (e.g., ".jpg")
                    String extension = "";
                    int dotIndex = rawName.lastIndexOf('.');
                    if (dotIndex >= 0) {
                        extension = rawName.substring(dotIndex); 
                    }

                    // b. Rebuild the physical S3 path: "email/hash_value.extension"
                    // This ensures the frontend downloads the correct file from the unique folder structure
                    String finalPath = ownerEmail + "/" + hash + extension;

                    item.put("S3Key", finalPath); 
                    item.put("email", ownerEmail);

                    items.put(item); // Add object to the list
                }
            }
            
            // 7. Return the list of objects as JSON
            return createResponse(200, items.toString());

        } catch (Exception ex) {
            logger.log("Error: " + ex.toString());
            return createResponse(500, "{\"error\":\"Internal Server Error\"}");
        }
    }

    /**
     * Helper method to invoke the LambdaTokenChecker function
     */
    private boolean isTokenValid(String email, String token, LambdaLogger logger) {
        try {
            // Prepare payload for the checker function
            JSONObject actualData = new JSONObject();
            actualData.put("email", email);
            actualData.put("token", token);

            JSONObject proxyPayload = new JSONObject();
            proxyPayload.put("body", actualData.toString()); 

            // Invoke Lambda synchronously
            InvokeRequest invokeRequest = InvokeRequest.builder()
                    .functionName(CHECKER_FUNCTION_NAME)
                    .payload(SdkBytes.fromUtf8String(proxyPayload.toString()))
                    .build();
            InvokeResponse response = lambdaClient.invoke(invokeRequest);

            // Parse response
            String responseStr = response.payload().asUtf8String();
            JSONObject fullRes = new JSONObject(responseStr);
            
            if (fullRes.has("body")) {
                JSONObject bodyRes = new JSONObject(fullRes.getString("body"));
                return bodyRes.optBoolean("valid", false);
            }
            return false;
            
        } catch (Exception e) { 
            return false; }
    }

    // Helper to create API Gateway response
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(statusCode);
        response.setBody(body);
        response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        return response;
    }
    
    // Helper to set up JDBC properties with IAM Auth Token
    private static Properties setMySqlConnectionProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("user", DB_USER);
        props.setProperty("password", generateAuthToken()); // Use generated token instead of static password
        return props;
    }

    // Generates an RDS IAM Authentication Token
    private static String generateAuthToken() throws Exception {
        RdsUtilities rdsUtilities = RdsUtilities.builder().build();
        return rdsUtilities.generateAuthenticationToken(
                GenerateAuthenticationTokenRequest.builder()
                        .hostname(RDS_INSTANCE_HOSTNAME)
                        .port(RDS_INSTANCE_PORT)
                        .username(DB_USER)
                        .region(Region.AP_SOUTHEAST_2)
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .build());
    }
}