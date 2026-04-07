package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*;
import org.json.JSONObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.*;
import software.amazon.awssdk.services.lambda.model.*;


import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public class LambdaTokenChecker implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

        LambdaLogger logger = context.getLogger();

        try {
            // 1. RETRIEVE SECRET KEY
            // We need the master key to verify the signature. 
            // We fetch it from AWS Parameter Store via the local Extension.
            String SECRET_KEY = getSecretKeyFromParameterStore(logger);

            // Check if key retrieval failed
            if (SECRET_KEY == null) {
                return createResponse(500, "{\"error\":\"Internal Server Error: Unable to fetch secrets.\"}");
            }

            // 2. PARSE INPUT
            // The request should contain the User's Email and the Token they possess.
            JSONObject body = new JSONObject(request.getBody());
            String email = body.getString("email");
            String clientToken = body.getString("token");

            // 3. VERIFICATION LOGIC (HMAC RE-COMPUTATION)
            // We cannot decrypt a Hash. Instead, we take the email and our Secret Key
            // and regenerate the token ourselves.
            String expectedToken = generateSecureToken(email, SECRET_KEY, logger);

            // 4. COMPARE & RESPOND
            JSONObject responseJson = new JSONObject();
            
            // If the token we calculated matches the token the client sent, they are authenticated.
            if (expectedToken.equals(clientToken)) {
                responseJson.put("valid", true);
                responseJson.put("message", "Token is valid!");
                return createResponse(200, responseJson.toString());
            } 
            else {
                responseJson.put("valid", false);
                responseJson.put("message", "Token is not correct!");
                // Return 401 Unauthorized
                return createResponse(401, responseJson.toString());
            }

        } catch (Exception e) {
            logger.log("Error verifying token: " + e.getMessage());
            return createResponse(500, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    // =========================================================================
    // HELPER 1: CRYPTOGRAPHIC LOGIC (HMAC-SHA256)
    // =========================================================================
    public static String generateSecureToken (String data, String key, LambdaLogger logger)  {
        try {
            // 1. Specify the Algorithm (HMAC with SHA-256)
            Mac mac = Mac.getInstance("HmacSHA256");

            // 2. Initialize the Key
            SecretKeySpec secretKeySpec
                    = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8),
                            "HmacSHA256");
            mac.init(secretKeySpec);

            // 3. Compute Hash
            byte[] hmacBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));

            // 4. Encode to Base64 (Result is the signature)
            String base64 = Base64.getEncoder().encodeToString(hmacBytes);

            logger.log("Verifying for: " + data);
            // logger.log("Computed Token: " + base64); 

            return base64;

        } catch (NoSuchAlgorithmException e) {
            logger.log("HmacSHA256 algorithm not found: " + e.getMessage());
            return null;
            
        } catch (InvalidKeyException ex) {
            logger.log("InvalidKeyException: " + ex.getMessage());
            return null;
        }
    }

    // =========================================================================
    // HELPER 2: AWS PARAMETERS AND SECRETS EXTENSION
    // =========================================================================
    /**
     * Fetches the Secret Key from AWS Systems Manager (SSM) Parameter Store
     * using the AWS Lambda Extension (Sidecar) running on localhost:2773.
     */
    private String getSecretKeyFromParameterStore(LambdaLogger logger) {
        try {
            // 1. Get Session Token (Required for extension authentication)
            String sessionToken = System.getenv("AWS_SESSION_TOKEN");
            
            // 2. Setup HTTP Client
            HttpClient client = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1) 
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            
            // 3. Build Request to Local Extension
            // Parameter Name: "Cloud26SecretKey"
            // withDecryption=true: Essential for SecureString parameters
            HttpRequest requestParameter;
            requestParameter = HttpRequest.newBuilder()
                    .uri(URI.create(
                            "http://localhost:2773/systemsmanager/parameters/get/?name=Cloud26SecretKey&withDecryption=true"))
                    .header("Accept", "application/json") 
                    .header("X-Aws-Parameters-Secrets-Token", sessionToken)
                    .GET() 
                    .build();

            // 4. Send Request
            HttpResponse<String> responseParameter = client.send(requestParameter,
                    HttpResponse.BodyHandlers.ofString());
            
            // Check for errors
            if (responseParameter.statusCode() != 200) {
                logger.log("Extension Error: Status " + responseParameter.statusCode() + " - " + responseParameter.body());
                return null;
            }

            // 5. Parse JSON Response
            // Response format: {"Parameter": {"Name": "...", "Value": "SECRET_VALUE", ...}}
            JSONObject jsonResponse = new JSONObject(responseParameter.body());
            String key = jsonResponse.getJSONObject("Parameter").getString("Value");

            logger.log("Successfully getting Secret Key from Parameter Store.");
            return key;

        } catch (Exception e) {
            logger.log("Error getting Parameter: " + e.getMessage());
            return null;
        }
    }

    // --- Helper to format API Gateway Response ---
    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent resp = new APIGatewayProxyResponseEvent();
        resp.setStatusCode(statusCode);
        resp.setHeaders(Collections.singletonMap("Content-Type", "application/json"));
        resp.setBody(body);
        return resp;
    }
}