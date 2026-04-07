package vgu.cloud26;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.lambda.runtime.events.*;
import org.json.JSONObject;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LambdaGenerateToken
        implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

        LambdaLogger logger = context.getLogger();
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();

        // 1. SETUP HEADERS
        response.setHeaders(Collections.singletonMap("Content-Type", "application/json"));

        try {
            // 2. RETRIEVE SECRET KEY
            // We fetch the secret key from AWS Parameter Store (SSM)
            // This key is needed to sign the token.
            String SECRET_KEY = getSecretKeyFromParameterStore(logger);

            // 3. PARSE INPUT BODY
            String bodyStr = request.getBody();
            if (bodyStr == null || bodyStr.isEmpty())
                throw new Exception("Empty body");

            JSONObject bodyJson = new JSONObject(bodyStr);
            String email = bodyJson.optString("email", "");

            // Validation
            if (email.isEmpty()) {
                response.setStatusCode(400);
                response.setBody(new JSONObject().put("error", "Missing 'email' field").toString());
                return response;
            }

            // Check if Secret Key was loaded successfully
            if (SECRET_KEY == null || SECRET_KEY.isEmpty()) {
                logger.log("Error: Could not retrieve Secret Key from SSM. Check Lambda logs.");
                response.setStatusCode(500);
                return response;
            }

            // 4. GENERATE TOKEN (HMAC-SHA256)
            // Input: Email + SecretKey -> Output: Base64 Signature
            String token = generateSecureToken(email, SECRET_KEY, logger);

            // 5. RETURN SUCCESS RESPONSE
            JSONObject resJson = new JSONObject();
            resJson.put("token", token);
            resJson.put("status", "success");

            response.setStatusCode(200);
            response.setBody(resJson.toString());
            return response;

        } catch (Exception e) {
            logger.log("Error: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody(new JSONObject().put("error", e.getMessage()).toString());
            return response;
        }
    }

    // =========================================================================
    // HELPER 1: CRYPTOGRAPHIC LOGIC (HMAC-SHA256)
    // =========================================================================
    public static String generateSecureToken(String data, String key, LambdaLogger logger) {
        try {
            // 1. Specify the Algorithm (HMAC with SHA-256)
            Mac mac = Mac.getInstance("HmacSHA256");

            // 2. Initialize the Key
            SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);

            // 3. Compute Hash (The signature)
            byte[] hmacBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));

            // 4. Encode to Base64 (to make it URL-safe and readable)
            String base64 = Base64.getEncoder().encodeToString(hmacBytes);

            logger.log("Generating Token for: " + data);
            // logger.log("Secure Token: " + base64); // Be careful logging secrets/tokens in prod

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
     * Retrieves the secret key using the local Lambda Extension sidecar.
     * NOTE: This requires the "AWS Parameters and Secrets Lambda Extension" layer to be added 
     * to your Lambda function in the AWS Console.
     */
    private String getSecretKeyFromParameterStore(LambdaLogger logger) {
        try {
            // 1. Get the Session Token (Required for authentication with the extension)
            String sessionToken = System.getenv("AWS_SESSION_TOKEN");
            
            // 2. Setup HTTP Client
            HttpClient client = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1) 
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            
            // 3. Build Request to LOCALHOST:2773
            // The extension listens on port 2773.
            // Parameter Name: "Cloud26SecretKey" (Must exist in SSM Parameter Store)
            // withDecryption=true: Ask SSM to decrypt the SecureString
            HttpRequest requestParameter = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:2773/systemsmanager/parameters/get/?name=Cloud26SecretKey&withDecryption=true"))
                .header("Accept", "application/json")
                .header("X-Aws-Parameters-Secrets-Token", sessionToken) // Auth Token
                .GET()
                .build();

            // 4. Send Request
            HttpResponse<String> responseParameter = client.send(requestParameter,
                    HttpResponse.BodyHandlers.ofString());

            // 5. Handle Errors (e.g., Extension not running, Permission denied)
            if (responseParameter.statusCode() != 200) {
                logger.log("Extension Error: Status " + responseParameter.statusCode() + " - " + responseParameter.body());
                return null;
            }

            // 6. Parse Response
            // The extension returns JSON: {"Parameter": {"Name": "...", "Value": "ACTUAL_SECRET", ...}}
            JSONObject jsonResponse = new JSONObject(responseParameter.body());
            String key = jsonResponse.getJSONObject("Parameter").getString("Value");

            logger.log("Successfully retrieved Secret Key from Parameter Store via Extension.");
            return key;

        } catch (Exception e) {
            logger.log("Error getting Parameter: " + e.getMessage());
            return null;
        }
    }
}