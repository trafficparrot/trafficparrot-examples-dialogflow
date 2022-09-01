package com.trafficparrot.example.dialogflow;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.BidiStream;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.dialogflow.cx.v3beta1.*;
import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.UUID;

import static io.grpc.netty.shaded.io.grpc.netty.NegotiationType.PLAINTEXT;
import static org.assertj.core.api.Assertions.*;

/**
 * Based on the GCP Dialogflow code sample:
 * <a href="https://cloud.google.com/dialogflow/cx/docs/quick/api#detect-intent">Call detect intent</a>
 */
class TrafficParrotDialogFlowTest {

    private static final String PROJECT_ID = "traffic-parrot-dialogflow-example";
    private static final String LOCATION_ID = "us-central1";
    private static final String AGENT_ID = "ef28899e-5401-465e-9352-2efc8a8ebef9";
    private static final String SESSION_ID = UUID.randomUUID().toString();
    private static final String LANGUAGE_CODE = "en-us";
    private static final String TRAFFIC_PARROT_HOST = "localhost";
    private static final int TRAFFIC_PARROT_NON_TLS_PORT = 5552;

    @Test
    void testDetectIntent() throws IOException {
        Map<String, QueryResult> result = detectIntent("hello", "world");
        assertThat(result.get("hello").getText()).isEqualTo("hello");
        assertThat(result.get("world").getText()).isEqualTo("world");
    }

    @Test
    void testDetectIntentError()  {
        assertThatThrownBy(() -> detectIntent("hello", "request-error", "ignored")).hasMessage("io.grpc.StatusRuntimeException: ABORTED");
    }

    @Test
    void testStreamingDetectIntent() throws IOException {
        Map<String, QueryResult> result = streamingDetectIntent("hello", "world");
        assertThat(result.get("hello").getText()).isEqualTo("hello");
        assertThat(result.get("world").getText()).isEqualTo("world");
    }

    @Test
    void testStreamingDetectIntentErrorCode() throws IOException {
        assertThatThrownBy(() -> streamingDetectIntent("hello", "request-error", "ignored")).hasMessage("io.grpc.StatusRuntimeException: ABORTED");
    }

    private static Map<String, QueryResult> streamingDetectIntent(String... texts) throws IOException, ApiException {
        ManagedChannel channel = NettyChannelBuilder.forAddress(TRAFFIC_PARROT_HOST, TRAFFIC_PARROT_NON_TLS_PORT)
                .negotiationType(PLAINTEXT)
                .build();

        SessionsSettings sessionsSettings = SessionsSettings.newBuilder()
                .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();

        Map<String, QueryResult> queryResults = Maps.newHashMap();

        try (SessionsClient sessionsClient = SessionsClient.create(sessionsSettings)) {
            SessionName session = SessionName.ofProjectLocationAgentSessionName(PROJECT_ID, LOCATION_ID, AGENT_ID, SESSION_ID);

            System.out.println("Session Path: " + session.toString());

            BidiStream<StreamingDetectIntentRequest, StreamingDetectIntentResponse> stream = sessionsClient.streamingDetectIntentCallable().call();

            // Detect intents for each text input.
            for (String text : texts) {
                TextInput.Builder textInput = TextInput.newBuilder().setText(text);

                QueryInput queryInput = QueryInput.newBuilder().setText(textInput).setLanguageCode(LANGUAGE_CODE).build();

                StreamingDetectIntentRequest request =
                        StreamingDetectIntentRequest.newBuilder()
                                .setSession(session.toString())
                                .setQueryInput(queryInput)
                                .build();

                stream.send(request);
            }
            stream.closeSend();

            for (StreamingDetectIntentResponse response : stream) {
                // Display the query result.
                QueryResult queryResult = response.getDetectIntentResponse().getQueryResult();

                System.out.println("====================");
                System.out.format("Query Text: '%s'\n", queryResult.getText());
                System.out.format(
                        "Detected Intent: %s (confidence: %f)\n",
                        queryResult.getMatch().getIntent().getDisplayName(),
                        queryResult.getMatch().getConfidence());

                queryResults.put(queryResult.getText(), queryResult);
            }
        }
        return queryResults;
    }

    private static Map<String, QueryResult> detectIntent(String... texts) throws IOException, ApiException {
        ManagedChannel channel = NettyChannelBuilder.forAddress(TRAFFIC_PARROT_HOST, TRAFFIC_PARROT_NON_TLS_PORT)
                .negotiationType(PLAINTEXT)
                .build();

        SessionsSettings sessionsSettings = SessionsSettings.newBuilder()
                .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();

        Map<String, QueryResult> queryResults = Maps.newHashMap();

        try (SessionsClient sessionsClient = SessionsClient.create(sessionsSettings)) {
            SessionName session = SessionName.ofProjectLocationAgentSessionName(PROJECT_ID, LOCATION_ID, AGENT_ID, SESSION_ID);

            System.out.println("Session Path: " + session.toString());

            // Detect intents for each text input.
            for (String text : texts) {
                TextInput.Builder textInput = TextInput.newBuilder().setText(text);

                QueryInput queryInput = QueryInput.newBuilder().setText(textInput).setLanguageCode(LANGUAGE_CODE).build();

                DetectIntentRequest request =
                        DetectIntentRequest.newBuilder()
                                .setSession(session.toString())
                                .setQueryInput(queryInput)
                                .build();

                DetectIntentResponse response = sessionsClient.detectIntent(request);

                // Display the query result.
                QueryResult queryResult = response.getQueryResult();

                System.out.println("====================");
                System.out.format("Query Text: '%s'\n", queryResult.getText());
                System.out.format(
                        "Detected Intent: %s (confidence: %f)\n",
                        queryResult.getMatch().getIntent().getDisplayName(),
                        queryResult.getMatch().getConfidence());

                queryResults.put(text, queryResult);
            }
        }
        return queryResults;
    }

    @BeforeEach
    void checkTrafficParrotRunning() {
        if (portIsAvailable(TRAFFIC_PARROT_HOST, TRAFFIC_PARROT_NON_TLS_PORT)) {
            fail("Could not find Traffic Parrot running on " + TRAFFIC_PARROT_HOST + " with non-TLS gRPC port " + TRAFFIC_PARROT_NON_TLS_PORT + ". Please see README.txt for instructions.");
        }
    }

    private static boolean portIsAvailable(String host, int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(host, port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}

