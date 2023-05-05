package io.burt.athena.configuration;

import io.burt.athena.polling.PollingStrategies;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.PreloadingStandardResult;
import io.burt.athena.result.Result;
import io.burt.athena.result.S3Result;
import io.burt.athena.result.StandardResult;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;

class ConcreteConnectionConfiguration implements ConnectionConfiguration {
    private final Region awsRegion;
    private final String databaseName;
    private final String workGroupName;
    private final String outputLocation;
    private final Duration networkTimeout;
    private final Duration queryTimeout;
    private final ResultLoadingStrategy resultLoadingStrategy;

    private AthenaAsyncClient athenaClient;
    private S3AsyncClient s3Client;
    private PollingStrategy pollingStrategy;

    private String accessKeyId;

    private String secretAccessKey;

    private String profile;

    ConcreteConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration networkTimeout, Duration queryTimeout, ResultLoadingStrategy resultLoadingStrategy) {
        this.awsRegion = awsRegion;
        this.databaseName = databaseName;
        this.workGroupName = workGroupName;
        this.outputLocation = outputLocation;
        this.networkTimeout = networkTimeout;
        this.queryTimeout = queryTimeout;
        this.resultLoadingStrategy = resultLoadingStrategy;
    }

    private ConcreteConnectionConfiguration(Region awsRegion, String databaseName, String workGroupName, String outputLocation, Duration networkTimeout, Duration queryTimeout, ResultLoadingStrategy resultLoadingStrategy, AthenaAsyncClient athenaClient, S3AsyncClient s3Client, PollingStrategy pollingStrategy) {
        this(awsRegion, databaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, resultLoadingStrategy);
        this.athenaClient = athenaClient;
        this.s3Client = s3Client;
        this.pollingStrategy = pollingStrategy;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public String workGroupName() {
        return workGroupName;
    }

    @Override
    public String outputLocation() {
        return outputLocation;
    }

    @Override
    public Duration networkTimeout() {
        return networkTimeout;
    }

    @Override
    public Duration queryTimeout() { return queryTimeout; }

    @Override
    public AthenaAsyncClient athenaClient() {
        if (athenaClient == null) {
            if (profile != null && !profile.equals("")) {
                ProfileCredentialsProvider profileCredentialsProvider = ProfileCredentialsProvider.create(profile);
                athenaClient = AthenaAsyncClient.builder().region(awsRegion).credentialsProvider(profileCredentialsProvider).build();
            }else if (accessKeyId == null || secretAccessKey == null) {
                athenaClient = AthenaAsyncClient.builder().region(awsRegion).build();
            }else {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
                StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
                athenaClient = AthenaAsyncClient.builder().region(awsRegion).credentialsProvider(credentialsProvider).build();
            }
        }
        return athenaClient;
    }

    @Override
    public S3AsyncClient s3Client() {
        if (s3Client == null) {
            s3Client = S3AsyncClient.builder().region(awsRegion).build();
        }
        return s3Client;
    }

    @Override
    public PollingStrategy pollingStrategy() {
        if (pollingStrategy == null) {
            pollingStrategy = PollingStrategies.backoff(Duration.ofMillis(10), Duration.ofSeconds(5));
        }
        return pollingStrategy;
    }

    @Override
    public ConnectionConfiguration withDatabaseName(String databaseName) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, resultLoadingStrategy, athenaClient, s3Client, pollingStrategy);
    }

    @Override
    public ConnectionConfiguration withNetworkTimeout(Duration networkTimeout) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, resultLoadingStrategy, athenaClient, s3Client, pollingStrategy);
    }

    @Override
    public ConnectionConfiguration withQueryTimeout(Duration queryTimeout) {
        return new ConcreteConnectionConfiguration(awsRegion, databaseName, workGroupName, outputLocation, networkTimeout, queryTimeout, resultLoadingStrategy, athenaClient, s3Client, pollingStrategy);
    }

    @Override
    public Result createResult(QueryExecution queryExecution) {
        if (resultLoadingStrategy == ResultLoadingStrategy.GET_EXECUTION_RESULTS) {
            return new PreloadingStandardResult(athenaClient(), queryExecution, StandardResult.MAX_FETCH_SIZE, Duration.ofSeconds(10));
        } else if (resultLoadingStrategy == ResultLoadingStrategy.S3) {
            return new S3Result(s3Client(), queryExecution, Duration.ofSeconds(10));
        } else {
            throw new IllegalStateException(String.format("No such result loading strategy: %s", queryExecution));
        }
    }

    @Override
    public void close() {
        if (athenaClient != null) {
            athenaClient.close();
            athenaClient = null;
        }
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }
}