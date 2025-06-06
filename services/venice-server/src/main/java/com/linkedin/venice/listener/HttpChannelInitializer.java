package com.linkedin.venice.listener;

import com.linkedin.alpini.netty4.handlers.BasicHttpServerCodec;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.grpc.handlers.GrpcOutboundResponseHandler;
import com.linkedin.venice.listener.grpc.handlers.GrpcOutboundStatsHandler;
import com.linkedin.venice.listener.grpc.handlers.GrpcReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.grpc.handlers.GrpcRouterRequestHandler;
import com.linkedin.venice.listener.grpc.handlers.GrpcStatsHandler;
import com.linkedin.venice.listener.grpc.handlers.GrpcStorageReadRequestHandler;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.stats.ServerConnectionStats;
import com.linkedin.venice.stats.ServerLoadStats;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.ServerInterceptor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOGGER = LogManager.getLogger(HttpChannelInitializer.class);

  private final StorageReadRequestHandler requestHandler;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private final ThreadPoolStats sslHandshakesThreadPoolStats;
  private final Optional<SSLFactory> sslFactory;
  private final ThreadPoolExecutor sslHandshakeExecutor;
  private final Optional<ServerAclHandler> aclHandler;
  private final Optional<ServerStoreAclHandler> storeAclHandler;
  private final VerifySslHandler verifySsl = new VerifySslHandler();
  private final VeniceServerConfig serverConfig;
  private final ReadQuotaEnforcementHandler quotaEnforcer;
  private final VeniceHttp2PipelineInitializerBuilder http2PipelineInitializerBuilder;
  private final ServerConnectionStatsHandler serverConnectionStatsHandler;
  private AggServerQuotaUsageStats quotaUsageStats;
  List<ServerInterceptor> aclInterceptors;
  private final IdentityParser identityParser;
  private final ServerLoadControllerHandler loadControllerHandler;
  private boolean isDaVinciClient;

  public HttpChannelInitializer(
      ReadOnlyStoreRepository storeMetadataRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      ThreadPoolExecutor sslHandshakeExecutor,
      VeniceServerConfig serverConfig,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      StorageReadRequestHandler requestHandler) {
    this.serverConfig = serverConfig;
    this.requestHandler = requestHandler;
    this.isDaVinciClient = serverConfig.isDaVinciClient();

    boolean isKeyValueProfilingEnabled = serverConfig.isKeyValueProfilingEnabled();
    boolean isUnregisterMetricForDeletedStoreEnabled = serverConfig.isUnregisterMetricForDeletedStoreEnabled();

    this.singleGetStats = new AggServerHttpRequestStats(
        serverConfig.getClusterName(),
        metricsRepository,
        RequestType.SINGLE_GET,
        isKeyValueProfilingEnabled,
        storeMetadataRepository,
        isUnregisterMetricForDeletedStoreEnabled,
        isDaVinciClient);
    this.multiGetStats = new AggServerHttpRequestStats(
        serverConfig.getClusterName(),
        metricsRepository,
        RequestType.MULTI_GET,
        isKeyValueProfilingEnabled,
        storeMetadataRepository,
        isUnregisterMetricForDeletedStoreEnabled,
        isDaVinciClient);
    this.computeStats = new AggServerHttpRequestStats(
        serverConfig.getClusterName(),
        metricsRepository,
        RequestType.COMPUTE,
        isKeyValueProfilingEnabled,
        storeMetadataRepository,
        isUnregisterMetricForDeletedStoreEnabled,
        isDaVinciClient);

    if (serverConfig.isComputeFastAvroEnabled()) {
      LOGGER.info("Fast avro for compute is enabled");
    }

    this.sslFactory = sslFactory;
    this.sslHandshakeExecutor = sslHandshakeExecutor;
    this.sslHandshakesThreadPoolStats =
        new ThreadPoolStats(metricsRepository, sslHandshakeExecutor, "ssl_handshake_thread_pool");

    Class<IdentityParser> identityParserClass = ReflectUtils.loadClass(serverConfig.getIdentityParserClassName());
    this.identityParser = ReflectUtils.callConstructor(identityParserClass, new Class[0], new Object[0]);

    this.storeAclHandler = storeAccessController.isPresent()
        ? Optional.of(
            new ServerStoreAclHandler(
                identityParser,
                storeAccessController.get(),
                storeMetadataRepository,
                serverConfig.getAclInMemoryCacheTTLMs()))
        : Optional.empty();
    /**
     * If the store-level access handler is present, we don't want to fail fast if the access gets denied by {@link ServerAclHandler}.
     */
    boolean aclHandlerFailOnAccessRejection = !this.storeAclHandler.isPresent();
    this.aclHandler = routerAccessController.isPresent()
        ? Optional.of(new ServerAclHandler(routerAccessController.get(), aclHandlerFailOnAccessRejection))
        : Optional.empty();

    if (serverConfig.isQuotaEnforcementEnabled()) {
      String nodeId = Utils.getHelixNodeIdentifier(serverConfig.getListenerHostname(), serverConfig.getListenerPort());
      this.quotaUsageStats = new AggServerQuotaUsageStats(serverConfig.getClusterName(), metricsRepository);
      this.quotaEnforcer = new ReadQuotaEnforcementHandler(
          serverConfig,
          storeMetadataRepository,
          customizedViewRepository,
          nodeId,
          quotaUsageStats,
          metricsRepository);
    } else {
      this.quotaEnforcer = null;
    }

    if (serverConfig.isHttp2InboundEnabled()) {
      if (!sslFactory.isPresent()) {
        throw new VeniceException("SSL is required when enabling HTTP2");
      }
      LOGGER.info("HTTP2 inbound request is supported");
    } else {
      LOGGER.info("HTTP2 inbound request isn't supported");
    }
    this.http2PipelineInitializerBuilder = new VeniceHttp2PipelineInitializerBuilder(serverConfig);

    if (sslFactory.isPresent()) {
      this.serverConnectionStatsHandler = new ServerConnectionStatsHandler(
          this.identityParser,
          new ServerConnectionStats(metricsRepository, "server_connection_stats"),
          serverConfig.getRouterPrincipalName());
    } else {
      this.serverConnectionStatsHandler = null;
    }
    if (serverConfig.isLoadControllerEnabled()) {
      this.loadControllerHandler =
          new ServerLoadControllerHandler(serverConfig, new ServerLoadStats(metricsRepository, "server_load"));
      LOGGER.info("Server load controller is enabled");
    } else {
      this.loadControllerHandler = null;
      LOGGER.info("Server load controller is disabled");
    }
  }

  /*
    Test only
   */
  protected ReadQuotaEnforcementHandler getQuotaEnforcer() {
    return quotaEnforcer;
  }

  interface ChannelPipelineConsumer {
    void accept(ChannelPipeline pipeline, boolean whetherNeedServerCodec);
  }

  @Override
  public void initChannel(SocketChannel ch) {
    if (sslFactory.isPresent()) {
      SslInitializer sslInitializer = new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory.get()), false);
      if (sslHandshakeExecutor != null) {
        sslInitializer
            .enableSslTaskExecutor(sslHandshakeExecutor, sslHandshakesThreadPoolStats::recordQueuedTasksCount);
      }
      sslInitializer.setIdentityParser(identityParser::parseIdentityFromCert);
      ch.pipeline().addLast(sslInitializer);
      ch.pipeline().addLast(serverConnectionStatsHandler);
    }

    ChannelPipelineConsumer httpPipelineInitializer = (pipeline, whetherNeedServerCodec) -> {
      StatsHandler statsHandler = new StatsHandler(singleGetStats, multiGetStats, computeStats, loadControllerHandler);
      pipeline.addLast(statsHandler);
      if (whetherNeedServerCodec) {
        pipeline.addLast(new HttpServerCodec());
      } else {
        // Hack!!!
        /**
         * {@link Http2PipelineInitializer#configurePipeline} will instrument {@link BasicHttpServerCodec} as the codec handler,
         * which is different from the default server codec handler, and we would like to resume the original one for HTTP/1.1.
         * This might not be necessary, but it will change the current behavior of HTTP/1.1 in Venice Server.
         */
        final String codecHandlerName = "http";
        ChannelHandler codecHandler = pipeline.get(codecHandlerName);
        if (codecHandler != null) {
          // For HTTP/1.1 code path
          if (!(codecHandler instanceof BasicHttpServerCodec)) {
            throw new VeniceException(
                "BasicHttpServerCodec is expected when the pipeline is instrumented by 'Http2PipelineInitializer'");
          }
          pipeline.remove(codecHandlerName);
          pipeline.addLast(new HttpServerCodec());
        }
      }

      pipeline.addLast(new HttpObjectAggregator(serverConfig.getMaxRequestSize()))
          .addLast(new OutboundHttpWrapperHandler(statsHandler))
          .addLast(new IdleStateHandler(0, 0, serverConfig.getNettyIdleTimeInSeconds()));
      if (loadControllerHandler != null) {
        pipeline.addLast(loadControllerHandler);
      }
      if (sslFactory.isPresent()) {
        pipeline.addLast(verifySsl);
        if (aclHandler.isPresent()) {
          pipeline.addLast(aclHandler.get());
        }
        /**
         * {@link #storeAclHandler} if present must come after {@link #aclHandler}
         */
        if (storeAclHandler.isPresent()) {
          pipeline.addLast(storeAclHandler.get());
        }
      }
      pipeline
          .addLast(new RouterRequestHttpHandler(statsHandler, serverConfig.getStoreToEarlyTerminationThresholdMSMap()));
      if (quotaEnforcer != null) {
        pipeline.addLast(quotaEnforcer);
      }
      pipeline.addLast(requestHandler).addLast(new ErrorCatchingHandler());
    };

    if (serverConfig.isHttp2InboundEnabled()) {
      Http2PipelineInitializer http2PipelineInitializer = http2PipelineInitializerBuilder
          .createHttp2PipelineInitializer(pipeline -> httpPipelineInitializer.accept(pipeline, false));
      ch.pipeline().addLast(http2PipelineInitializer);
    } else {
      httpPipelineInitializer.accept(ch.pipeline(), true);
    }
  }

  public VeniceServerGrpcRequestProcessor initGrpcRequestProcessor() {
    VeniceServerGrpcRequestProcessor grpcServerRequestProcessor = new VeniceServerGrpcRequestProcessor();

    StatsHandler statsHandler = new StatsHandler(singleGetStats, multiGetStats, computeStats, null);
    GrpcStatsHandler grpcStatsHandler = new GrpcStatsHandler(statsHandler);
    grpcServerRequestProcessor.addHandler(grpcStatsHandler);

    GrpcRouterRequestHandler grpcRouterRequestHandler = new GrpcRouterRequestHandler();
    grpcServerRequestProcessor.addHandler(grpcRouterRequestHandler);

    if (quotaEnforcer != null) {
      GrpcReadQuotaEnforcementHandler grpcReadQuotaEnforcementHandler =
          new GrpcReadQuotaEnforcementHandler(quotaEnforcer);
      grpcServerRequestProcessor.addHandler(grpcReadQuotaEnforcementHandler);
    }

    GrpcStorageReadRequestHandler storageReadRequestHandler = new GrpcStorageReadRequestHandler(requestHandler);
    grpcServerRequestProcessor.addHandler(storageReadRequestHandler);

    GrpcOutboundResponseHandler grpcOutboundResponseHandler = new GrpcOutboundResponseHandler();
    grpcServerRequestProcessor.addHandler(grpcOutboundResponseHandler);

    GrpcOutboundStatsHandler grpcOutboundStatsHandler = new GrpcOutboundStatsHandler();
    grpcServerRequestProcessor.addHandler(grpcOutboundStatsHandler);

    return grpcServerRequestProcessor;
  }

  /**
   * SSL Certificates can only be accessed easily via Server Interceptors for gRPC, so we create our acls here
   * We can create aclInterceptor list as these handlers are already present within
   */
  public List<ServerInterceptor> initGrpcInterceptors() {
    aclInterceptors = new ArrayList<>();

    if (sslFactory.isPresent()) {
      LOGGER.info("SSL is enabled, adding ACL Interceptors");
      aclInterceptors.add(verifySsl);
      aclHandler.ifPresent(serverAclHandler -> aclInterceptors.add(serverAclHandler));

      storeAclHandler.ifPresent(storeAclHandler -> aclInterceptors.add(storeAclHandler));
    }

    return aclInterceptors;
  }
}
