package com.linkedin.venice.controller;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * State model used to handle the change of leader-standby relationship for controllers.
 * <p>
 * This class should extend DistClusterControllerStateModel, but we need a helix manager to to the essential
 * initialization but it is the private member of DistClusterControllerStateModel, so we can't get it in sub-class. So
 * we don't extend it right now. //TODO Will ask Helix team to modify the visibility.
 */
@StateModelInfo(initialState = HelixState.OFFLINE_STATE, states = { HelixState.LEADER_STATE, HelixState.STANDBY_STATE })
public class VeniceControllerStateModel extends StateModel {
  private static final String PARTITION_SUFFIX = "_0";
  private static final long CLOSE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerStateModel.class);

  private final ZkClient zkClient;
  private final HelixAdapterSerializer adapterSerializer;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;
  private final MetricsRepository metricsRepository;
  private final ClusterLeaderInitializationRoutine controllerInitialization;
  private final Optional<DynamicAccessController> accessController;
  private final String clusterName;
  private final HelixAdminClient helixAdminClient;
  private final RealTimeTopicSwitcher realTimeTopicSwitcher;

  private volatile VeniceControllerClusterConfig clusterConfig;
  private volatile SafeHelixManager helixManager;
  private volatile HelixVeniceClusterResources clusterResources;

  private final ExecutorService workerService;
  private final ExecutorService cleanupService;
  private final Object lifecycleLock = new Object();
  private volatile Future<?> latestCleanupFuture;
  private LeaderInitializationAttempt activeInitialization;
  private boolean closed;
  private final Optional<List<VeniceVersionLifecycleEventListener>> versionLifecycleEventListeners;

  public VeniceControllerStateModel(
      String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      MetricsRepository metricsRepository,
      ClusterLeaderInitializationRoutine controllerInitialization,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      Optional<DynamicAccessController> accessController,
      HelixAdminClient helixAdminClient,
      Optional<List<VeniceVersionLifecycleEventListener>> versionLifecycleEventListeners) {
    this._currentState = new StateModelParser().getInitialState(VeniceControllerStateModel.class);
    this.clusterName = clusterName;
    this.zkClient = zkClient;
    this.adapterSerializer = adapterSerializer;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
    this.metricsRepository = metricsRepository;
    this.controllerInitialization = controllerInitialization;
    this.realTimeTopicSwitcher = realTimeTopicSwitcher;
    this.accessController = accessController;
    this.helixAdminClient = helixAdminClient;
    this.workerService = Executors.newSingleThreadExecutor(
        new DaemonThreadFactory(String.format("Controller-ST-Worker-%s", clusterName), admin.getLogContext()));
    this.cleanupService = Executors.newSingleThreadExecutor(
        new DaemonThreadFactory(String.format("Controller-ST-Cleanup-%s", clusterName), admin.getLogContext()));
    this.versionLifecycleEventListeners = versionLifecycleEventListeners;
  }

  /**
   * Test if current state is {@link HelixState#LEADER_STATE}.
   * @return  <code>true</code> if current state is {@link HelixState#LEADER_STATE};
   *          <code>false</code> otherwise.
   */
  public boolean isLeader() {
    synchronized (_currentState) {
      return getCurrentState().equals(HelixState.LEADER_STATE);
    }
  }

  /**
   * This runs after the state transition occurred.
   */
  @Override
  public boolean updateState(String newState) {
    boolean result;
    synchronized (_currentState) {
      result = super.updateState(newState);
    }
    if (newState.equals(HelixState.LEADER_STATE)) {
      controllerInitialization.execute(clusterName);
    }
    return result;
  }

  /**
   * Executes the state transition synchronously with a thread name prefix "Sync-Helix-ST".
   */
  private void executeStateTransitionSync(Message message, StateTransition stateTransition) {
    String threadName = String
        .format("Sync-Helix-ST-%s-%s->%s", message.getResourceName(), message.getFromState(), message.getToState());
    executeStateTransitionWithThreadName(threadName, stateTransition);
  }

  /**
   * Executes the state transition asynchronously with a thread name prefix "Async-ClusterName-Helix-ST".
   */
  Future<?> executeStateTransitionAsync(Message message, StateTransition stateTransition) {
    String threadName = String.format(
        "Async-%s-Helix-ST-%s-%s->%s",
        clusterName,
        message.getResourceName(),
        message.getFromState(),
        message.getToState());
    synchronized (lifecycleLock) {
      if (closed) {
        throw new VeniceException("Cannot execute state transition for closed cluster " + clusterName);
      }
      return workerService.submit(() -> executeStateTransitionWithThreadName(threadName, stateTransition));
    }
  }

  /**
   * Core method that runs the state transition with a custom thread name.
   * The thread name is set for debugging purposes.
   */
  private void executeStateTransitionWithThreadName(String threadName, StateTransition stateTransition) {
    Thread currentThread = Thread.currentThread();
    String originalName = currentThread.getName();
    currentThread.setName(threadName);
    try {
      stateTransition.execute();
    } catch (Exception e) {
      LOGGER.error("Failed to execute controller state transition", e);
      throw new VeniceException("Failed to execute '" + threadName + "'.", e);
    } finally {
      // Once st is terminated, change the name back to indicate this thread will not be occupied by this st.
      Thread.currentThread().setName(originalName);
    }
  }

  interface StateTransition {
    void execute() throws Exception;
  }

  /**
   * A callback for Helix state transition from {@link HelixState#STANDBY_STATE} to {@link HelixState#LEADER_STATE}.
   */
  @Transition(to = HelixState.LEADER_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    String controllerName = message.getTgtName();

    // Call it in executeStateTransition to log the start of state transition with correct thread name.
    executeStateTransitionSync(message, () -> {
      if (clusterConfig == null) {
        throw new VeniceException("No configuration exists for " + clusterName);
      }
      LOGGER.info("{} becoming leader from standby for {}", controllerName, clusterName);
    });

    /**
     * STANDBY->LEADER state transition is fine to be synchronous and slow transition, because controller client has
     * retry logic. However, the state transition has to be executed in order, such that if there is any unfinished
     * state transition actions from previous round (e.g. LEADER -> FOLLOWER) for the same controller, it will be
     * blocked until the previous transition is finished.
     *
     * The transition timeout gives another healthy controller a chance to become leader if the current one is stuck.
     * Initialization resources remain private to one generation until initialization completes, so timeout cleanup can
     * disconnect the manager without concurrently clearing resources that the worker is still mutating.
     */
    LeaderInitializationAttempt initializationAttempt = newLeaderInitializationAttempt();
    Future<?> stateTransitionFuture = null;
    try {
      stateTransitionFuture =
          executeStateTransitionAsync(message, () -> initializeAsLeader(initializationAttempt, controllerName));
      initializationAttempt.future = stateTransitionFuture;
      stateTransitionFuture.get(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      abortStateTransition(initializationAttempt, stateTransitionFuture);
      Thread.currentThread().interrupt();
      LOGGER.error("Failed to execute the controller state transition from STANDBY to LEADER for {}", clusterName, e);
      throw new VeniceException(e);
    } catch (ExecutionException | TimeoutException e) {
      abortStateTransition(initializationAttempt, stateTransitionFuture);
      LOGGER.error("Failed to execute the controller state transition from STANDBY to LEADER for {}", clusterName, e);
      throw new VeniceException(e);
    }
  }

  private LeaderInitializationAttempt newLeaderInitializationAttempt() {
    synchronized (lifecycleLock) {
      if (activeInitialization != null) {
        throw new VeniceException("Leader initialization is already active for cluster " + clusterName);
      }
      LeaderInitializationAttempt attempt = new LeaderInitializationAttempt();
      activeInitialization = attempt;
      return attempt;
    }
  }

  private void initializeAsLeader(LeaderInitializationAttempt attempt, String controllerName) throws Exception {
    attempt.started.set(true);
    boolean published = false;
    try {
      awaitPreviousCleanup();
      ensureAttemptIsActive(attempt);
      synchronized (lifecycleLock) {
        if (helixManagerInitialized()) {
          throw new VeniceException(
              String.format(
                  "Helix manager already exists for instance %s on cluster %s while controller %s is becoming leader",
                  helixManager.getInstanceName(),
                  clusterName,
                  controllerName));
        }
      }

      SafeHelixManager attemptManager = createHelixManager(controllerName);
      attempt.helixManager = attemptManager;
      ensureAttemptIsActive(attempt);
      attemptManager.connect();
      ensureAttemptIsActive(attempt);
      attemptManager.startTimerTasks();

      HelixVeniceClusterResources attemptResources = createClusterResources(attemptManager);
      attempt.clusterResources = attemptResources;
      initializeClusterResources(attempt, attemptResources);

      synchronized (lifecycleLock) {
        ensureAttemptIsActive(attempt);
        helixManager = attemptManager;
        clusterResources = attemptResources;
        activeInitialization = null;
        published = true;
      }
      LOGGER.info(
          "Controller {} with instance {} is the leader of cluster {}",
          controllerName,
          attemptManager.getInstanceName(),
          clusterName);
    } finally {
      if (!published) {
        cleanupUnpublishedAttempt(attempt);
      }
    }
  }

  private void initializeClusterResources(
      LeaderInitializationAttempt attempt,
      HelixVeniceClusterResources attemptResources) {
    attemptResources.refresh();
    ensureAttemptIsActive(attempt);
    attemptResources.startErrorPartitionResetTask();
    ensureAttemptIsActive(attempt);
    attemptResources.startDeadStoreStatsPreFetchTask();
    ensureAttemptIsActive(attempt);
    attemptResources.startLeakedPushStatusCleanUpService();
    ensureAttemptIsActive(attempt);
    attemptResources.startProtocolVersionAutoDetectionService();
    ensureAttemptIsActive(attempt);
    attemptResources.startLogCompactionService();
    ensureAttemptIsActive(attempt);
    attemptResources.startMultiTaskSchedulerService();
    ensureAttemptIsActive(attempt);
  }

  private void awaitPreviousCleanup() throws InterruptedException, ExecutionException {
    Future<?> cleanupFuture = latestCleanupFuture;
    if (cleanupFuture != null) {
      cleanupFuture.get();
    }
  }

  private void ensureAttemptIsActive(LeaderInitializationAttempt attempt) {
    synchronized (lifecycleLock) {
      if (attempt.aborted.get() || activeInitialization != attempt) {
        throw new VeniceException("Leader initialization is no longer active for cluster " + clusterName);
      }
    }
  }

  private void abortStateTransition(LeaderInitializationAttempt attempt, Future<?> stateTransitionFuture) {
    attempt.aborted.set(true);
    if (stateTransitionFuture != null && !stateTransitionFuture.isDone()) {
      stateTransitionFuture.cancel(true);
    }
    if (!attempt.started.get()) {
      synchronized (lifecycleLock) {
        if (activeInitialization == attempt && !attempt.started.get()) {
          activeInitialization = null;
        }
      }
    }
    submitCleanup(detachPublishedResources(), attempt);
  }

  private boolean helixManagerInitialized() {
    return helixManager != null && helixManager.isConnected();
  }

  @VisibleForTesting
  SafeHelixManager createHelixManager(String controllerName) {
    InstanceType instanceType =
        clusterConfig.isVeniceClusterLeaderHAAS() ? InstanceType.SPECTATOR : InstanceType.CONTROLLER;
    return new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(clusterName, controllerName, instanceType, zkClient.getServers()));
  }

  @VisibleForTesting
  HelixVeniceClusterResources createClusterResources(SafeHelixManager attemptManager) {
    VeniceVersionLifecycleEventManager versionLifecycleEventManager = new VeniceVersionLifecycleEventManager();
    versionLifecycleEventListeners.ifPresent(listeners -> listeners.forEach(versionLifecycleEventManager::addListener));
    return new HelixVeniceClusterResources(
        clusterName,
        zkClient,
        adapterSerializer,
        attemptManager,
        clusterConfig,
        admin,
        metricsRepository,
        realTimeTopicSwitcher,
        accessController,
        helixAdminClient,
        versionLifecycleEventManager);
  }

  /**
   * A callback for Helix state transition from {@link HelixState#LEADER_STATE} to {@link HelixState#STANDBY_STATE}.
   */
  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.LEADER_STATE)
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    // Call it in executeStateTransition to log the start of state transition with correct thread name.
    executeStateTransitionSync(message, () -> {
      String controllerName = message.getTgtName();

      LOGGER.info("{} becoming standby from leader for {}", controllerName, clusterName);
    });

    /**
     * The reset() method could be a long-running operation, and it should be run asynchronously in a separate thread
     * to avoid blocking the Helix state transition thread. Running it in the Helix state transition thread could lead
     * to a problem that the controller is still in the leader role thus still supposed to serve requests, however its
     * metadata is already cleared and not able to serve. This often results in returning 404 or store not found for
     * a store that actually exists.
     */
    reset();
  }

  /**
   * A callback for Helix state transition from {@link HelixState#STANDBY_STATE} to {@link HelixState#OFFLINE_STATE}.
   */
  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.STANDBY_STATE)
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    executeStateTransitionSync(message, () -> {
      String controllerName = message.getTgtName();
      LOGGER.info("{} becoming offline from standby for {}", controllerName, clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#OFFLINE_STATE} to {@link HelixState#STANDBY_STATE}.
   */
  @Transition(to = HelixState.STANDBY_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    executeStateTransitionSync(message, () -> {
      clusterConfig = multiClusterConfigs.getControllerConfig(clusterName);
      String controllerName = message.getTgtName();
      LOGGER.info("{} becoming standby from offline for {}", controllerName, clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#OFFLINE_STATE} to {@link HelixState#DROPPED_STATE}.
   */
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.OFFLINE_STATE)
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    executeStateTransitionSync(message, () -> {
      LOGGER.info("{} going from OFFLINE to DROPPED.", clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#ERROR_STATE} to {@link HelixState#DROPPED_STATE}.
   */
  @Transition(to = HelixState.DROPPED_STATE, from = HelixState.ERROR_STATE)
  public void onBecomeDroppedFromError(Message message, NotificationContext context) {
    executeStateTransitionSync(message, () -> {
      LOGGER.info("{} going from ERROR to DROPPED.", clusterName);
    });
  }

  /**
   * A callback for Helix state transition from {@link HelixState#ERROR_STATE} to {@link HelixState#OFFLINE_STATE}.
   */
  @Transition(to = HelixState.OFFLINE_STATE, from = HelixState.ERROR_STATE)
  public void onBecomingOfflineFromError(Message message, NotificationContext context) {
    executeStateTransitionSync(message, () -> {
      LOGGER.info("{} going from ERROR to OFFLINE.", clusterName);
    });
  }

  /**
   * Called when error occurs in state transition.
   */
  @Override
  public void rollbackOnError(Message message, NotificationContext context, StateTransitionError error) {
    String controllerName = message.getTgtName();
    LOGGER.error("{} rollbacks on error for {}", controllerName, clusterName);
    reset();
  }

  /**
   * Called when the state model is reset.
   */
  @Override
  public void reset() {
    LeaderInitializationAttempt attempt;
    synchronized (lifecycleLock) {
      if (closed) {
        LOGGER.warn("Skipping reset for cluster {} because the state model is already closed", clusterName);
        return;
      }
      attempt = activeInitialization;
      if (attempt != null) {
        attempt.aborted.set(true);
        Future<?> initializationFuture = attempt.future;
        if (initializationFuture != null && !initializationFuture.isDone()) {
          initializationFuture.cancel(true);
        }
      }
    }
    submitCleanup(detachPublishedResources(), attempt);
  }

  private CleanupSnapshot detachPublishedResources() {
    synchronized (lifecycleLock) {
      CleanupSnapshot snapshot = new CleanupSnapshot(clusterResources, helixManager);
      clusterResources = null;
      helixManager = null;
      return snapshot;
    }
  }

  private void submitCleanup(CleanupSnapshot cleanupSnapshot, LeaderInitializationAttempt attempt) {
    synchronized (lifecycleLock) {
      if (cleanupService.isShutdown()) {
        return;
      }
      latestCleanupFuture = cleanupService.submit(() -> {
        cleanupPublishedResources(cleanupSnapshot);
        disconnectAttemptManager(attempt);
      });
    }
  }

  private void cleanupUnpublishedAttempt(LeaderInitializationAttempt attempt) {
    try {
      HelixVeniceClusterResources resources = attempt.clusterResources;
      if (resources != null) {
        try (AutoCloseableLock ignore = resources.lockForShutdown()) {
          clearResources(resources);
        }
      }
    } finally {
      disconnectAttemptManager(attempt);
      synchronized (lifecycleLock) {
        if (activeInitialization == attempt) {
          activeInitialization = null;
        }
      }
    }
  }

  private void cleanupPublishedResources(CleanupSnapshot cleanupSnapshot) {
    try {
      if (cleanupSnapshot.clusterResources != null) {
        try (AutoCloseableLock ignore = cleanupSnapshot.clusterResources.lockForShutdown()) {
          clearResources(cleanupSnapshot.clusterResources);
        }
      }
    } finally {
      if (cleanupSnapshot.helixManager != null) {
        cleanupSnapshot.helixManager.disconnect();
      }
    }
  }

  private void disconnectAttemptManager(LeaderInitializationAttempt attempt) {
    if (attempt != null && attempt.helixManager != null && attempt.managerDisconnected.compareAndSet(false, true)) {
      attempt.helixManager.disconnect();
    }
  }

  private void clearResources(HelixVeniceClusterResources resources) {
    resources.stopMultiTaskSchedulerService();
    resources.stopLogCompactionService();
    resources.stopProtocolVersionAutoDetectionService();
    /**
     * Leaked push status clean up service depends on VeniceHelixAdmin, so VeniceHelixAdmin should be stopped after
     * its dependent service.
     */
    resources.stopLeakedPushStatusCleanUpService();
    resources.stopDeadStoreStatsPreFetchTask();
    resources.stopErrorPartitionResetTask();
    resources.clear();
  }

  /**
   * Get the regular Venice cluster name after removing the suffix {@code PARTITION_SUFFIX}.
   * @param partitionName controller partition name.
   * @return Venice cluster name.
   */
  protected static String getVeniceClusterNameFromPartitionName(String partitionName) {
    // Exclude the partition id.
    if (!partitionName.endsWith(PARTITION_SUFFIX)) {
      throw new VeniceException("Invalid partition name:" + partitionName + " should end with " + PARTITION_SUFFIX);
    }
    return partitionName.substring(0, partitionName.lastIndexOf('_'));
  }

  /**
   * Get the controller partition name. The suffix {@code PARTITION_SUFFIX} is used after the regular cluster name.
   * @param veniceClusterName Venice cluster name.
   * @return partition name for the input Venice cluster.
   */
  protected static String getPartitionNameFromVeniceClusterName(String veniceClusterName) {
    return veniceClusterName + PARTITION_SUFFIX;
  }

  /**
   * @return an {@code Optional} describing the Venice cluster aggregated resources, if non-null,
   * otherwise returns an empty {@code Optional}.
   */
  protected Optional<HelixVeniceClusterResources> getResources() {
    return Optional.ofNullable(clusterResources);
  }

  /**
   * @return the name of the Venice cluster that the model manages.
   */
  protected String getClusterName() {
    return clusterName;
  }

  /**
   * Clean up controller-cluster resources before shutting down dependent services.
   */
  public void close() {
    LeaderInitializationAttempt attempt;
    synchronized (lifecycleLock) {
      if (closed) {
        return;
      }
      closed = true;
      attempt = activeInitialization;
      if (attempt != null) {
        attempt.aborted.set(true);
      }
      workerService.shutdownNow();
    }

    Future<?> cleanupFuture = cleanupService.submit(() -> {
      cleanupPublishedResources(detachPublishedResources());
      disconnectAttemptManager(attempt);
    });
    cleanupService.shutdown();

    long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_TIMEOUT_MS);
    try {
      cleanupFuture.get(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      long remainingNs = deadlineNs - System.nanoTime();
      if (remainingNs <= 0 || !workerService.awaitTermination(remainingNs, TimeUnit.NANOSECONDS)) {
        throw new VeniceException("Timed out waiting for controller state model worker for cluster " + clusterName);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while closing controller state model for cluster " + clusterName, e);
    } catch (ExecutionException e) {
      throw new VeniceException("Failed to close controller state model for cluster " + clusterName, e.getCause());
    } catch (TimeoutException e) {
      cleanupService.shutdownNow();
      throw new VeniceException("Timed out closing controller state model for cluster " + clusterName, e);
    }
  }

  @VisibleForTesting
  void setClusterResources(HelixVeniceClusterResources clusterResources) {
    this.clusterResources = clusterResources;
  }

  @VisibleForTesting
  void setClusterConfig(VeniceControllerClusterConfig config) {
    this.clusterConfig = config;
  }

  @VisibleForTesting
  void setHelixManager(SafeHelixManager helixManager) {
    this.helixManager = helixManager;
  }

  @VisibleForTesting
  ExecutorService getWorkService() {
    return workerService;
  }

  private static class LeaderInitializationAttempt {
    private final AtomicBoolean aborted = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean managerDisconnected = new AtomicBoolean(false);
    private volatile Future<?> future;
    private volatile SafeHelixManager helixManager;
    private volatile HelixVeniceClusterResources clusterResources;
  }

  private static class CleanupSnapshot {
    private final HelixVeniceClusterResources clusterResources;
    private final SafeHelixManager helixManager;

    private CleanupSnapshot(HelixVeniceClusterResources clusterResources, SafeHelixManager helixManager) {
      this.clusterResources = clusterResources;
      this.helixManager = helixManager;
    }
  }
}
