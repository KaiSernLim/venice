package com.linkedin.venice.controller;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVeniceControllerStateModel {
  private VeniceControllerStateModel stateModel;
  private Message mockMessage;
  private NotificationContext mockContext;

  private HelixVeniceClusterResources mockClusterResources;
  private VeniceControllerMultiClusterConfig mockMultiClusterConfig;

  @BeforeMethod
  public void setUp() {
    mockMessage = mock(Message.class);
    mockContext = mock(NotificationContext.class);
    mockClusterResources = mock(HelixVeniceClusterResources.class);
    mockMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    stateModel = new VeniceControllerStateModel(
        "test-cluster",
        mock(ZkClient.class),
        mock(HelixAdapterSerializer.class),
        mockMultiClusterConfig,
        mock(VeniceHelixAdmin.class),
        mock(MetricsRepository.class),
        mock(ClusterLeaderInitializationRoutine.class),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class),
        Optional.empty());
  }

  @Test(timeOut = 10000)
  public void testStateTransitionsAreSerialized() throws Exception {
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch allowCleanupToFinish = new CountDownLatch(1);
    CountDownLatch leaderTransitionWaiting = new CountDownLatch(1);
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch leaderTransitionFinished = new CountDownLatch(1);
    AtomicBoolean leaderTransitionFailed = new AtomicBoolean(false);

    when(mockMessage.getTgtName()).thenReturn("test-controller");
    when(mockMessage.getFromState()).thenReturn("LEADER");
    when(mockMessage.getToState()).thenReturn("STANDBY");
    when(mockMessage.getResourceName()).thenReturn("test_v1");
    when(mockClusterResources.lockForShutdown()).thenReturn(mock(AutoCloseableLock.class));
    doAnswer(invocation -> {
      cleanupStarted.countDown();
      allowCleanupToFinish.await();
      return null;
    }).when(mockClusterResources).stopLeakedPushStatusCleanUpService();

    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenAnswer(invocation -> {
      leaderTransitionWaiting.countDown();
      return TimeUnit.SECONDS.toMillis(5);
    });
    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    HelixVeniceClusterResources initializedResources = mock(HelixVeniceClusterResources.class);

    stateModel = spy(stateModel);
    stateModel.setClusterResources(mockClusterResources);
    stateModel.setClusterConfig(clusterConfig);
    doAnswer(invocation -> {
      return initializedManager;
    }).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> {
      initializationStarted.countDown();
      return initializedResources;
    }).when(stateModel).createClusterResources(initializedManager);

    stateModel.onBecomeStandbyFromLeader(mockMessage, mockContext);
    assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));

    Thread leaderTransitionThread = new Thread(() -> {
      try {
        stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);
      } catch (VeniceException e) {
        leaderTransitionFailed.set(true);
      } finally {
        leaderTransitionFinished.countDown();
      }
    });
    leaderTransitionThread.start();

    assertTrue(leaderTransitionWaiting.await(5, TimeUnit.SECONDS));
    assertEquals(initializationStarted.getCount(), 1L);
    allowCleanupToFinish.countDown();
    assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
    assertTrue(leaderTransitionFinished.await(5, TimeUnit.SECONDS));
    assertFalse(leaderTransitionFailed.get());
  }

  @Test
  public void testStateModelClose() {
    VeniceDistClusterControllerStateModelFactory factory = new VeniceDistClusterControllerStateModelFactory(
        mock(ZkClient.class),
        mock(HelixAdapterSerializer.class),
        mock(VeniceHelixAdmin.class),
        mock(VeniceControllerMultiClusterConfig.class),
        mock(MetricsRepository.class),
        mock(ClusterLeaderInitializationRoutine.class),
        mock(RealTimeTopicSwitcher.class),
        Optional.empty(),
        mock(HelixAdminClient.class),
        Optional.empty());
    int testPartition = 0;
    String resourceName = Version.composeKafkaTopic("testStore", 1);
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    factory.createNewStateModel(resourceName, partitionName);
    factory.close();
    assertTrue(factory.getModel(resourceName).getWorkService().isShutdown());
  }

  @Test(timeOut = 10000)
  public void testCloseWaitsForCleanupBeforeReturning() throws Exception {
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch allowCleanupToFinish = new CountDownLatch(1);
    CountDownLatch closeFinished = new CountDownLatch(1);
    CountDownLatch managerDisconnected = new CountDownLatch(1);

    when(mockClusterResources.lockForShutdown()).thenReturn(mock(AutoCloseableLock.class));
    doAnswer(invocation -> {
      cleanupStarted.countDown();
      allowCleanupToFinish.await();
      return null;
    }).when(mockClusterResources).clear();
    stateModel.setClusterResources(mockClusterResources);
    stateModel.setHelixManager(mockConnectedHelixManager("test-instance", managerDisconnected));

    Thread closeThread = new Thread(() -> {
      stateModel.close();
      closeFinished.countDown();
    });
    closeThread.start();

    assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
    assertEquals(closeFinished.getCount(), 1L);
    allowCleanupToFinish.countDown();
    assertTrue(closeFinished.await(5, TimeUnit.SECONDS));
    assertEquals(managerDisconnected.getCount(), 0L);
  }

  @Test(timeOut = 10000)
  public void testClosePreservesInterruptStatus() throws Exception {
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch allowCleanupToFinish = new CountDownLatch(1);
    CountDownLatch closeFinished = new CountDownLatch(1);
    AtomicBoolean closeFailed = new AtomicBoolean(false);
    AtomicBoolean interruptStatusPreserved = new AtomicBoolean(false);

    when(mockClusterResources.lockForShutdown()).thenReturn(mock(AutoCloseableLock.class));
    doAnswer(invocation -> {
      cleanupStarted.countDown();
      allowCleanupToFinish.await();
      return null;
    }).when(mockClusterResources).clear();
    stateModel.setClusterResources(mockClusterResources);

    Thread closeThread = new Thread(() -> {
      try {
        stateModel.close();
      } catch (VeniceException e) {
        closeFailed.set(true);
        interruptStatusPreserved.set(Thread.currentThread().isInterrupted());
      } finally {
        closeFinished.countDown();
      }
    });
    closeThread.start();

    assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
    closeThread.interrupt();
    assertTrue(closeFinished.await(5, TimeUnit.SECONDS));
    assertTrue(closeFailed.get());
    assertTrue(interruptStatusPreserved.get());
    allowCleanupToFinish.countDown();
  }

  @Test
  public void testCloseSurfacesCleanupFailure() {
    SafeHelixManager manager = mock(SafeHelixManager.class);
    doAnswer(invocation -> {
      throw new VeniceException("Disconnect failed");
    }).when(manager).disconnect();
    stateModel.setHelixManager(manager);

    assertThrows(VeniceException.class, stateModel::close);
  }

  @Test(timeOut = 10000)
  public void testStateTransitionTimeoutInterruptsResourceInitialization() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    CountDownLatch managerDisconnected = new CountDownLatch(1);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenAnswer(invocation -> {
      assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
      return 100L;
    });
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance", managerDisconnected);
    HelixVeniceClusterResources initializingResources = mock(HelixVeniceClusterResources.class);
    stateModel = spy(stateModel);
    doAnswer(invocation -> initializedManager).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> initializingResources).when(stateModel).createClusterResources(initializedManager);
    doAnswer(invocation -> {
      initializationStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        initializationInterrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(initializingResources).refresh();

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));

    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));
    assertTrue(managerDisconnected.await(5, TimeUnit.SECONDS));
    verify(clusterConfig).getControllerStandbyToLeaderTransitionTimeoutMs();
  }

  @Test(timeOut = 10000)
  public void testStateTransitionExecutionException() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(5));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    HelixVeniceClusterResources initializingResources = mock(HelixVeniceClusterResources.class);
    doAnswer(invocation -> {
      initializationStarted.countDown();
      throw new VeniceException("Resource initialization failed");
    }).when(initializingResources).refresh();
    stateModel = spy(stateModel);
    doAnswer(invocation -> initializedManager).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> initializingResources).when(stateModel).createClusterResources(initializedManager);

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
  }

  @Test(timeOut = 10000)
  public void testInterruptedTransitionPreservesInterruptStatus() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    CountDownLatch transitionFinished = new CountDownLatch(1);
    AtomicBoolean transitionFailed = new AtomicBoolean(false);
    AtomicBoolean interruptStatusPreserved = new AtomicBoolean(false);

    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(30));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance");
    HelixVeniceClusterResources initializingResources = mock(HelixVeniceClusterResources.class);
    stateModel = spy(stateModel);
    doAnswer(invocation -> initializedManager).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> initializingResources).when(stateModel).createClusterResources(initializedManager);
    doAnswer(invocation -> {
      initializationStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        initializationInterrupted.countDown();
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(initializingResources).refresh();

    Thread transitionThread = new Thread(() -> {
      try {
        stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);
      } catch (VeniceException e) {
        transitionFailed.set(true);
        interruptStatusPreserved.set(Thread.currentThread().isInterrupted());
      } finally {
        transitionFinished.countDown();
      }
    });
    transitionThread.start();

    assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
    transitionThread.interrupt();
    assertTrue(transitionFinished.await(5, TimeUnit.SECONDS));
    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));
    assertTrue(transitionFailed.get());
    assertTrue(interruptStatusPreserved.get());
  }

  @Test
  public void testResetDisconnectsManagerWithoutClusterResources() throws InterruptedException {
    CountDownLatch managerDisconnected = new CountDownLatch(1);
    SafeHelixManager manager = mockConnectedHelixManager("test-instance", managerDisconnected);
    stateModel.setHelixManager(manager);

    stateModel.reset();

    assertTrue(managerDisconnected.await(5, TimeUnit.SECONDS));
  }

  @Test(timeOut = 10000)
  public void testTimeoutDisconnectsManagerWhileInitializationIgnoresInterruption() throws Exception {
    CountDownLatch initializationStarted = new CountDownLatch(1);
    CountDownLatch initializationInterrupted = new CountDownLatch(1);
    CountDownLatch allowInitializationToFinish = new CountDownLatch(1);
    CountDownLatch managerDisconnected = new CountDownLatch(1);
    CountDownLatch resourcesCleared = new CountDownLatch(1);

    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenAnswer(invocation -> {
      assertTrue(initializationStarted.await(5, TimeUnit.SECONDS));
      return 100L;
    });
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager initializedManager = mockConnectedHelixManager("test-instance", managerDisconnected);
    HelixVeniceClusterResources initializingResources = mock(HelixVeniceClusterResources.class);
    when(initializingResources.lockForShutdown()).thenReturn(mock(AutoCloseableLock.class));
    doAnswer(invocation -> {
      initializationStarted.countDown();
      awaitIgnoringInterrupt(allowInitializationToFinish, initializationInterrupted);
      return null;
    }).when(initializingResources).refresh();
    doAnswer(invocation -> {
      resourcesCleared.countDown();
      return null;
    }).when(initializingResources).clear();
    stateModel = spy(stateModel);
    doAnswer(invocation -> initializedManager).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> initializingResources).when(stateModel).createClusterResources(initializedManager);

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    assertTrue(initializationInterrupted.await(5, TimeUnit.SECONDS));
    assertTrue(
        managerDisconnected.await(5, TimeUnit.SECONDS),
        "Timeout should disconnect the manager without waiting for resource initialization");
    verify(initializingResources, never()).clear();
    assertFalse(stateModel.getResources().isPresent());

    allowInitializationToFinish.countDown();
    assertTrue(resourcesCleared.await(5, TimeUnit.SECONDS));
    verify(initializingResources, never()).startErrorPartitionResetTask();
  }

  @Test(timeOut = 10000)
  public void testStaleManagerFailsTransitionUntilReset() throws Exception {
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    when(clusterConfig.getControllerStandbyToLeaderTransitionTimeoutMs()).thenReturn(TimeUnit.SECONDS.toMillis(5));
    stateModel.setClusterConfig(clusterConfig);
    configureLeaderTransitionMessage();

    SafeHelixManager staleManager = mockConnectedHelixManager("stale-instance");
    SafeHelixManager newManager = mockConnectedHelixManager("new-instance");
    HelixVeniceClusterResources newResources = mock(HelixVeniceClusterResources.class);
    stateModel.setHelixManager(staleManager);
    stateModel = spy(stateModel);
    doAnswer(invocation -> newManager).when(stateModel).createHelixManager("test-controller");
    doAnswer(invocation -> newResources).when(stateModel).createClusterResources(newManager);

    assertThrows(VeniceException.class, () -> stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext));
    verify(stateModel, never()).createHelixManager("test-controller");
    stateModel.onBecomeLeaderFromStandby(mockMessage, mockContext);

    verify(stateModel).createHelixManager("test-controller");
    verify(newManager, never()).disconnect();
  }

  private void configureLeaderTransitionMessage() {
    when(mockMessage.getTgtName()).thenReturn("test-controller");
    when(mockMessage.getFromState()).thenReturn("STANDBY");
    when(mockMessage.getToState()).thenReturn("LEADER");
    when(mockMessage.getResourceName()).thenReturn("test-cluster_0");
  }

  private SafeHelixManager mockConnectedHelixManager(String instanceName) {
    return mockConnectedHelixManager(instanceName, null);
  }

  private SafeHelixManager mockConnectedHelixManager(String instanceName, CountDownLatch disconnected) {
    SafeHelixManager helixManager = mock(SafeHelixManager.class);
    when(helixManager.isConnected()).thenReturn(true);
    when(helixManager.getInstanceName()).thenReturn(instanceName);
    if (disconnected != null) {
      doAnswer(invocation -> {
        disconnected.countDown();
        return null;
      }).when(helixManager).disconnect();
    }
    return helixManager;
  }

  private void awaitIgnoringInterrupt(CountDownLatch latch, CountDownLatch interrupted) {
    while (true) {
      try {
        latch.await();
        return;
      } catch (InterruptedException e) {
        interrupted.countDown();
      }
    }
  }

}
