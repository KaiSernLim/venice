package com.linkedin.venice.meta;

import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;

import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * This is an abstraction of metadata maintained per Store.
 */
public abstract class AbstractStore implements Store {
  public static final int DEFAULT_REPLICATION_FACTOR = 3;
  /**
   * Default storage quota 20GB
   */
  public static final long DEFAULT_STORAGE_QUOTA = (long) 20 * (1 << 30);

  /**
   * Default read quota 1800 QPS per node
   */
  public static final long DEFAULT_READ_QUOTA = 1800;

  protected interface StoreVersionSupplier {
    /**
     * This function will return a reference to the internal versions structure, and any change applying to the returned
     * object will be reflected in the referenced {@link Store}.
     * @return
     */
    List<StoreVersion> getForUpdate();

    /**
     * This function will return a list of Versions, which are read-only, and any modification against them will
     * throw {@link UnsupportedOperationException}.
     * @return
     */
    List<Version> getForRead();
  }

  /**
   * This field is to let current class talk to the inherited classes for version related operations.
   */
  private StoreVersionSupplier storeVersionsSupplier;

  /**
   * This function should be invoked only once.
   */
  protected synchronized void setupVersionSupplier(StoreVersionSupplier versionsSupplier) {
    if (this.storeVersionsSupplier != null) {
      throw new VeniceException("Field: 'storeVersionsSupplier' shouldn't be setup more than once");
    }
    this.storeVersionsSupplier = versionsSupplier;
  }

  private void checkVersionSupplier() {
    if (this.storeVersionsSupplier == null) {
      throw new VeniceException("Field: 'storeVersionsSupplier' hasn't been setup yet");
    }
  }

  @Override
  public List<Version> getVersions() {
    checkVersionSupplier();
    return storeVersionsSupplier.getForRead();
  }

  @Override
  public void setVersions(List<Version> versions) {
    checkVersionSupplier();
    storeVersionsSupplier.getForUpdate().clear();
    versions.forEach(v -> storeVersionsSupplier.getForUpdate().add(v.dataModel()));
  }

  @Override
  public void addVersion(Version version) {
    addVersion(version, true, false, DEFAULT_RT_VERSION_NUMBER);
  }

  @Override
  public void addVersion(Version version, boolean isClonedVersion, int currentRTVersionNumber) {
    addVersion(version, true, isClonedVersion, currentRTVersionNumber);
  }

  @Override
  public void forceAddVersion(Version version, boolean isClonedVersion) {
    addVersion(version, false, isClonedVersion, DEFAULT_RT_VERSION_NUMBER);
  }

  @Override
  public void checkDisableStoreWrite(String action, int version) {
    if (!isEnableWrites()) {
      throw new StoreDisabledException(getName(), action, version);
    }
  }

  /**
   * Add a version into store
   * @param version
   * @param checkDisableWrite if checkDisableWrite is true, and the store is disabled to write, then this will throw a StoreDisabledException.
   *                    Setting to false will ignore the enableWrites status of the store (for example for cloning a store).
   * @param isClonedVersion if true, the version being added is cloned from an existing version instance, so don't apply
   *                        any store level config on it; if false, the version being added is new version, so the new version
   *                        config should be the same as store config.
   */
  private void addVersion(
      Version version,
      boolean checkDisableWrite,
      boolean isClonedVersion,
      int currentRTVersionNumber) {
    checkVersionSupplier();
    if (checkDisableWrite) {
      checkDisableStoreWrite("add", version.getNumber());
    }
    if (!getName().equals(version.getStoreName())) {
      throw new VeniceException("Version does not belong to this store.");
    }
    int index = 0;
    for (; index < storeVersionsSupplier.getForUpdate().size(); index++) {
      if (storeVersionsSupplier.getForUpdate().get(index).number == version.getNumber()) {
        throw new VeniceException("Version is repeated. Store: " + getName() + " Version: " + version.getNumber());
      }
      if (storeVersionsSupplier.getForUpdate().get(index).number > version.getNumber()) {
        break;
      }
    }

    if (!isClonedVersion) {
      /**
       * Important:
       * We need to clone the object from the store config here since the version-level config could be
       * changed after. Without a new copy, the following version-level change will reflect in the store-level
       * config as well since they are referring to the same object.
       */
      // For new version, apply store level config on it.
      // update version compression type
      version.setCompressionStrategy(getCompressionStrategy());

      version.setChunkingEnabled(isChunkingEnabled());
      version.setRmdChunkingEnabled(isRmdChunkingEnabled());

      PartitionerConfig partitionerConfig = getPartitionerConfig();
      if (partitionerConfig != null) {
        version.setPartitionerConfig(partitionerConfig.clone());
      }

      version.setNativeReplicationEnabled(isNativeReplicationEnabled());

      version.setReplicationFactor(getReplicationFactor());

      version.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());

      version.setIncrementalPushEnabled(isIncrementalPushEnabled());

      version.setSeparateRealTimeTopicEnabled(isSeparateRealTimeTopicEnabled());

      version.setBlobTransferEnabled(isBlobTransferEnabled());

      version.setUseVersionLevelIncrementalPushEnabled(true);

      version.setTargetSwapRegion(getTargetSwapRegion());

      version.setTargetSwapRegionWaitTime(getTargetSwapRegionWaitTime());

      version.setGlobalRtDivEnabled(isGlobalRtDivEnabled());

      HybridStoreConfig hybridStoreConfig = getHybridStoreConfig();
      if (hybridStoreConfig != null) {
        HybridStoreConfig clonedHybridStoreConfig = hybridStoreConfig.clone();
        if (currentRTVersionNumber > DEFAULT_RT_VERSION_NUMBER) {
          String newRealTimeTopicName = Utils.isRTVersioningApplicable(getName())
              ? Utils.composeRealTimeTopic(getName(), currentRTVersionNumber)
              : DEFAULT_REAL_TIME_TOPIC_NAME;
          clonedHybridStoreConfig.setRealTimeTopicName(newRealTimeTopicName);
        }
        version.setHybridStoreConfig(clonedHybridStoreConfig);
      }

      version.setUseVersionLevelHybridConfig(true);

      version.setActiveActiveReplicationEnabled(isActiveActiveReplicationEnabled());
      version.setViewConfigs(getViewConfigs());
    }

    storeVersionsSupplier.getForUpdate().add(index, version.dataModel());
    if (version.getNumber() > getLargestUsedVersionNumber()) {
      setLargestUsedVersionNumber(version.getNumber());
    }
  }

  @Override
  public Version deleteVersion(int versionNumber) {
    checkVersionSupplier();
    for (int i = 0; i < storeVersionsSupplier.getForUpdate().size(); i++) {
      Version version = new VersionImpl(storeVersionsSupplier.getForUpdate().get(i));
      if (version.getNumber() == versionNumber) {
        storeVersionsSupplier.getForUpdate().remove(i);
        return version;
      }
    }
    return null;
  }

  @Override
  public boolean containsVersion(int versionNumber) {
    checkVersionSupplier();
    for (Version version: storeVersionsSupplier.getForRead()) {
      if (version.getNumber() == versionNumber) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateVersionStatus(int versionNumber, VersionStatus status) {
    checkVersionSupplier();
    if (status.equals(VersionStatus.ONLINE)) {
      checkDisableStoreWrite("become ONLINE", versionNumber);
    }
    for (int i = storeVersionsSupplier.getForUpdate().size() - 1; i >= 0; i--) {
      Version version = new VersionImpl(storeVersionsSupplier.getForUpdate().get(i));
      if (version.getNumber() == versionNumber) {
        version.setStatus(status);
        return;
      }
    }
    throw new VeniceException("Version:" + versionNumber + " does not exist");
  }

  @Override
  public int peekNextVersionNumber() {
    int nextVersionNumber = getLargestUsedVersionNumber() + 1;
    checkDisableStoreWrite("increase", nextVersionNumber);
    return nextVersionNumber;
  }

  @Override
  @Nullable
  public Version getVersion(int versionNumber) {
    checkVersionSupplier();
    for (Version version: storeVersionsSupplier.getForRead()) {
      if (version.getNumber() == versionNumber) {
        return version;
      }
    }

    return null;
  }

  @Override
  @Nonnull
  public Version getVersionOrThrow(int versionNumber) throws StoreVersionNotFoundException {
    Version version = getVersion(versionNumber);
    if (version == null) {
      throw new StoreVersionNotFoundException(getName(), versionNumber);
    }
    return version;
  }

  @Override
  public VersionStatus getVersionStatus(int versionNumber) {
    Version version = getVersion(versionNumber);
    if (version == null) {
      return VersionStatus.NOT_CREATED;
    }

    return version.getStatus();
  }

  @Override
  public List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve) {
    checkVersionSupplier();
    int curNumVersionsToPreserve = clusterNumVersionsToPreserve;
    if (getNumVersionsToPreserve() != NUM_VERSION_PRESERVE_NOT_SET) {
      curNumVersionsToPreserve = getNumVersionsToPreserve();
    }
    // when numVersionsToPreserve is less than 1, it usually means a config issue.
    // Setting it to zero, will cause the store to be deleted as soon as push completes.
    if (curNumVersionsToPreserve < 1) {
      throw new IllegalArgumentException(
          "At least 1 version should be preserved. Parameter " + curNumVersionsToPreserve);
    }

    List<Version> versions = storeVersionsSupplier.getForRead();
    int versionCnt = versions.size();
    if (versionCnt == 0) {
      return Collections.emptyList();
    }

    // The code assumes that Versions are sorted in increasing order by addVersion and increaseVersion
    int lastElementIndex = versionCnt - 1;
    List<Version> versionsToDelete = new ArrayList<>();

    /**
     * The current version need not be the last largest version (e.g. we rolled back to an earlier version).
     * The versions which can be deleted are:
     *     a) ONLINE versions except the current version given we preserve numVersionsToPreserve versions.
     *     b) ERROR version (ideally should not be there as AbstractPushmonitor#handleErrorPush deletes those)
     *     c) STARTED versions if its not the last one and the store is not migrating.
     *     d) KILLED versions by {@link org.apache.kafka.clients.admin.Admin#killOfflinePush} api.
     */
    for (int i = lastElementIndex; i >= 0; i--) {
      Version version = versions.get(i);

      if (version.getNumber() == getCurrentVersion()) { // currentVersion is always preserved
        curNumVersionsToPreserve--;
      } else if (VersionStatus.canDelete(version.getStatus())) { // ERROR and KILLED versions are always deleted
        versionsToDelete.add(version);
      } else if (VersionStatus.ONLINE.equals(version.getStatus())) {
        if (curNumVersionsToPreserve > 0) { // keep the minimum number of version to preserve
          curNumVersionsToPreserve--;
        } else {
          versionsToDelete.add(version);
        }
      } else if (VersionStatus.STARTED.equals(version.getStatus()) && (i != lastElementIndex) && !isMigrating()) {
        // For the non-last started version, if it's not the current version(STARTED version should not be the current
        // version, just prevent some edge cases here.), we should delete it only if the store is not migrating
        // as during store migration are there are concurrent pushes with STARTED version.
        // So if the store is not migrating, it's stuck in STARTED, it means somehow the controller did not update the
        // version status properly.
        versionsToDelete.add(version);
      }
      // TODO here we don't deal with the PUSHED version, just keep all of them, need to consider collect them too in
      // the future.
    }
    return versionsToDelete;
  }

  @Override
  public boolean isSystemStore() {
    return Store.isSystemStore(getName());
  }

  @Override
  public void fixMissingFields() {
    checkVersionSupplier();
    for (StoreVersion storeVersion: storeVersionsSupplier.getForUpdate()) {
      Version version = new VersionImpl(storeVersion);
      if (version.getPartitionerConfig() == null) {
        version.setPartitionerConfig(getPartitionerConfig());
      }
      if (version.getPartitionCount() == 0) {
        version.setPartitionCount(getPartitionCount());
      }
    }
  }

  @Override
  public void updateVersionForDaVinciHeartbeat(int versionNumber, boolean reported) {
    checkVersionSupplier();

    for (StoreVersion storeVersion: storeVersionsSupplier.getForUpdate()) {
      Version version = new VersionImpl(storeVersion);
      if (version.getNumber() == versionNumber) {
        version.setIsDavinciHeartbeatReported(reported);
        return;
      }
    }
  }
}
