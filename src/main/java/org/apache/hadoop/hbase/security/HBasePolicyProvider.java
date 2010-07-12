package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * Implementation of secure Hadoop policy provider for mapping
 * protocol interfaces to hadoop-policy.xml entries.
 */
public class HBasePolicyProvider extends PolicyProvider {
  protected static Service[] services = {
      new Service("security.client.protocol.acl", HRegionInterface.class),
      new Service("security.admin.protocol.acl", HMasterInterface.class),
      new Service("security.masterregion.protocol.acl", HMasterRegionInterface.class)
  };

  @Override
  public Service[] getServices() {
    return services;
  }

  public static void init(Configuration conf) {
    // set service-level authorization security policy
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      ServiceAuthorizationManager.refresh(conf, new HBasePolicyProvider());
    }
  }
}
