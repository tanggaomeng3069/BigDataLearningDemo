package com.learning.kerberos;

import java.io.IOException;
import java.security.PrivilegedAction;

import com.learning.utils.KerberosConfigLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/13 15:12
 * @Description:
 * @Version 1.0
 */
public class HBaseClientDDL {
    private Connection conn = null;

    @Before
    public void getHBaseConn() throws Exception {
        // 设置Kerberos认证
        System.setProperty("java.security.krb5.conf", KerberosConfigLoader.getString("java.security.krb5.conf"));
        // 创建HBase链接配置
        final Configuration conf = HBaseConfiguration.create();
        // Kerberos认证
        conf.set("hbase.zookeeper.quorum", KerberosConfigLoader.getString("hbase.zookeeper.list"));
        conf.set("hbase.zookeeper.property.clientPort", KerberosConfigLoader.getString("hbase.zookeeper.port"));
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.master.kerberos.principal", KerberosConfigLoader.getString("hbase.master.kerberos.principal"));
        conf.set("hbase.regionserver.kerberos.principal", KerberosConfigLoader.getString("hbase.regionserver.kerberos.principal"));
        conf.set("zookeeper.znode.parent", KerberosConfigLoader.getString("zookeeper.znode.parent"));
        conf.set("hbase.client.retries.number", KerberosConfigLoader.getString("hbase.client.retries.number")); // 设置重试次数

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"));
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        loginUser.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    conn = ConnectionFactory.createConnection(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }


}
