package com.example.hadoop.sql;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author xd.guo
 * @date 2025/01/13
 */
@Slf4j
@Component
public class HiveConnectionBean implements DisposableBean {
    @Value("${hadoop.hive.driver}")
    private String driver;
    @Value("${hadoop.hive.principal}")
    private String principal;
    @Value("${hadoop.hive.krb5}")
    private String krb5;
    @Value("${hadoop.hive.keytab}")
    private String keytab;
    @Value("${hadoop.hive.realm}")
    private String realm;
    @Value("${hadoop.hive.kdc}")
    private String kdc;
    @Value("${hadoop.hive.authentication}")
    private String authentication;
    @Value("${hadoop.hive.debug}")
    private String debug;
    @Value("${hadoop.hive.host}")
    private String host;
    @Value("${hadoop.hive.port}")
    private String port;
    @Value("${hadoop.hive.enable}")
    private boolean enable;
    @Value("${hadoop.hive.username}")
    private String username;
    @Value("${hadoop.hive.password}")
    private String password;
    private Connection connection;

    private String url;

    /**
     * 建立数据库连接
     *
     * @return Connection
     */
    public Connection getConnection() {
        if (connection == null) {
            try {
                Class.forName(driver);
                connection = connect();
            } catch (ClassNotFoundException e) {
                log.error("建立数据库连接异常: 【{}】", e.getMessage(), e);
                throw new RuntimeException("建立数据库连接异常: " + e.getMessage());
            }
        }
        return connection;
    }

    private void kerberosAuth() {
        System.setProperty("java.security.krb5.conf", krb5);
        System.setProperty("java.security.krb5.realm", realm);
        System.setProperty("java.security.krb5.kdc", kdc);
        System.setProperty("sun.security.krb5.debug", debug);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", authentication);
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (Exception e) {
            log.error("kerberos认证失败:【{}】 ", e.getMessage(), e);
            throw new RuntimeException("kerberos认证失败:" + e.getMessage());
        }
    }

    private Connection connect() {
        Connection conn = null;
        url = String.format("jdbc:hive2://%s:%s/default;principal=%s", host, port, principal);
        if (enable) {
            kerberosAuth();
            log.info("kerberos认证成功");
            try {
                conn = DriverManager.getConnection(url);
            } catch (Exception e) {
                log.error("建立kerberos认证数据库连接异常：【{}】", e.getMessage(), e);
            }
        } else {
            try {
                conn = DriverManager.getConnection(url, username, password);
            } catch (Exception e) {
                log.error("建立非kerberos认证数据库连接异常：【{}】", e.getMessage(), e);
            }
        }
        return conn;
    }

    @Override
    public void destroy() {
        log.info("销毁iconnection bean");
        closeConnetion();
    }


    private void closeConnetion() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("关闭jdbc连接失败: 【{}】", e.getMessage(), e);
            } finally {
                connection = null;
            }
        }
    }
}
