package com.example.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.example.hadoop.sql.HiveConnectionBean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class HiveJdbcService {
    private final HiveConnectionBean hiveConnectionBean;

    public void testJdbc() {
        try {
            Connection conn = hiveConnectionBean.getConnection();
            Statement stmt = conn.createStatement();
            String sql = "SHOW TABLES";
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            res.close();
            stmt.close();
        } catch (Exception e) {
            log.error("jdbc测试异常：【{}】", e.getMessage(), e);
        } finally {
            hiveConnectionBean.destroy();
        }
    }
}
