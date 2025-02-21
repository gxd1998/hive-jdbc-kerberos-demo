package com.example.web;


import com.example.service.HiveJdbcService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author gxd
 * @since 2023-07-17
 */
@Slf4j
@RestController
@RequestMapping("/api/hive")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class HiveController {

    private final HiveJdbcService hiveJdbcService;

    /***
     */
    @GetMapping("/test")
    public void hiveTest() {
        log.info("进入【HiveController.hiveTest】方法");
        hiveJdbcService.testJdbc();
    }
}

