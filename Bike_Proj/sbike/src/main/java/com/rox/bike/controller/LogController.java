package com.rox.bike.controller;

import com.rox.bike.service.LogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class LogController {

    @Autowired
    private LogService logService;

    @PostMapping("/log/ready")
    @ResponseBody  // 返回 响应的 json 字符串
    public String ready(@RequestBody String log) {  //请求体为 json
        logService.save(log);
        return "success";
    }


}
