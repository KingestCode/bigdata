package com.rox.bike.controller;

import com.rox.bike.pojo.Bike;
import com.rox.bike.service.BikeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
public class BikeController {

/*    @PostMapping("/bike")
    @ResponseBody  //响应Ajax请求，会将响应的对象转成json
    public String getById(@RequestBody String id) {
        // @ResponseBody 请求时接受 json 类型的数据
        System.out.println(id);
        return "succ";
    }*/

    @Autowired
    private BikeService bikeService;

    @GetMapping("/bike")
    @ResponseBody  //响应Ajax请求，会将响应的对象转成json
    public String getById(Bike bike) {

        //调用 service 保存 bike
        bikeService.save(bike);

        System.out.println(bike.toString());
        return "success";
    }


    @PostMapping("/bike")
    @ResponseBody  //响应Ajax请求，会将响应的对象转成json
    public String getById(@RequestBody String bike) {

        //调用 service 保存 bike
        bikeService.save(bike);

        System.out.println(bike.toString());
        return "success";
    }

    @GetMapping("/bikes")
    @ResponseBody
    public List<Bike> findAll() {
        return bikeService.findAll();
    }

    //先跳转到视图界面
    @GetMapping("/bike_list")
    public String toList() {
        return "bike/list";
    }





}
















