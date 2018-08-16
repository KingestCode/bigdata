package com.rox.bike.service;

import com.rox.bike.mapper.BikeMapper;
import com.rox.bike.pojo.Bike;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// 添加事务
@Transactional
@Service
public class BikeServiceImpl implements BikeService {

    @Autowired
    private BikeMapper bikeMapper;


    @Override
    public void save(Bike bike) {

        bikeMapper.save(bike);
        int i= 100/0;
        bikeMapper.save(bike);
    }
}
