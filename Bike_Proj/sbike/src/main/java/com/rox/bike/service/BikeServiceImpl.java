package com.rox.bike.service;

import com.rox.bike.mapper.BikeMapper;
import com.rox.bike.pojo.Bike;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

// 添加事务
@Transactional
@Service
public class BikeServiceImpl implements BikeService {

    @Autowired
    private BikeMapper bikeMapper;

    //注入操作 maongo 数据库的模板
    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public void save(Bike bike) {
        bikeMapper.save(bike);
    }

    @Override
    public void save(String bike) {
        mongoTemplate.save(bike, "bikes");
    }

    @Override
    public List<Bike> findAll() {
        return mongoTemplate.findAll(Bike.class, "bikes");
    }


}
