package com.rox.bike.service;

import com.rox.bike.pojo.Bike;

import java.util.List;

public interface BikeService {

    void save(Bike bike);

    void save(String bike);

    List<Bike> findAll();
}

