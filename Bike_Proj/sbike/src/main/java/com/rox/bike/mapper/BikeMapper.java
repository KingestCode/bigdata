package com.rox.bike.mapper;

import com.rox.bike.pojo.Bike;
import org.apache.ibatis.annotations.Mapper;

/**
 * 这里别忘了加上注解
 */
@Mapper
public interface BikeMapper {

    void save(Bike bike);
}
