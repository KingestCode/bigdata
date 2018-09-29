package cn.edu360.web.controller;

import cn.edu360.web.pojo.Bike;
import cn.edu360.web.service.BikeService;
import cn.edu360.web.service.UserService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.GeoResults;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import java.util.List;


@Controller
public class BikeController {

	@Autowired
	private BikeService bikeService;

	@RequestMapping("/bike_list")
	public String list() {
		return "bike/list";
	}

	@RequestMapping("/bike_add")
	public String toAdd() {
		return "bike/add";
	}

	@RequestMapping("/bike_edit")
	public String toEdit() {
		return "bike/edit";
	}

	@PostMapping("/bike")
	@ResponseBody
	public String add(@RequestBody String bike) {
		Bike b = JSONObject.parseObject(bike, Bike.class);
		bikeService.save(b);
		return "success";
	}

	@PostMapping("/bike_edit")
	@ResponseBody
	public String edit(Bike bike) {
		bikeService.update(bike);
		return "success";
	}

	@DeleteMapping("/bike/{ids}")
	@ResponseBody
	public String deleteByIds(@PathVariable("ids") Long[] ids) {
		bikeService.deleteByIds(ids);
		return "success";
	}

	@GetMapping("/bike/{id}")
	@ResponseBody
	public Bike getById(@PathVariable("id") Long id) {
		return bikeService.getById(id);
	}


	/**
	 * 查找当前坐标附近的单车
	 * @param longitude
	 * @param latitude
	 * @return
	 */
	@GetMapping("/bikes")
	@ResponseBody
	public GeoResults<Bike> findNear(double longitude, double latitude) {
		GeoResults<Bike> bikes = bikeService.findNear(longitude, latitude);
		return bikes;
	}

}
