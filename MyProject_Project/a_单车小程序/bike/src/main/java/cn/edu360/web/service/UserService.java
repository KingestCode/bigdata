package cn.edu360.web.service;

import cn.edu360.web.pojo.User;

import java.util.List;


public interface UserService {

	User getById(Long id);

	List<User> findAll();

	void save(User user);

	void deleteByIds(Long[] ids);

	void update(User user);

	User login(User user);

	void register(User user);

	void genVerifyCode(String nationCode, String phoneNum) throws Exception;

	User getUserByOpenid(String openid);

	void deposit(User user);

	boolean verify(User user);

	void identify(User user);

	boolean recharge(String params);
}
