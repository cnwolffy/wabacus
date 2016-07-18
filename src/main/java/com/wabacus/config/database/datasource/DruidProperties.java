/*
 * www.yiji.com Inc.
 * Copyright (c) 2014 All Rights Reserved
 */

/*
 * 修订记录:
 * zhouyang@yiji.com 2016-07-18 09:59 创建
 *
 */
package com.wabacus.config.database.datasource;

import com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker;
import com.alibaba.druid.pool.vendor.OracleValidConnectionChecker;
import com.wabacus.exception.WabacusConfigLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.sql.SQLException;

/**
 * @author zhouyang@yiji.com
 */
public class DruidProperties {
	private static final Logger logger = LoggerFactory.getLogger(DruidProperties.class);

	private static final int ORACLE_MAX_ACTIVE = 200;

	private static final int MYSQL_MAX_ACTIVE = 100;

	/**
	 * 必填：jdbc url
	 */
	//@NotBlank(message = "数据库连接不能为空")
	//用jsr303信息展示太不直观
	private String url;

	/**
	 * 必填：数据库用户名
	 */
	private String username;

	/**
	 * 必填：数据库密码
	 */
	private String password;

	/**
	 * 初始连接数
	 */
	private Integer initialSize = 5;

	/**
	 * 最小空闲连接数
	 */
	private Integer minIdle = 20;

	/**
	 * 最大连接数
	 */
	private Integer maxActive = 100;

	/**
	 * 获取连接等待超时的时间
	 */
	private Integer maxWait = 10000;

	/**
	 * 慢sql日志阈值，超过此值则打印日志
	 */
	private Integer slowSqlThreshold = 5000;

	/**
	 * 大结果集阈值，超过此值则打印日志
	 */
	private Integer maxResultThreshold = 1000;

	/**
	 * 是否在非线上环境开启打印sql，默认开启
	 */
	private boolean showSql = true;

	private ClassLoader beanClassLoader;

	public void check() {
		Assert.hasText(url, "数据库连接yiji.ds.url不能为空");
		Assert.hasText(username, "数据库用户名yiji.ds.username不能为空");
		Assert.hasText(password, "数据库密码yiji.ds.password不能为空");
	}

	public String getUrl() {
		if (mysql()) {
			if (!url.contains("?")) {
				return url
					   + "?useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true";
			}
		}
		return url;
	}

	public boolean mysql() {
		return url.toLowerCase().startsWith("jdbc:mysql");
	}

	public ClassLoader getBeanClassLoader() {
		return beanClassLoader;
	}

	public Integer getInitialSize() {
		return initialSize;
	}

	public void setInitialSize(Integer initialSize) {
		this.initialSize = initialSize;
	}

	public Integer getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(Integer maxActive) {
		this.maxActive = maxActive;
	}

	public Integer getMaxResultThreshold() {
		return maxResultThreshold;
	}

	public void setMaxResultThreshold(Integer maxResultThreshold) {
		this.maxResultThreshold = maxResultThreshold;
	}

	public Integer getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(Integer maxWait) {
		this.maxWait = maxWait;
	}

	public Integer getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(Integer minIdle) {
		this.minIdle = minIdle;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Integer getSlowSqlThreshold() {
		return slowSqlThreshold;
	}

	public void setSlowSqlThreshold(Integer slowSqlThreshold) {
		this.slowSqlThreshold = slowSqlThreshold;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public boolean isShowSql() {
		return showSql;
	}

	public void setShowSql(boolean showSql) {
		this.showSql = showSql;
	}

	/**
	 * 通过当前配置创建datasource
	 */
	public com.alibaba.druid.pool.DruidDataSource build() {
		this.check();
		if (this.beanClassLoader == null) {
			this.beanClassLoader = ClassUtils.getDefaultClassLoader();
		}
		com.alibaba.druid.pool.DruidDataSource dataSource = new com.alibaba.druid.pool.DruidDataSource();
		// 基本配置
		dataSource.setDriverClassLoader(this.getBeanClassLoader());
		dataSource.setUrl(this.getUrl());
		dataSource.setUsername(this.getUsername());
		dataSource.setPassword(this.getPassword());
		//应用程序可以自定义的参数
		dataSource.setInitialSize(this.getInitialSize());
		dataSource.setMinIdle(this.getMinIdle());

		if (mysql()) {
			maxActive = Math.max(maxActive, MYSQL_MAX_ACTIVE);
		} else {
			maxActive = Math.max(maxActive, ORACLE_MAX_ACTIVE);
		}
		dataSource.setMaxActive(maxActive);
		dataSource.setMaxWait(this.getMaxWait());
		//dataSource.setConnectionProperties(druidProperties.getConnectionProperties());
		//检测需要关闭的空闲连接间隔，单位是毫秒
		dataSource.setTimeBetweenEvictionRunsMillis(60000);
		//连接在池中最小生存的时间
		dataSource.setMinEvictableIdleTimeMillis(300000);

		dataSource.setTestWhileIdle(true);
		dataSource.setTestOnBorrow(false);
		dataSource.setTestOnReturn(false);

		dataSource.setRemoveAbandoned(true);
		dataSource.setRemoveAbandonedTimeout(1800);
		dataSource.setLogAbandoned(true);
		if (this.mysql()) {
			System.setProperty("druid.mysql.usePingMethod", "true");
			dataSource.setValidConnectionChecker(new MySqlValidConnectionChecker());
		} else {
			System.setProperty("druid.oracle.pingTimeout", "5");
			dataSource.setValidConnectionChecker(new OracleValidConnectionChecker());
		}
		try {
			dataSource.init();
		} catch (SQLException e) {
			throw new WabacusConfigLoadingException("druid连接池初始化失败", e);
		}
		return dataSource;
	}
}
