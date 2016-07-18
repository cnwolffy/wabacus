/* 
 * Copyright (C) 2010---2014 星星(wuweixing)<349446658@qq.com>
 * 
 * This file is part of Wabacus 
 * 
 * Wabacus is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.wabacus.config.database.datasource;

import com.wabacus.exception.WabacusConfigLoadingException;
import com.wabacus.exception.WabacusRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Element;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DruidDataSource extends AbsDataSource {
	private static Log log = LogFactory.getLog(DruidDataSource.class);

	private com.alibaba.druid.pool.DruidDataSource ds;

	public Connection getConnection() {
		try {
			log.debug("从数据源" + this.getName() + "获取数据库连接");
			return ds.getConnection();
		} catch (SQLException e) {
			throw new WabacusRuntimeException("获取" + this.getName() + "数据源的数据库连接失败", e);
		}
	}

	public DataSource getDataSource() {
		return this.ds;
	}

	public void closePool() {
		super.closePool();
		if (this.ds != null) {
			log.debug("正在关闭Druid连接池....................................................");
			this.ds.close();
		}
		this.ds = null;
	}

	public void loadConfig(Element eleDataSource) {
		super.loadConfig(eleDataSource);
		List lstEleProperties = eleDataSource.elements("property");
		if (lstEleProperties == null || lstEleProperties.size() == 0) {
			throw new WabacusConfigLoadingException("没有为数据源：" + this.getName()
													+ "配置alias、configfile等参数");
		}

		DruidProperties druidProperties = new DruidProperties();

		Element eleChild;
		String name;
		String value;
		for (int i = 0; i < lstEleProperties.size(); i++) {
			eleChild = (Element) lstEleProperties.get(i);
			name = eleChild.attributeValue("name");
			value = eleChild.getText();
			name = name == null ? "" : name.trim();
			value = value == null ? "" : value.trim();
			if (value.equals("")) {
				continue;
			}
			if (name.equals("max_size")) {
				//最大连接数
				druidProperties.setMaxActive(Integer.parseInt(value));
			} else if (name.equals("min_size")) {
				//配置初始化大小,最小连接数
				druidProperties.setInitialSize(Integer.parseInt(value));
				druidProperties.setMinIdle(Integer.parseInt(value));
			} else if (name.equals("timeout")) {
				//配置获取连接等待超时的时间
				druidProperties.setMaxWait(Integer.parseInt(value));
			} else if (name.equals("url")) {
				druidProperties.setUrl(value);
			} else if (name.equals("user")) {
				druidProperties.setUsername(value);
			} else if (name.equals("password")) {
				druidProperties.setPassword(decryptPassword(value));
			}
		}
		druidProperties.check();

		this.ds = druidProperties.build();
	}

	protected void finalize() throws Throwable {
		closePool();
		super.finalize();
	}
}
