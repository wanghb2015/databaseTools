package indi.hb.databaseTools.dynamicreport;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 表
 * @author wanghb
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableBean {
	/**
	 * 表名
	 */
	private String table;
	/**
	 * 字段名
	 */
	private String column;
}
