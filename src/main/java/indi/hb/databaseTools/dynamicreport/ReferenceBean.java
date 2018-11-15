package indi.hb.databaseTools.dynamicreport;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * 数据表依赖关系
 * @author wanghb
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper=false)
public class ReferenceBean extends TableBean implements Comparable<ReferenceBean> {
	/**
	 * 依赖表名
	 */
	private String rTable;
	/**
	 * 依赖表的字段名
	 */
	private String rColumn;
	@Override
	public int compareTo(ReferenceBean o) {
		int sort = 0;
		if (o.getTable().compareTo(this.getRTable()) == 0) {
			sort = 1;
		} else if (o.getRTable().compareTo(this.getTable()) == 0) {
			sort = -1;
		} else {
			sort = o.getTable().compareTo(this.getTable());
		}
		return sort;
	}
	
}
