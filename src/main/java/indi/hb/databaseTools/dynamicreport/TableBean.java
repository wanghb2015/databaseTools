package indi.hb.databaseTools.dynamicreport;

import java.util.Comparator;

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
public class TableBean implements Comparable<TableBean>, Comparator<TableBean[]> {
	/**
	 * 表名
	 */
	private String table;
	/**
	 * 字段名
	 */
	private String column;
	@Override
	public int compareTo(TableBean o) {
		int diff = this.getTable().compareToIgnoreCase(o.getTable());
		if (diff == 0) {
			diff = this.getColumn().compareTo(o.getColumn());
		}
		return diff;
	}
	public int compareTo(ReferenceBean o) {
		return 0;
	}
	@Override
	public int compare(TableBean[] o1, TableBean[] o2) {
		int diff = o1.length - o2.length;
		if (diff == 0) {
			for (int i = 0, len = o1.length; i < len; i++) {
				do {
					diff = o1[i].compareTo(o2[i]);
				} while (diff == 0);
			}
		}
		return diff;
	}
}
