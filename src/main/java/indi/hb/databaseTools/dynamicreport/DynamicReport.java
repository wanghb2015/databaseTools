package indi.hb.databaseTools.dynamicreport;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import indi.hb.databaseTools.dbutil.BaseDBUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
/**
 * 根据数据库自身的外键依赖关系，主动判定选定的数据项的来源表，并生成DQL语句
 * @author wanghb
 * 2018/11/15
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DynamicReport extends BaseDBUtil {
	/**
	 * 分解因子
	 */
	private final String SPLIT_STR = ",";
	/**
	 * 必需表：选中的数据项唯一锁定的表
	 */
	private TreeSet<String> essential = new TreeSet<>();
	/**
	 * 可选表：选中的数据项对应的多选的表
	 */
	private TreeSet<String> selectable = new TreeSet<>();
	/**
	 * 选中的字段，使用分解因子分割，区分大小写
	 */
	private String columns;
	/**
	 * 构造所有可能的组合
	 * @param tables 所有选中的字段和对应表
	 * @param col_n 可多选的字段
	 * @param col_1  唯一的字段
	 * @return
	 */
	public TableBean[][] makeUp(List<TableBean> tables, TreeSet<String> col_n, TreeSet<String> col_1) {
		// 多选字段对应的出现次数
		Map<String, Integer> colNum = new LinkedHashMap<>();
		for (String col : col_n) {
			colNum.put(col, 0);
			for (TableBean tab : tables) {
				if (col.equalsIgnoreCase(tab.getColumn())) {
					colNum.put(col, colNum.get(col).intValue() + 1);
				}
			}
		}
		// 所有可能的表.字段组合数
		int all = 1;
		for (Entry<String, Integer> entry : colNum.entrySet()) {
			all *= entry.getValue().intValue();
		}
		// 所有可能的表.字段组合矩阵
		TableBean[][] allPossible = new TableBean[all][this.colCnt(getColumns())];
		int serial_y, serial_x = 0;
		// 填充多选表.字段
		for (String col : col_n) {
			// 行号初始化
			serial_y = 0;
			while (serial_y < all) {
				for (TableBean tab : tables) {
					if (col.equalsIgnoreCase(tab.getColumn())) {
						// 同列循环填充
						allPossible[serial_y++][serial_x] = tab;
					}
				}
			}
			serial_x += 1;
		}
		// 填充唯一表.字段
		for (String col : col_1) {
			for (TableBean tab : tables) {
				if (col.equalsIgnoreCase(tab.getColumn())) {
					for (serial_y = 0; serial_y < all; serial_y++) {
						allPossible[serial_y][serial_x] = tab;
					}
				}
			}
			serial_x += 1;
		}
		for (int i = 0, l = allPossible.length; i < l; i++) {
			System.out.println("----------组合" + (i+1));
			for (TableBean tab: allPossible[i]) {
				System.out.println(tab.getTable() + "<--->" + tab.getColumn());
			}
		}
		return allPossible;
	}
	/**
	 * 区分必需表和可选表，并做出分组
	 * @param tables
	 * @return
	 */
	public TableBean[][] markOff(List<TableBean> tables) {
		if (tables.isEmpty()) {
			return null;
		}
		Map<String, Integer> cols = new HashMap<>();
		// 记录列名出现的次数
		for (TableBean tab : tables) {
			cols.put(tab.getColumn(), (cols.get(tab.getColumn()) == null) ? 1 : cols.get(tab.getColumn()) + 1);
		}
		// 出现过多次和一次的字段
		TreeSet<String> col_n = new TreeSet<>(), col_1 = new TreeSet<>();
		for (Entry<String, Integer> entrySet : cols.entrySet()) {
			// 区分出现过多次的字段项
			if (entrySet.getValue() > 1) {
				col_n.add(entrySet.getKey());
			} else {
				col_1.add(entrySet.getKey());
			}
		}
		// 遍历所有选中表
		for (TableBean tab : tables) {
			if (col_1.contains(tab.getColumn())) {
				// 如果字段只出现过1次，记入必需表
				essential.add(tab.getTable());
				// 如果已记入可选表，从可选表中移除
				if (selectable.contains(tab.getTable())) {
					selectable.remove(tab.getTable());
				}
			} else if (col_n.contains(tab.getColumn()) && !essential.contains(tab.getTable())) {
				// 如果字段出现多次，且不在必需表，才记入可选表
				selectable.add(tab.getTable());
			}
		}
		// 分组
		return makeUp(tables, col_n, col_1);
	}
	/**
	 * 查询数据项从属的表
	 * @param columns
	 * @return
	 */
	public List<TableBean> dependence() {
		String sql_getDependences = "SELECT table_name, column_name FROM v_tab_col WHERE column_name IN (";
		String[] column = getColumns().split(SPLIT_STR);
		StringBuilder placeholder = new StringBuilder();
		List<ArgumentDomain> args = new ArrayList<>();
		for (int i = 0; i < column.length; i++) {
			placeholder.append(",?");
			args.add(new ArgumentDomain(i + 1, "varchar2", column[i].trim()));
		}
		List<Map<String, String>> dependences = query(sql_getDependences + placeholder.substring(1) + ")", args);
		List<TableBean> tables = new ArrayList<>();
		for (Map<String, String> map : dependences) {
			tables.add(new TableBean(map.get("table_name"), map.get("column_name")));
		}
		return tables;
	}
	/**
	 * 表名集合
	 * @param list
	 * @return
	 */
	public TreeSet<String> tables(List<TableBean> list) {
		TreeSet<String> tables = new TreeSet<>();
		for (TableBean table : list) {
			tables.add(table.getTable());
		}
		return tables;
	}
	public TreeSet<ReferenceBean> references(String columns) {
		
		return null;
	}
	
	public ReferenceBean reference(String table) {
		return null;
	}
	/**
	 * 字段数量
	 * @param columns
	 * @return
	 */
	public int colCnt(String columns) {
		String filtSplit = columns.replace(SPLIT_STR, "");
		return columns.length() - filtSplit.length() + 1;
	}
	/**
	 * 执行查询
	 * @param sql
	 * @param args
	 * @return
	 */
	public List<Map<String, String>> query(String sql, List<ArgumentDomain> args) {
		Connection conn = getConn("dynRepo");
		List<Map<String, Object>> list = execDQL(conn, sql, args);
		disconn(conn);
		List<Map<String, String>> result = new ArrayList<>();
		Map<String, String> map;
		for (Map<String, Object> m : list) {
			map = new HashMap<>();
			for (Entry<String, Object> entrySet : m.entrySet()) {
				map.put(entrySet.getKey(), obj2str(entrySet.getValue()));
			}
			result.add(map);
		}
		return result;
	}
	/**
	 * 字段类型转换
	 * @param obj
	 * @return
	 */
	String obj2str (Object obj) {
		return (obj == null) ? "" : obj.toString();
	}
}
