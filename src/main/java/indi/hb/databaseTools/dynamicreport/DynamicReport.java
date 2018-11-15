package indi.hb.databaseTools.dynamicreport;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
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
	 * 区分必需表和可选表
	 * @param tables
	 */
	public boolean markOff(List<TableBean> tables) {
		boolean result = false;
		if (tables.isEmpty()) {
			return result;
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
		result = true;
		return result;
	}
	/**
	 * 查询数据项从属的表
	 * @param columns
	 * @return
	 */
	public List<TableBean> dependence(String columns) {
		String sql_getDependences = "SELECT table_name, column_name FROM v_tab_col WHERE column_name IN (";
		String[] column = columns.split(SPLIT_STR);
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
