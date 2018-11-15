package indi.hb.databaseTools.auditsi;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import indi.hb.databaseTools.dbutil.BaseDBUtil;

/**
 * 社保审计数据迁移 支持全表导入，方向：Oracle-Microsoft SQL Server
 * 
 * @author wanghb
 */
public class TycsiAudit extends BaseDBUtil {

	private Connection oraConn;
	private Connection mssqlConn;
	private String sourceDB;
	private String targetDB;
	private Long PER_ROWS;

	public void init() {
		InputStream is = this.getClass().getResourceAsStream("/tab.properties");
		Properties prop = new Properties();
		String[] tableNames;
		try {
			prop.load(is);
			String tableNameStr = prop.getProperty(getSourceDB() + ".table_name");
			tableNames = tableNameStr.split(",");
		} catch (IOException e) {
			throw new RuntimeException("[" + this.getClass().getName() + "]加载数据库参数文件异常！" + e.getMessage());
		}
		for (String tableName : tableNames) {
			transfer(tableName.trim());
		}
	}

	/**
	 * 数据迁移
	 * 
	 * @param table_name
	 */
	public void transfer(String table_name) {
		oraConn = getConn(this.getSourceDB());
		mssqlConn = getConn(this.getTargetDB());
		if (isExistsTab(table_name)) {
			Long rowCount = rowCount(mssqlConn, table_name);
			if (rowCount > 0) {
				System.out.println("请核对源库和目标库中表[" + table_name + "]的数据，暂不迁移！");
				disconn(oraConn);
				disconn(mssqlConn);
				return;
			}
		} else {
			List<Map<String, Object>> tab_cols = getOraTab(table_name);
			createMSSQLTab(table_name, transformData(tab_cols));
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date begin = new Date();
		System.out.println("[" + sdf.format(begin) + "]开始迁移表[" + table_name + "]");
		Long sourceRows = rowCount(oraConn, table_name);
		List<Map<String, Object>> datas = new ArrayList<>();
		int i = 0;
		while (i < Math.ceil((double) sourceRows / this.getPER_ROWS())) {
			System.out.println("-------------批次：" + (i + 1) + "-------------");
			datas = queryOra(table_name, i * this.getPER_ROWS(), (i + 1) * this.getPER_ROWS());
			transBegin(mssqlConn);
			if (datas.size() > 0 && insertMSSQL(table_name, datas)) {
				transCommit(mssqlConn);
			} else {
				transRollback(mssqlConn);
			}
			;
			i++;
		}
		disconn(oraConn);
		disconn(mssqlConn);
		Date end = new Date();
		System.out.println("[" + sdf.format(begin) + "]结束迁移表[" + table_name + "]");
		System.out.println("迁移[" + table_name + "]共分[" + i + "]总耗时：" + consum(begin, end));
	}

	/**
	 * 判断表是否在SQL Server中已存在
	 * 
	 * @param table_name
	 * @return
	 */
	public boolean isExistsTab(String table_name) {
		// String sql = "select COUNT(*) tab_cnt from sysobjects where name = ? and type
		// = 'U'"; //不可行
		boolean exists = false;
		try {
			DatabaseMetaData meta = mssqlConn.getMetaData();
			ResultSet rs = meta.getTables(mssqlConn.getCatalog(), "%", table_name, new String[] { "TABLE" });
			while (rs.next()) {
				exists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("SQL Server目标库中存在表[" + table_name + "]" + exists);
		return exists;
	}

	/**
	 * 查询数据行数
	 * 
	 * @param conn
	 * @param table_name
	 * @return
	 */
	public Long rowCount(Connection conn, String table_name) {
		Long result = 0L;
		String sql = "select count(*) row_cnt from " + table_name;
		List<Map<String, Object>> list = execDQL(conn, sql, new ArrayList<>());
		result = Long.parseLong(list.get(0).get("row_cnt").toString());
		System.out.println("表[" + table_name + "]数据行数为：" + result);
		return result;
	}

	/**
	 * 获取Oracle中的表结构
	 * 
	 * @param table_name
	 * @return
	 */
	public List<Map<String, Object>> getOraTab(String table_name) {
		List<Map<String, Object>> result = new ArrayList<>();
		String sql = "select column_name," + "       decode(data_type," + "              'VARCHAR2',"
				+ "              'VARCHAR(' || data_length || ')',"
				+ "'NUMBER', 'NUMERIC(' || DATA_PRECISION || ',' || DATA_SCALE || ')'," + "'DATE', 'DATETIME',"
				+ "              data_type) data_type" + "  from user_tab_cols" + " where table_name = upper(?)"
				+ " order by column_id";
		System.out.println(sql);
		result = execDQL(oraConn, sql, setArg(new String[] { table_name }));
		return result;
	}

	/**
	 * 创建Microsoft SQLServer 库表
	 * 
	 * @param table_name
	 * @param list
	 * @return
	 */
	public boolean createMSSQLTab(String table_name, List<Map<String, String>> list) {
		boolean result = false;
		StringBuilder sql = new StringBuilder("create table " + table_name + "(");
		for (Map<String, String> map : list) {
			sql.append(map.get("column_name") + " " + map.get("data_type") + ",");
		}
		sql.replace(sql.length() - 1, sql.length(), ")");
		try {
			System.out.println("在目标数据库中建表[" + table_name + "]\n" + sql.toString());
			execDML(mssqlConn, sql.toString(), new ArrayList<>());
			result = isExistsTab(table_name);
		} catch (SQLException e) {
			if (("数据库中已存在名为 '" + table_name + "' 的对象。").equals(e.getMessage())) {
				result = true;
			}
		}
		return result;
	}

	/**
	 * 收集Oracle中数据
	 * 
	 * @param table_name
	 * @return
	 */
	public List<Map<String, Object>> queryOra(String table_name, Long begin, Long end) {
		List<Map<String, Object>> datas = new ArrayList<>(500000);
		List<String> cols = getMSSQLCols(table_name);
		String colStr = cols.toString();
		// System.out.println(colStr.substring(1, colStr.length() - 1));
		String sql = "select " + colStr.substring(1, colStr.length() - 1) + " from (select a.*, rownum rownum_ from "
				+ table_name + " a where rownum <= ?)" + " where rownum_ > ?";
		// System.out.println(sql);
		datas = execDQL(oraConn, sql, setArg(new String[] { end.toString(), begin.toString() }));
		System.out.println("源数据库中表[" + table_name + "]收集到的数据" + datas.size() + "条。");
		return datas;
	}

	/**
	 * 数据写入SQL Server
	 * 
	 * @param datas
	 * @return
	 */
	public boolean insertMSSQL(String table_name, List<Map<String, Object>> datas) {
		boolean result = false;
		StringBuilder insertSql = new StringBuilder("insert into " + table_name + "(");
		List<String> columns = getMSSQLCols(table_name);
		for (String col : columns) {
			insertSql.append(col + ",");
		}
		insertSql.replace(insertSql.length() - 1, insertSql.length(), ") values (" + placeholder(columns.size()) + ")");
		// System.out.println(insertSql.toString());
		try {
			int rows = execDMLBatch(mssqlConn, insertSql.toString(), setArg(datas));
			System.out.println("目标数据库中表[" + table_name + "]插入数据" + rows + "条。");
			result = rows == datas.size();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 获取SQL Server表字段
	 * 
	 * @param table_name
	 * @return
	 */
	List<String> getMSSQLCols(String table_name) {
		// String sql = "select a.name column_name from syscolumns a, sysobjects b where
		// a.id = b.id and b.name = ? and b.type = 'U' order by a.colid"; //不可行
		List<String> result = new ArrayList<>();
		try {
			DatabaseMetaData meta = mssqlConn.getMetaData();
			ResultSet rs = meta.getColumns(mssqlConn.getCatalog(), "%", table_name, "%");
			while (rs.next()) {
				result.add(rs.getString("COLUMN_NAME"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * 数据类型转换为String
	 * 
	 * @param map
	 * @return
	 */
	Map<String, String> transformData(Map<String, Object> map) {
		Map<String, String> result = new LinkedHashMap<>();
		for (Entry<String, Object> entry : map.entrySet()) {
			Object val = entry.getValue();
			result.put(entry.getKey(), (val == null) ? "" : val.toString());
		}
		return result;
	}

	/**
	 * 数据类型转换为String
	 * 
	 * @param list
	 * @return
	 */
	List<Map<String, String>> transformData(List<Map<String, Object>> list) {
		List<Map<String, String>> result = new ArrayList<>();
		for (Map<String, Object> map : list) {
			result.add(transformData(map));
		}
		return result;
	}

	/**
	 * 生成占位符
	 * 
	 * @param len
	 * @return
	 */
	String placeholder(int len) {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < len; i++) {
			result.append("?,");
		}
		result.replace(result.length() - 1, result.length(), "");
		return result.toString();
	}

	/**
	 * 设置表名参数
	 * 
	 * @param table_name
	 * @return
	 */
	List<ArgumentDomain> setArg(String[] args) {
		List<ArgumentDomain> list = new ArrayList<>();
		int i = 1;
		for (String arg : args) {
			list.add(new ArgumentDomain(i++, "varchar", arg));
		}
		return list;
	}

	/**
	 * 参数转换
	 * 
	 * @param map
	 * @return
	 */
	List<ArgumentDomain> setArg(Map<String, Object> map) {
		List<ArgumentDomain> result = new ArrayList<>();
		int i = 1;
		for (Entry<String, Object> entry : map.entrySet()) {
			result.add(new ArgumentDomain(i++, getJdbcType(entry.getValue().getClass().getSimpleName()), entry.getValue()));
		}
		return result;
	}

	/**
	 * 参数转换
	 * 
	 * @param list
	 * @return
	 */
	List<List<ArgumentDomain>> setArg(List<Map<String, Object>> list) {
		List<List<ArgumentDomain>> result = new ArrayList<>();
		for (Map<String, Object> map : list) {
			result.add(setArg(map));
		}
		return result;
	}
	/**
	 * Java数据类型转换为jdbc数据类型
	 * @param javaType
	 * @return
	 */
	int getJdbcType(String javaType) {
		int jdbcType = Types.VARCHAR;
		if ("String".equalsIgnoreCase(javaType)) {
			jdbcType = Types.VARCHAR;
		} else if ("BigDecimal".equals(javaType)) {
			jdbcType = Types.NUMERIC;
		} else if ("Timestamp".equals(javaType)) {
			jdbcType = Types.TIMESTAMP;
		}
		return jdbcType;
	}
	/**
	 * 计算耗时
	 * 
	 * @param begin
	 * @param end
	 * @return
	 */
	String consum(Date begin, Date end) {
		String result;
		Long consum = end.getTime() - begin.getTime();
		int day = (int) (consum / (1000 * 60 * 60 * 24));
		int dayMod = (int) (consum % (1000 * 60 * 60 * 24));
		int hour = (int) (dayMod / (1000 * 60 * 60));
		int hourMod = (int) (dayMod % (1000 * 60 * 60));
		int minute = (int) (hourMod / (1000 * 60));
		int minuteMod = (int) (hourMod % (1000 * 60));
		int second = (int) (minuteMod / (1000));
		int msec = (int) (minuteMod % (1000));
		result = String.format("%d天%02d小时%02d分%02d秒%03d毫秒", day, hour, minute, second, msec);
		return result;
	}

	public String getSourceDB() {
		sourceDB = (sourceDB == null || sourceDB.isEmpty()) ? "szsj" : sourceDB;
		return sourceDB;
	}

	public void setSourceDB(String sourceDB) {
		this.sourceDB = sourceDB;
	}

	public String getTargetDB() {
		targetDB = (targetDB == null || targetDB.isEmpty()) ? "mssqlserver" : targetDB;
		return targetDB;
	}

	public void setTargetDB(String targetDB) {
		this.targetDB = targetDB;
	}

	public Long getPER_ROWS() {
		PER_ROWS = (PER_ROWS == null || PER_ROWS == 0L) ? 500000L : PER_ROWS;
		return PER_ROWS;
	}

	public void setPER_ROWS(Long pER_ROWS) {
		PER_ROWS = pER_ROWS;
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			args = new String[] {"ybsj", "", "500000"};
		}
		TycsiAudit ta = new TycsiAudit();
		if (args.length > 0) {
			ta.setSourceDB(args[0]);
			if (args.length > 1) {
				ta.setTargetDB(args[1]);
				if (args.length > 2) {
					ta.setPER_ROWS(Long.parseLong(args[2]));
				}
			}
		}
		ta.init();
	}

}
