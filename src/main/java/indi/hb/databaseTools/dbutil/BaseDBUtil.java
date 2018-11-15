package indi.hb.databaseTools.dbutil;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库处理工具
 * @author wanghb
 */
public abstract class BaseDBUtil {
	/**
	 * 驱动类名
	 */
	String driveClassName;
	/**
	 * 数据库url
	 */
	String dbURL;
	/**
	 * 数据库用户名
	 */
	String dbusername;
	/**
	 * 数据库密码
	 */
	String dbpassword;
	/**
	 * 参数对象
	 * @author wanghb
	 */
	public static class ArgumentDomain {
	    /**参数名*/
	    private String name;
	    /**参数序号*/
	    private int position;
	    /**参数数据类型*/
	    private String dataType;
	    /**参数jdbc类型*/
	    private int jdbcType;
	    /**参数值*/
	    private Object val;
	    public ArgumentDomain (int position, String dataType, Object val) {
	        this.setPosition(position);
	        this.setDataType(dataType);
	        this.setVal(val);
	    }
	    public ArgumentDomain (int position, int jdbcType, Object val) {
	        this.setPosition(position);
	        this.setJdbcType(jdbcType);
	        this.setVal(val);
	    }
	    public String getName() {
	        return name;
	    }

	    public void setName(String name) {
	        this.name = name;
	    }

	    public int getPosition() {
	        return position;
	    }

	    public void setPosition(int position) {
	        this.position = position;
	    }

	    public String getDataType() {
	        return dataType;
	    }

	    public void setDataType(String dataType) {
	        this.dataType = dataType;
	        setJdbcType(transferSQLType(dataType));
	    }

	    public int getJdbcType() {
	        return jdbcType;
	    }

	    public void setJdbcType(int jdbcType) {
	        this.jdbcType = jdbcType;
	    }

	    public Object getVal() {
	        return val;
	    }

	    public void setVal(Object val) {
	        this.val = val;
	    }
	    /**
	     * 参数类型转换
	     * @param s_dataType
	     * @return
	     */
	    public static int transferSQLType (String s_dataType) {
	        int i_sqlType = 12; //默认VARCHAR类型
	        if ("varchar2".equalsIgnoreCase(s_dataType) || "varchar".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.VARCHAR;
	        } else if ("int".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.INTEGER;
	        } else if ("smallint".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.SMALLINT;
	        } else if ("tinyint".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.TINYINT;
	        } else if ("bigint".equalsIgnoreCase(s_dataType)) {
	        	i_sqlType = Types.BIGINT;
	        } else if ("numeric".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.NUMERIC;
	        } else if ("char".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.CHAR;
	        } else if ("text".equalsIgnoreCase(s_dataType) || "clob".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.CLOB;
	        } else if ("datetime".equalsIgnoreCase(s_dataType) || "date".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = Types.TIMESTAMP;
	        } else if ("ref cursor".equalsIgnoreCase(s_dataType)) {
	            i_sqlType = oracle.jdbc.OracleTypes.CURSOR;    //Oracle游标类型
	        }
	        return i_sqlType;
	    }
	}
	/**
	 * 获取数据库连接，需要
	 * @param dbID
	 * @return
	 */
	public Connection getConn(String dbID) {
		loadProperties(dbID);
		return getConn();
	};
	/**
	 * 加载数据库参数文件
	 * @param dbID 数据库连接标识
	 */
	void loadProperties(String dbID) {
		InputStream is = this.getClass().getResourceAsStream("/db.properties");
		Properties prop = new Properties();
		try {
			prop.load(is);
			setDriveClassName(prop.getProperty(dbID + ".driverClassName"));
			setDbURL(prop.getProperty(dbID + ".url"));
			setDbusername(prop.getProperty(dbID + ".username"));
			setDbpassword(prop.getProperty(dbID + ".password"));
		} catch (IOException e) {
			throw new RuntimeException("[" + this.getClass().getName() + "]加载数据库参数文件异常！" + e.getMessage());
		}
	}
	/**
	 * 获取数据库连接
	 * @return
	 */
	Connection getConn() {
		Connection conn = null;
		try {
			Class.forName(getDriveClassName());
			conn = DriverManager.getConnection(getDbURL(), getDbusername(), getDbpassword());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("[" + this.getClass().getName() + "]找不到驱动类！" + e.getMessage());
		} catch (SQLException e) {
			throw new RuntimeException("[" + this.getClass().getName() + "]数据库连接失败！" + e.getMessage());
		}
		return conn;
	}
	/**
	 * 断开数据库连接
	 * @param conn
	 */
	public void disconn (Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				System.err.println("断开数据库连接异常···");
				//e.printStackTrace();
			}
		}
	}
	/**
	 * 事务开始
	 * @param conn
	 */
	public void transBegin (Connection conn) {
        if (conn == null) return;
        try {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            System.err.println("事务开始异常！");
        }
    }
    /**
     * 事务提交
     * @param conn
     */
    public void transCommit (Connection conn) {
        if (conn == null) return;
        try {
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            System.err.println("事务提交异常！");
        }
    }
    /**
     * 事务回滚
     * @param conn
     */
    public void transRollback (Connection conn) {
        if (conn == null) return;
        try {
            if (conn.getAutoCommit()) {
                conn.rollback();
            }
        } catch (SQLException e) {
            System.err.println("事务回滚异常！");
        }
    }
    /**
     * 查询
     * @param conn 数据库连接
     * @param sql 查询语句
     * @param arguments 参数集合(初始化时需要指定数据类型)
     * @return
     */
    public List<Map<String, Object>> execDQL(Connection conn, String sql, List<ArgumentDomain> arguments) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (sql.toUpperCase().indexOf("INSERT ") > 0 || sql.toUpperCase().indexOf("DELETE ") > 0 || sql.toUpperCase().indexOf("UPDATE ") > 0) return result;    //语句包含DML关键字
        if (sqlNotBegin(conn, sql, arguments)) return result;
        PreparedStatement pre = null;   //初始化预编译对象
        ResultSet rs = null;    //初始化结果集对象
        try {
            pre = conn.prepareStatement(sql); //预编译
            ArgumentDomain argument;
            for (int i = 0; i < arguments.size(); i++) {
                argument = arguments.get(i);    //获取参数对象
                pre.setObject(argument.getPosition(), argument.getVal(), argument.getJdbcType());    //传值
            }
            rs = pre.executeQuery();
            result.addAll(result2List(rs, true));
        } catch (SQLException e) {
            System.err.println("SQL编译异常！");
            e.printStackTrace();
        } finally {
            closeAll(rs, pre);
        }
        return result;
    }

    /**
     * 增、删、改
     * @param conn 数据库连接
     * @param sql 查询语句
     * @param arguments 参数集合(初始化时需要指定数据类型)
     * @return
     */
    public int execDML(Connection conn, String sql, List<ArgumentDomain> arguments) throws SQLException {
        int sqlRowCount = 0;
        if (sqlNotBegin(conn, sql, arguments)) return sqlRowCount;
        PreparedStatement pre = null;   //初始化预编译对象
        ResultSet rs = null;    //初始化结果集对象
        try {
            pre = conn.prepareStatement(sql); //预编译
            ArgumentDomain argument;
            for (int i = 0; i < arguments.size(); i++) {
                argument = arguments.get(i);    //获取参数对象
                pre.setObject(argument.getPosition(), argument.getVal(), argument.getJdbcType());    //传值
            }
            sqlRowCount = pre.executeUpdate();
        } catch (SQLException e) {
            //System.err.println("SQL编译异常:" + e.getMessage());
            throw e;
        } finally {
            closeAll(rs, pre);
        }
        return sqlRowCount;
    }

    /**
     * 批量执行DML语句
     * @param conn
     * @param sql
     * @param arguments
     * @return
     * @throws SQLException
     */
    public int execDMLBatch(Connection conn, String sql, List<List<ArgumentDomain>> arguments) throws SQLException {
        int sqlRowCount = 0;
        PreparedStatement pre = null;   //初始化预编译对象
        ResultSet rs = null;    //初始化结果集对象
        try {
            pre = conn.prepareStatement(sql); //预编译
            pre.clearBatch();
            for (int i = 0; i < arguments.size(); i++) {
                List<ArgumentDomain> list = arguments.get(i);
                if (sqlNotBegin(conn, sql, list)) continue; //参数个数检查
                for (ArgumentDomain argument : list) {
                    pre.setObject(argument.getPosition(), (null == argument.getVal() || "null".equalsIgnoreCase(argument.getVal().toString())) ? "" : argument.getVal(), argument.getJdbcType());    //传值
//                    System.out.println("position:"+argument.getPosition()+"value:"+argument.getVal()+"jdbctype:"+argument.getJdbcType());
                }
                pre.addBatch();
            }
            System.out.println(sql);
            int[] rows = pre.executeBatch();
            sqlRowCount = rows.length;
        } catch (SQLException e) {
            //System.err.println("SQL编译异常:" + e.getMessage());
        	System.err.println(e.getSQLState());
            throw e;
        } catch (Exception e) {
        	//System.err.println("SQL参数装配异常：" + e.getMessage());
            throw e;
        } finally {
            closeAll(rs, pre);
        }
        return sqlRowCount;
    }
    
    /**
     * 执行存储过程
     * 由于视图dba_arguments需要用户具有访问权限，目前只支持调用当前用户的存储过程
     * @param conn 数据库连接
     * @param sql 存储过程名[pkg_name.]prc_name
     * @param arguments 参数集合(初始化时需要指定数据类型)
     * @return
     */
    public Map<String, Object> execPrc(Connection conn, String sql, Map<String, Object> arguments) {
        Map<String, Object> result = new HashMap<>();
        if (conn == null) return result;    //数据库连接无效，退出
        String[] str = sql.split("\\.");    //分割包名与存储过程名
        /*查询存储过程执行参数*/
        List<ArgumentDomain> argList = new ArrayList<>();
        switch (str.length) {
            case 1: //"prc_name"
                argList.add(new ArgumentDomain(1, "varchar", str[0]));
                break;
            case 2: //"pkg_name.prc_name"
                argList.add(new ArgumentDomain(1, "varchar", str[1]));
                argList.add(new ArgumentDomain(2, "varchar", str[0]));
                break;
            default:return result;
        }
        String argSql = "SELECT /*+ RESULT_CACHE */ + lower(argument_name) as argument_name, lower(data_type) as data_type, "
                        + "lower(in_out) as in_out, position FROM user_arguments where object_name= upper(?) "
                        + "and package_name " + ((str.length == 1) ? "is null" : "=upper(?)") + " order by POSITION";
        List<Map<String, Object>> paramList = execDQL(conn, argSql, argList);   //查询过程执行参数
        CallableStatement cstmt = null;
        /*存储过程执行语句初始化*/
        StringBuffer prcSql = new StringBuffer("");
        prcSql.append("{ call ");
        prcSql.append(sql);
        if (paramList.size() > 0) {
            prcSql.append("(");
            for (int i = 0; i < paramList.size(); i++) {
                prcSql.append("?,");
            }
            prcSql.replace(prcSql.length() - 1, prcSql.length(), ")");
        }
        prcSql.append("}");
        try {
            cstmt = conn.prepareCall(prcSql.toString());    //预编译
            String in_out, argument_name, data_type;
            int position;
            List<Map<String, Object>> outList = new ArrayList<>();
            for (Map<String, Object> map : paramList) {
                in_out = map.get("IN_OUT").toString();
                argument_name = map.get("ARGUMENT_NAME").toString();
                position = ((BigDecimal)map.get("POSITION")).intValue();
                data_type = map.get("DATA_TYPE").toString();
                if ("in".equalsIgnoreCase(in_out)) {
                    if (arguments.containsKey(argument_name)) {
                        cstmt.setObject(position, arguments.get(argument_name), ArgumentDomain.transferSQLType(data_type));
                    } else {
                        System.err.println("参数不正确！");
                        return result;
                    }
                } else if ("out".equalsIgnoreCase(in_out)){
                    cstmt.registerOutParameter(position, ArgumentDomain.transferSQLType(data_type));
                    outList.add(map);
                }
            }
            cstmt.execute();    //执行
            /*出参处理*/
            for (Map<String, Object> map : outList) {
                in_out = map.get("IN_OUT").toString();
                argument_name = map.get("ARGUMENT_NAME").toString();
                position = ((BigDecimal)map.get("POSITION")).intValue();
                data_type = map.get("DATA_TYPE").toString();
                if ("out".equalsIgnoreCase(in_out)) {
                    if ("ref cursor".equalsIgnoreCase(data_type)) {
                        result.put(argument_name, result2List((ResultSet) cstmt.getObject(position), true));
                    } else {
                        result.put(argument_name, cstmt.getObject(position));
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("SQL编译异常:" + e.getMessage());
        } finally {
            if (cstmt != null)
                try {
                    cstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return result;
    }

    /**
     * 将set集合转换为list
     * 对异常进行了处理，并关闭了结果集
     * @param rs 结果集
     * @return
     */
    public List<Map<String, Object>> result2List(ResultSet rs, boolean key2lower) {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData rsmd = null;
        Map<String, Object> map = null;
        try {
            while (rs.next()) {
                rsmd = rs.getMetaData();
                map = new LinkedHashMap<>();	//确保各列顺序
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(key2lower ? rsmd.getColumnLabel(i).toLowerCase() : rsmd.getColumnLabel(i), (rs.getObject(i) == null || "null".equals(rs.getObject(i))) ? "" : rs.getObject(i)); //将key转为小写
                }
                list.add(map);
            }
        } catch (SQLException e) {
            System.err.println("读取结果集异常！");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    System.err.println("关闭结果集异常！");
                }
            }
        }
        return list;
    }

    /**
     * 关闭对象
     * @param rs
     * @param pre
     */
    public void closeAll (ResultSet rs, PreparedStatement pre) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
        	if (pre != null)
        		pre.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断是否可以开始SQL操作
     * @param conn 数据库连接
     * @param sql 查询语句
     * @param arguments 参数集合(初始化时需要指定数据类型)
     * @return
     */
    public boolean sqlNotBegin(Connection conn, String sql, List<ArgumentDomain> arguments) {
        int index = sql.length() - sql.replace("?", "").length();
        return conn == null || index != arguments.size();   //数据库连接无效或参数个数不匹配
    }
    
	public String getDriveClassName() {
		return driveClassName;
	}
	public void setDriveClassName(String driveClassName) {
		if (driveClassName == null || driveClassName.isEmpty()) throw new RuntimeException("[" + this.getClass().getName() + "]驱动类名不能为空！");
		this.driveClassName = driveClassName;
	}
	public String getDbURL() {
		return dbURL;
	}
	public void setDbURL(String dbURL) {
		if (dbURL == null || dbURL.isEmpty()) throw new RuntimeException("[" + this.getClass().getName() + "]数据库连接url不能为空！");
		this.dbURL = dbURL;
	}
	public String getDbusername() {
		return dbusername;
	}
	public void setDbusername(String dbusername) {
		if (dbusername == null || dbusername.isEmpty()) throw new RuntimeException("[" + this.getClass().getName() + "]数据库用户名不能为空！");
		this.dbusername = dbusername;
	}
	public String getDbpassword() {
		return dbpassword;
	}
	public void setDbpassword(String dbpassword) {
		if (dbpassword == null || dbpassword.isEmpty()) throw new RuntimeException("[" + this.getClass().getName() + "]数据库密码不能为空！");
		this.dbpassword = dbpassword;
	}
}
