package indi.hb.databaseTools.test;

import java.util.List;

import indi.hb.databaseTools.dynamicreport.DynamicReport;
import indi.hb.databaseTools.dynamicreport.TableBean;

public class DynamicReportDemo {
	public static void main(String[] args) {
		DynamicReport dr = new DynamicReport();
		
		List<TableBean> list = dr.dependence("BIRTH,COMPANYABBR,COMPANYTYPE,DISPOSEDEPT,DISPOSENAME,ESTABLISHNO,GOABROADSTATE,PREPAREDNUM,STARTWORKDATE");
		for (TableBean tb : list) {
			System.out.println(tb.getTable() + "<--->" + tb.getColumn());
		}
	}
}
