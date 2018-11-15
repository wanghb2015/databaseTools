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
		if (dr.markOff(list)) {
			System.out.println("----必需表----");
			for (String str : dr.getEssential()) {
				System.out.println(str);
			}
			System.out.println("----可选表----");
			for (String str : dr.getSelectable()) {
				System.out.println(str);
			}
		}
	}
}
