package com.smile.demo.spark.constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase表配置的常量
 * @author smile
 */
public class HbaseTableConsts {

	/**
	 * 列族-如没特殊需求，则每个表只有一个列族，且命名唯一（information的首字母）
	 */
	public static final String COL_FAMILY = "i";
	public static final byte[] BYTES_COL_FAMILY = Bytes.toBytes(COL_FAMILY);

	/**
	 * Rowkey 拼接分隔符
	 */
	public static final String ROWKEY_SEP = "_";
}
