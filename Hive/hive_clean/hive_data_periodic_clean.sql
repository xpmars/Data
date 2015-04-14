use dw;
DROP TABLE IF EXISTS `hive_data_periodic_clean`;
CREATE TABLE `hive_data_periodic_clean` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `database` char(64) NOT NULL COMMENT '数据库名称',
  `tablename` char(64) NOT NULL COMMENT '表名称',
  `partition_field_name` char(128) NOT NULL COMMENT '分区字段名称',
  `dateformat` char(64) NOT NULL DEFAULT '%Y-%m-%d' COMMENT '日期格式,默认为2014-12-09',
  `location_path` varchar(255) NOT NULL COMMENT '内外部表的存储路径',
  `periodic` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '数据保存周期，以天为单位',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

INSERT INTO hive_data_periodic_clean (`database`,tablename,partition_field_name,location_path,periodic)
VALUES ('pc','pc_extends_log','date','/user/hive/externaldb/pc_extends_log',7);