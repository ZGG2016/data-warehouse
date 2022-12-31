MySQL中的数据结果表

建表语句
----------------------------------------------------
DROP TABLE IF EXISTS `ads_user_topic`;
CREATE TABLE `ads_user_topic`  (
  `dt` date NOT NULL,
  `day_users` bigint(255) NULL DEFAULT NULL,
  `day_new_users` bigint(255) NULL DEFAULT NULL,
  `day_new_payment_users` bigint(255) NULL DEFAULT NULL,
  `payment_users` bigint(255) NULL DEFAULT NULL,
  `users` bigint(255) NULL DEFAULT NULL,
  `day_users2users` double(255, 2) NULL DEFAULT NULL,
  `payment_users2users` double(255, 2) NULL DEFAULT NULL,
  `day_new_users2users` double(255, 2) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
------------------------------------------------------