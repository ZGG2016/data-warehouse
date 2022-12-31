模拟可视化数据

会员主题
建表语句
----------------------------------------------------------
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
----------------------------------------------------------

导入数据
----------------------------------------------------------
INSERT INTO `ads_user_topic` VALUES ('2020-03-12', 761, 52, 0, 8, 989, 0.77, 0.01, 0.07);
INSERT INTO `ads_user_topic` VALUES ('2020-03-13', 840, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-14', 900, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-15', 890, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-16', 607, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-17', 812, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-18', 640, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-19', 740, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-20', 540, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-21', 940, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-22', 840, 69, 10, 5, 1000, 0.59, 0.01, 0.05);
INSERT INTO `ads_user_topic` VALUES ('2020-03-23', 1000, 32, 32, 32, 23432, 22.00, 0.11, 0.55);
----------------------------------------------------------


地区主题
建表语句
----------------------------------------------------------
DROP TABLE IF EXISTS `ads_area_topic`;
CREATE TABLE `ads_area_topic`  (
  `dt` date NOT NULL,
  `iso_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `province_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `area_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `order_count` bigint(255) NULL DEFAULT NULL,
  `order_amount` double(255, 2) NULL DEFAULT NULL,
  `payment_count` bigint(255) NULL DEFAULT NULL,
  `payment_amount` double(255, 2) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`, `iso_code`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
----------------------------------------------------------

导入数据
----------------------------------------------------------
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-11', '北京', '华北', 652, 652.00, 652, 652.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-12', '天津', '华北', 42, 42.00, 42, 42.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-13', '河北', '华北', 3435, 3435.00, 3435, 3435.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-14', '山西', '华北', 4, 4.00, 4, 4.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-15', '内蒙古', '华北', 44, 44.00, 44, 44.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-21', '辽宁', '东北', 335, 335.00, 335, 335.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-22', '吉林', '东北', 44, 44.00, 44, 44.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-23', '黑龙江', '东北', 337, 337.00, 337, 337.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-31', '上海', '华东', 4, 4.00, 4, 4.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-32', '江苏', '华东', 4545, 4545.00, 4545, 4545.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-33', '浙江', '华东', 43, 43.00, 43, 43.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-34', '安徽', '华东', 12345, 2134.00, 324, 252.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-35', '福建', '华东', 435, 435.00, 435, 435.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-36', '江西', '华东', 4453, 4453.00, 4453, 4453.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-37', '山东', '华东', 34, 34.00, 34, 34.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-41', '河南', '华中', 34, 34.00, 34, 34.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-42', '湖北', '华中', 4, 4.00, 4, 4.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-43', '湖南', '华中', 54, 54.00, 54, 54.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-44', '广东', '华南', 24, 24.00, 24, 24.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-45', '广西', '华南', 4, 4.00, 4, 4.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-46', '海南', '华南', 42, 42.00, 42, 42.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-50', '重庆', '西南', 4532, 4532.00, 4532, 4532.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-51', '四川', '西南', 3435, 3435.00, 3435, 3435.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-52', '贵州', '西南', 5725, 5725.00, 5725, 5725.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-53', '云南', '西南', 4357, 4357.00, 4357, 4357.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-54', '西藏', '西南', 54, 54.00, 54, 54.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-61', '陕西', '西北', 44, 44.00, 44, 44.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-62', '甘肃', '西北', 78, 78.00, 78, 78.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-63', '青海', '西北', 3444, 3444.00, 3444, 3444.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-64', '宁夏', '西北', 445, 445.00, 445, 445.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-65', '新疆', '西北', 4442, 4442.00, 4442, 4442.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-71', '台湾', '华东', 343, 343.00, 343, 343.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-91', '香港', '华南', 44, 44.00, 44, 44.00);
INSERT INTO `ads_area_topic` VALUES ('2020-03-10', 'CN-92', '澳门', '华南', 34, 34.00, 34, 34.00);
----------------------------------------------------------
