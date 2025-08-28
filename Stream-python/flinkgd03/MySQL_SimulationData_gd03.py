# -*- coding: utf-8 -*-
import pymysql
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import time
import re
from collections import defaultdict
import json
from functools import wraps

# ==================== 动态配置字段（ODS层日志场景优化）====================
MYSQL_DB = "FlinkGd03"  # 目标数据库名
TOTAL_RECORDS = 8000    # 总记录数
BATCH_SIZE = 1500       # 每批次记录数
DAY_TIME_RANGE = 7      # 生成天数范围
FIXED_SHOP_ID = 1001    # 固定店铺ID

# ==================== ODS层建表语句（源自GD03文档）====================
CREATE_TABLE_SQL_LIST = [
    # 1. ODS - 用户访问日志表（ods_user_visit_log）
    """
    CREATE TABLE `ods_user_visit_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID（埋点生成）',
      `visit_time` DATETIME NOT NULL COMMENT '访问时间（精确到秒，原始时间戳转换）',
      `visitor_id` VARCHAR(64) NOT NULL COMMENT '访客唯一标识（如/设备，去重依据）',
      `terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型：端，-无线端（原始埋点值）',
      `visit_object_type` VARCHAR(20) NOT NULL COMMENT '访问对象类型：-店铺页，-商品详情页，-自定义页面',
      `visit_object_id` BIGINT NOT NULL COMMENT '访问对象ID：店铺ID（=）/商品（）/页面（）',
      `traffic_source_first` VARCHAR(50) DEFAULT NULL COMMENT '一级流量来源（如“搜索”“直播”，原始埋点）',
      `traffic_source_second` VARCHAR(50) DEFAULT NULL COMMENT '二级流量来源（如“搜索-关键词”“直播-官方直播间”，原始埋点）',
      `is_new_visitor` TINYINT NOT NULL DEFAULT 0 COMMENT '是否新访客：1-新访客，0-老访客（原始埋点判断）',
      `page_view` INT NOT NULL DEFAULT 1 COMMENT '单次访问的浏览量（PV，默认1次访问记1PV，多次刷新累加）',
      `stay_time` INT DEFAULT 0 COMMENT '停留时间（秒，原始埋点计算：离开时间-进入时间）',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步到ODS层的时间',
      PRIMARY KEY (`log_id`) COMMENT '原始日志唯一主键',
      INDEX `idx_visit_time` (`visit_time`) COMMENT '按访问时间查询优化',
      INDEX `idx_visitor_object` (`visitor_id`, `visit_object_type`, `visit_object_id`) COMMENT '按访客+访问对象查询优化'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-用户访问日志表（原始埋点数据）';
    """,
    # 2. ODS - 商品交互日志表（ods_product_interaction_log）
    """
    CREATE TABLE `ods_product_interaction_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID',
      `interact_time` DATETIME NOT NULL COMMENT '交互时间（精确到秒）',
      `visitor_id` VARCHAR(64) NOT NULL COMMENT '访客唯一标识',
      `terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型：-无线端',
      `product_id` BIGINT NOT NULL COMMENT '商品ID',
      `interact_type` VARCHAR(20) NOT NULL COMMENT '交互类型：-加购，-收藏',
      `is_cancel` TINYINT NOT NULL DEFAULT 0 COMMENT '是否取消：1-取消加购/收藏，0-新增（避免重复统计）',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`log_id`),
      INDEX `idx_interact_time` (`interact_time`),
      INDEX `idx_visitor_product` (`visitor_id`, `product_id`, `interact_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-商品加购收藏交互日志表';
    """,
    # 3. ODS - 订单支付日志表（ods_order_pay_log）
    """
    CREATE TABLE `ods_order_pay_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID',
      `order_id` BIGINT NOT NULL COMMENT '订单ID（唯一）',
      `pay_time` DATETIME NOT NULL COMMENT '支付时间（精确到秒，无支付则为NULL）',
      `buyer_id` VARCHAR(64) NOT NULL COMMENT '买家唯一标识（对应访客，需关联）',
      `terminal_type` VARCHAR(20) NOT NULL COMMENT '下单终端类型：-无线端',
      `product_id` BIGINT NOT NULL COMMENT '订单中的商品ID（若多商品拆分行存储）',
      `pay_amount` DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '该商品的支付金额（分商品统计）',
      `is_paid` TINYINT NOT NULL DEFAULT 0 COMMENT '是否支付：1-已支付，0-未支付',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`log_id`),
      INDEX `idx_pay_time` (`pay_time`),
      INDEX `idx_buyer_product` (`buyer_id`, `product_id`, `is_paid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-订单支付日志表';
    """,
    # 4. ODS - 搜索日志表（ods_search_log）
    """
    CREATE TABLE `ods_search_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID',
      `search_time` DATETIME NOT NULL COMMENT '搜索时间（精确到秒）',
      `visitor_id` VARCHAR(64) NOT NULL COMMENT '访客唯一标识',
      `terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型：端，无线端',
      `keyword` VARCHAR(100) NOT NULL COMMENT '搜索关键词（原始输入，如“夏季连衣裙”）',
      `is_click_result` TINYINT NOT NULL DEFAULT 0 COMMENT '是否点击搜索结果：1-是，0-否（关联访问日志）',
      `click_product_id` BIGINT DEFAULT NULL COMMENT '点击的商品ID（=1时非空）',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`log_id`),
      INDEX `idx_search_time` (`search_time`),
      INDEX `idx_keyword_visitor` (`keyword`, `visitor_id`) COMMENT '按关键词+访客去重统计'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-用户搜索日志表';
    """,
    # 5. ODS - 页面点击日志表（ods_page_click_log）
    """
    CREATE TABLE `ods_page_click_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID',
      `click_time` DATETIME NOT NULL COMMENT '点击时间（精确到秒）',
      `visitor_id` VARCHAR(64) NOT NULL COMMENT '访客唯一标识',
      `terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型：端，-无线端',
      `page_id` BIGINT NOT NULL COMMENT '页面ID（被点击的页面）',
      `module_name` VARCHAR(50) NOT NULL COMMENT '页面板块名称（如“轮播图1”“商品推荐区A”）',
      `module_position` VARCHAR(30) DEFAULT NULL COMMENT '板块位置（如“顶部”“中部”“底部”）',
      `guide_product_id` BIGINT DEFAULT NULL COMMENT '引导至的商品ID（点击板块后跳转的商品详情页ID）',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`log_id`),
      INDEX `idx_click_time` (`click_time`),
      INDEX `idx_page_module` (`page_id`, `module_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-页面板块点击日志表';
    """,
    # 6. ODS - 人群属性日志表（ods_crowd_attribute_log）
    """
    CREATE TABLE `ods_crowd_attribute_log` (
      `log_id` BIGINT NOT NULL COMMENT '日志唯一ID',
      `visitor_id` VARCHAR(64) NOT NULL COMMENT '访客唯一标识（关联访问日志）',
      `gender` VARCHAR(10) DEFAULT 'unknown' COMMENT '性别：-男，-女，-未知',
      `age_range` VARCHAR(20) DEFAULT 'unknown' COMMENT '年龄区间：0-18，19-25，26-35，36-45，46+，-未知',
      `city` VARCHAR(50) DEFAULT NULL COMMENT '所在城市（如“北京”）',
      `city_level` VARCHAR(20) DEFAULT 'unknown' COMMENT '城市等级：-一线，-新一线，-二线，-三线及以下，-未知',
      `taoqi_value` INT DEFAULT 0 COMMENT '淘气值（原始数值，如“850”）',
      `update_time` DATETIME NOT NULL COMMENT '属性更新时间（属性变化时同步）',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`log_id`),
      INDEX `idx_visitor` (`visitor_id`) COMMENT '按访客ID关联其他日志',
      INDEX `idx_update_time` (`update_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层-用户人群属性日志表';
    """
]

# ==================== MySQL配置（固定不变）====================
MYSQL_CONFIG = {
    "host": "192.168.142.131",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": MYSQL_DB,
    "charset": "utf8mb4",
    "autocommit": True
}

# ==================== ODS层专用真实数据模板====================
TERMINAL_TYPES = ["pc", "wireless"]
VISIT_OBJECT_TYPES = ["shop", "product", "page"]
TRAFFIC_SOURCES = {
    "first": ["搜索", "直播", "店铺页", "内容推荐", "活动推广", "直接访问"],
    "second": {
        "搜索": ["搜索-夏季连衣裙", "搜索-华为P70", "搜索-无线耳机", "搜索-运动跑鞋", "搜索-空调"],
        "直播": ["直播-官方直播间", "直播-达人带货1", "直播-达人带货2", "直播-品牌专场"],
        "店铺页": ["店铺页-首页", "店铺页-分类页", "店铺页-活动页", "店铺页-商品详情"],
        "内容推荐": ["内容推荐-短视频1", "内容推荐-图文测评", "内容推荐-攻略指南"],
        "活动推广": ["活动推广-618大促", "活动推广-开学季", "活动推广-周末特惠"],
        "直接访问": ["直接访问-书签", "直接访问-输入网址", "直接访问-历史记录"]
    }
}
INTERACT_TYPES = ["add_cart", "collect"]
SEARCH_KEYWORDS = [
    "夏季连衣裙 显瘦", "华为P70 最新款", "iPhone 15 Pro 价格", "小米14 评测",
    "无线耳机 降噪", "运动跑鞋 减震", "空调 一级能效", "笔记本电脑 轻薄"
]
PAGE_MODULES = {
    "module_name": ["轮播图1", "轮播图2", "商品推荐区A", "商品推荐区B", "优惠券模块", "活动入口", "新品专区"],
    "module_position": ["顶部", "中部", "底部", "左侧", "右侧"]
}
CROWD_ATTRIBUTES = {
    "gender": ["male", "female", "unknown"],
    "age_range": ["0-18", "19-25", "26-35", "36-45", "46+", "unknown"],
    "city": [
        ("北京", "first"), ("上海", "first"), ("广州", "first"), ("深圳", "first"),
        ("杭州", "new_first"), ("成都", "new_first"), ("武汉", "new_first"), ("重庆", "new_first"),
        ("南京", "second"), ("苏州", "second"), ("西安", "second"), ("长沙", "second"),
        ("无锡", "third"), ("佛山", "third"), ("合肥", "third"), ("泉州", "third")
    ],
    "taoqi_value": lambda: random.randint(0, 1200)
}
PRODUCT_CATEGORIES = {
    '手机数码': {
        'products': [
            'iPhone 15 Pro', 'Samsung Galaxy S24', '华为P70', '小米14', 'OPPO Find X7',
            'vivo X100 Pro', '荣耀Magic6', '一加12', '魅族21 Pro', '努比亚Z60 Ultra'
        ],
        'product_ids': list(range(1001, 1011))
    },
    '家用电器': {
        'products': [
            '美的变频空调', '格力中央空调', '海尔冰箱', '西门子洗衣机', '戴森吸尘器V15',
            '松下微波炉', '飞利浦电动牙刷', '九阳豆浆机', '苏泊尔电饭煲', '老板油烟机'
        ],
        'product_ids': list(range(2001, 2011))
    },
    '服装鞋帽': {
        'products': [
            'Nike Air Jordan 1', 'Adidas Ultraboost 22', '优衣库摇粒绒外套', 'ZARA牛仔裤',
            'H&M连衣裙', '太平鸟卫衣', '森马夹克', '海澜之家衬衫', '李宁跑鞋', '安踏篮球鞋'
        ],
        'product_ids': list(range(3001, 3011))
    }
}
VISITOR_ID_POOL = [str(uuid.uuid4()) for _ in range(500)]
USER_NAME_GENDER_MAP = {
    '张伟': 'male', '王伟': 'male', '王芳': 'female', '李伟': 'male', '李娜': 'female',
    '张敏': 'female', '李静': 'female', '王静': 'female', '刘伟': 'male', '王秀英': 'female'
}
EMAIL_DOMAINS = ['163.com', 'qq.com', 'gmail.com', 'outlook.com', 'aliyun.com']

# ==================== 数据库连接池====================
class DatabaseConnectionPool:
    def __init__(self, config, max_connections=10):
        self.config = config
        self.max_connections = max_connections
        self.connections = []
        self.used_connections = []

    def get_connection(self):
        if self.connections:
            conn = self.connections.pop()
            self.used_connections.append(conn)
            return conn
        elif len(self.used_connections) < self.max_connections:
            conn = pymysql.connect(** self.config)
            self.used_connections.append(conn)
            return conn
        else:
            time.sleep(0.1)
            return self.get_connection()

    def release_connection(self, conn):
        if conn in self.used_connections:
            self.used_connections.remove(conn)
            self.connections.append(conn)

    def close_all(self):
        for conn in self.connections + self.used_connections:
            try:
                conn.close()
            except Exception as e:
                print(f"关闭连接异常: {str(e)}")
        self.connections = []
        self.used_connections = []

db_pool = DatabaseConnectionPool(MYSQL_CONFIG)

# ==================== ODS层专用数据生成逻辑====================
def get_db_connection():
    return db_pool.get_connection()

def release_db_connection(conn):
    db_pool.release_connection(conn)

def get_random_time(days_range=DAY_TIME_RANGE):
    start = datetime.now() - timedelta(days=days_range)
    end = datetime.now()
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

GENERATED_LOG_IDS = set()
def generate_log_id():
    global GENERATED_LOG_IDS
    timestamp = int(datetime.now().timestamp() * 1000)
    random_suffix = random.randint(100, 999)
    log_id = int(f"{timestamp}{random_suffix}")

    # 确保log_id唯一
    while log_id in GENERATED_LOG_IDS:
        random_suffix = random.randint(100, 999)
        log_id = int(f"{timestamp}{random_suffix}")

    GENERATED_LOG_IDS.add(log_id)
    return log_id

def get_random_visitor_id():
    return random.choice(VISITOR_ID_POOL)

def get_traffic_source():
    first = random.choice(TRAFFIC_SOURCES["first"])
    second = random.choice(TRAFFIC_SOURCES["second"][first])
    return first, second

def get_product_info():
    category = random.choice(list(PRODUCT_CATEGORIES.values()))
    idx = random.randint(0, len(category["products"])-1)
    return category["product_ids"][idx], category["products"][idx]

def get_city_info():
    city, level = random.choice(CROWD_ATTRIBUTES["city"])
    return city, level

def generate_shop_id():
    return FIXED_SHOP_ID

def generate_page_id():
    return random.randint(1, 20)

def generate_order_id():
    timestamp = int(datetime.now().timestamp())
    random_suffix = random.randint(1000, 9999)
    return int(f"{timestamp}{random_suffix}")

# ==================== 修复核心：表字段解析逻辑====================
def identify_field_semantic(field_name, field_comment):
    field_name = field_name.lower()
    field_comment = field_comment.lower() if field_comment else ""

    semantic_mapping = [
        ("log_id", ["log_id"]),
        ("visit_time", ["visit_time"]),
        ("interact_time", ["interact_time"]),
        ("search_time", ["search_time"]),
        ("click_time", ["click_time"]),
        ("pay_time", ["pay_time"]),
        ("update_time", ["update_time"]),
        ("visitor_id", ["visitor_id"]),
        ("buyer_id", ["buyer_id"]),
        ("terminal_type", ["terminal_type"]),
        ("visit_object_type", ["visit_object_type"]),
        ("visit_object_id", ["visit_object_id"]),
        ("traffic_source_first", ["traffic_source_first"]),
        ("traffic_source_second", ["traffic_source_second"]),
        ("is_new_visitor", ["is_new_visitor"]),
        ("page_view", ["page_view"]),
        ("stay_time", ["stay_time"]),
        ("product_id", ["product_id"]),
        ("interact_type", ["interact_type"]),
        ("is_cancel", ["is_cancel"]),
        ("order_id", ["order_id"]),
        ("pay_amount", ["pay_amount"]),
        ("is_paid", ["is_paid"]),
        ("keyword", ["keyword"]),
        ("is_click_result", ["is_click_result"]),
        ("click_product_id", ["click_product_id"]),
        ("page_id", ["page_id"]),
        ("module_name", ["module_name"]),
        ("module_position", ["module_position"]),
        ("guide_product_id", ["guide_product_id"]),
        ("gender", ["gender"]),
        ("age_range", ["age_range"]),
        ("city", ["city"]),
        ("city_level", ["city_level"]),
        ("taoqi_value", ["taoqi_value"]),
        ("create_time", ["create_time"])
    ]

    for semantic, keywords in semantic_mapping:
        for keyword in keywords:
            if keyword in field_name or keyword in field_comment:
                return semantic
    return "general"

# 关键修复：优化建表语句解析，过滤索引和主键定义
def parse_create_table_sql(create_sql):
    # 提取表名
    table_name_match = re.search(r'CREATE TABLE `(\w+)`', create_sql)
    if not table_name_match:
        raise ValueError(f"无法提取表名: {create_sql[:100]}...")
    table_name = table_name_match.group(1)

    # 分离字段定义和索引/主键定义（关键修复点）
    # 将建表语句分割为字段定义部分和约束部分
    if 'PRIMARY KEY' in create_sql:
        fields_part, constraints_part = create_sql.split('PRIMARY KEY', 1)
    else:
        fields_part = create_sql
        constraints_part = ''

    # 只从字段定义部分提取字段信息（忽略约束部分）
    field_pattern = r'`(\w+)`\s+([^,\n]+?)(?:\s+COMMENT\s+[\'\"](.*?)[\'\"])?(?:,|\s*\))'
    fields = re.findall(field_pattern, fields_part, re.DOTALL | re.IGNORECASE)

    field_info = []
    for field in fields:
        field_name = field[0].strip()
        field_type = field[1].strip()
        field_comment = field[2].strip() if len(field) > 2 else ""

        # 排除非字段关键字（增强过滤）
        if field_name.upper() not in ["PRIMARY", "KEY", "INDEX", "UNIQUE", "CONSTRAINT"]:
            field_info.append({
                "name": field_name,
                "type": field_type,
                "comment": field_comment,
                "semantic": identify_field_semantic(field_name, field_comment)
            })

    return table_name, field_info

def generate_and_insert_batch(table_name, fields, batch_num, batch_size):
    conn = get_db_connection()
    try:
        non_auto_fields = [f for f in fields if not f["type"].lower().startswith("auto_increment")]
        field_names = [f["name"] for f in non_auto_fields]
        placeholders = ", ".join(["%s"] * len(field_names))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{n}`' for n in field_names])}) VALUES ({placeholders})"

        batch_data = []
        for _ in range(batch_size):
            row = []
            context = {}  # 用于维护行内字段间的依赖关系

            # 第一步：预处理需要特殊处理的字段
            for field in non_auto_fields:
                if field["semantic"] == "visit_object_type":
                    obj_type = random.choice(VISIT_OBJECT_TYPES)
                    context["visit_object_type"] = obj_type
                    break

            # 第二步：按字段顺序生成数据
            for field in non_auto_fields:
                if field["semantic"] == "visit_object_type":
                    value = context["visit_object_type"]
                elif field["semantic"] == "visit_object_id":
                    # 根据 visit_object_type 生成对应的 ID
                    obj_type = context.get("visit_object_type", random.choice(VISIT_OBJECT_TYPES))
                    if obj_type == "shop":
                        value = generate_shop_id()
                    elif obj_type == "product":
                        product_id, product_name = get_product_info()
                        value = product_id
                    else:  # page
                        value = generate_page_id()
                else:
                    value = generate_field_value(field["semantic"], table_name)

                row.append(value)

            batch_data.append(tuple(row))

        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, batch_data)
            conn.commit()
        print(f"📥 表 {table_name} 批次 {batch_num}: 成功插入 {batch_size} 条数据")
    except Exception as e:
        print(f"❌ 表 {table_name} 批次 {batch_num} 插入失败: {str(e)}")
        if batch_data:
            print(f"错误数据示例: {batch_data[0]}")
        conn.rollback()
        raise
    finally:
        release_db_connection(conn)


def show_sample_data(table_name, limit=3):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{table_name}` ORDER BY create_time DESC LIMIT {limit}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            print(f"\n📊 表 {table_name} 数据示例（前{limit}条）:")
            print(" | ".join(columns))
            print("-" * 100)
            for row in rows:
                print(" | ".join([str(item) if item is not None else "NULL" for item in row]))
            print("-" * 100)
    except Exception as e:
        print(f"❌ 查询 {table_name} 示例数据失败: {str(e)}")
    finally:
        release_db_connection(conn)

def generate_field_value(semantic, table_name):
    if semantic == "log_id":
        return generate_log_id()
    elif semantic in ["visit_time", "interact_time", "search_time", "click_time", "pay_time", "update_time"]:
        return get_random_time().strftime("%Y-%m-%d %H:%M:%S")
    elif semantic == "visitor_id":
        return get_random_visitor_id()
    elif semantic == "buyer_id":
        return get_random_visitor_id()
    elif semantic == "terminal_type":
        return random.choice(TERMINAL_TYPES)
    elif semantic == "visit_object_type":
        return random.choice(VISIT_OBJECT_TYPES)
    elif semantic == "visit_object_id":
        # 修复：根据 visit_object_type 生成对应的 ID
        # 这里需要根据实际的 visit_object_type 来生成 ID
        # 但由于我们是在生成字段值时单独处理每个字段，没有上下文信息
        # 所以我们随机选择一种类型并生成对应的 ID
        obj_type = random.choice(VISIT_OBJECT_TYPES)
        if obj_type == "shop":
            return generate_shop_id()
        elif obj_type == "product":
            product_id, product_name = get_product_info()
            return product_id
        else:  # page
            return generate_page_id()
    elif semantic == "traffic_source_first":
        return get_traffic_source()[0]
    elif semantic == "traffic_source_second":
        return get_traffic_source()[1]
    elif semantic == "is_new_visitor":
        return 1 if random.random() < 0.3 else 0
    elif semantic == "page_view":
        return random.randint(1, 5)
    elif semantic == "stay_time":
        return random.randint(0, 3600)
    elif semantic == "product_id":
        product_id, product_name = get_product_info()
        return product_id
    elif semantic == "interact_type":
        return random.choice(INTERACT_TYPES)
    elif semantic == "is_cancel":
        return 1 if random.random() < 0.1 else 0
    elif semantic == "order_id":
        return generate_order_id()
    elif semantic == "pay_amount":
        return round(random.uniform(10, 10000), 2)
    elif semantic == "is_paid":
        return 1 if random.random() < 0.6 else 0
    elif semantic == "keyword":
        return random.choice(SEARCH_KEYWORDS)
    elif semantic == "is_click_result":
        return 1 if random.random() < 0.4 else 0
    elif semantic == "click_product_id":
        if random.random() < 0.4:
            product_id, product_name = get_product_info()
            return product_id
        else:
            return None
    elif semantic == "page_id":
        return generate_page_id()
    elif semantic == "module_name":
        return random.choice(PAGE_MODULES["module_name"])
    elif semantic == "module_position":
        return random.choice(PAGE_MODULES["module_position"])
    elif semantic == "guide_product_id":
        if random.random() < 0.7:
            product_id, product_name = get_product_info()
            return product_id
        else:
            return None
    elif semantic == "gender":
        return random.choice(CROWD_ATTRIBUTES["gender"])
    elif semantic == "age_range":
        return random.choice(CROWD_ATTRIBUTES["age_range"])
    elif semantic == "city":
        city, level = get_city_info()
        return city
    elif semantic == "city_level":
        city, level = get_city_info()
        return level
    elif semantic == "taoqi_value":
        return CROWD_ATTRIBUTES["taoqi_value"]()
    elif semantic == "create_time":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        return None


def create_table_if_not_exists(create_sql):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
            conn.commit()
            table_name = re.search(r'CREATE TABLE `(\w+)`', create_sql).group(1)
            print(f"✅ 表 {table_name} 创建成功（或已存在）")
            return True
    except Exception as e:
        if "already exists" in str(e).lower():
            table_name = re.search(r'CREATE TABLE `(\w+)`', create_sql).group(1)
            print(f"ℹ️ 表 {table_name} 已存在，跳过创建")
            return True
        else:
            print(f"❌ 建表失败: {str(e)}")
            return False
    finally:
        release_db_connection(conn)

# ==================== 主函数====================
def main():
    print("=" * 60)
    print("        GD03 数仓 ODS层 模拟数据生成脚本")
    print(f"  数据库: {MYSQL_DB} | 总记录数: {TOTAL_RECORDS} | 批次大小: {BATCH_SIZE}")
    print("=" * 60)

    try:
        for create_sql in CREATE_TABLE_SQL_LIST:
            table_name, fields = parse_create_table_sql(create_sql)
            print(f"\n🔍 开始处理表: {table_name}")

            if not create_table_if_not_exists(create_sql):
                print(f"⚠️ 跳过表 {table_name} 数据生成")
                continue

            total_batches = (TOTAL_RECORDS + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"📝 表 {table_name} 共需生成 {TOTAL_RECORDS} 条数据，分 {total_batches} 批次")

            for batch_num in range(1, total_batches + 1):
                current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - (batch_num - 1) * BATCH_SIZE)
                generate_and_insert_batch(table_name, fields, batch_num, current_batch_size)
                time.sleep(0.1)

            show_sample_data(table_name)

        print("\n" + "=" * 60)
        print("🎉 所有ODS层表数据生成完成！")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 脚本执行失败: {str(e)}")
        raise
    finally:
        db_pool.close_all()
        print("\n🔌 数据库连接已全部关闭")

if __name__ == "__main__":
    main()