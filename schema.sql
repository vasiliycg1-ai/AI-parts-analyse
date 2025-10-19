-- Таблица регионов
CREATE TABLE IF NOT EXISTS regions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Таблица брендов
CREATE TABLE IF NOT EXISTS brands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    country TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Таблица поставщиков
CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_id INTEGER NOT NULL,
    name TEXT NOT NULL UNIQUE,
    currency TEXT DEFAULT 'RUB',
    contact_info TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions (id)
);

-- Основной каталог запчастей
CREATE TABLE IF NOT EXISTS parts_catalog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id INTEGER NOT NULL,
    main_article TEXT NOT NULL,
    additional_article TEXT,
    name_ru TEXT,
    name_en TEXT,
    weight REAL,
    volume_coefficient REAL,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands (id)
);

-- Таблица загруженных прайс-листов
CREATE TABLE IF NOT EXISTS price_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id INTEGER NOT NULL,
    upload_date DATE NOT NULL,
    file_name TEXT NOT NULL,
    description TEXT,  -- ← НОВОЕ ПОЛЕ
    is_active BOOLEAN DEFAULT 1,
    uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
);

-- Таблица цен
CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    price_list_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    price REAL NOT NULL,
    FOREIGN KEY (price_list_id) REFERENCES price_lists (id),
    FOREIGN KEY (part_id) REFERENCES parts_catalog (id)
);

-- Таблица курсов валют (ручное управление)
CREATE TABLE IF NOT EXISTS currency_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    currency_code TEXT NOT NULL UNIQUE,  -- USD, EUR, CNY, etc.
    rate_to_rub REAL NOT NULL,           -- Реальный курс к рублю
    description TEXT,                     -- Например: "Реальный курс для закупок"
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Таблица стоимости доставки по регионам
CREATE TABLE IF NOT EXISTS delivery_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_id INTEGER NOT NULL,
    cost_per_kg REAL NOT NULL,           -- Стоимость доставки за 1 кг
    min_cost REAL NOT NULL,              -- Минимальная стоимость доставки
    description TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (region_id) REFERENCES regions (id)
);

-- Таблица ожидаемых цен продажи
CREATE TABLE IF NOT EXISTS expected_sale_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id INTEGER NOT NULL,
    main_article TEXT NOT NULL,
    price_rub REAL NOT NULL,
    effective_date DATE NOT NULL,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands (id)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_expected_prices_brand_article ON expected_sale_prices(brand_id, main_article);
CREATE INDEX IF NOT EXISTS idx_expected_prices_date ON expected_sale_prices(effective_date);
CREATE INDEX IF NOT EXISTS idx_expected_prices_article ON expected_sale_prices(main_article);

-- Таблица статистики продаж
CREATE TABLE IF NOT EXISTS sales_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id INTEGER NOT NULL,
    main_article TEXT NOT NULL,
    data_type TEXT NOT NULL,  -- 'own_sales', 'competitor_sales', 'analytics_center'
    period DATE NOT NULL,      -- Период данных (год-месяц)
    quantity INTEGER,          -- Количество продаж/запросов
    volume_group TEXT,         -- Группа объема: 'top_sales', 'good_demand', 'low_demand', 'no_demand'
    requests_per_month INTEGER,-- Количество запросов в месяц (для analytics_center)
    source_name TEXT,          -- Название источника (для competitor_sales)
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands (id)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_sales_stats_brand_article ON sales_statistics(brand_id, main_article);
CREATE INDEX IF NOT EXISTS idx_sales_stats_type ON sales_statistics(data_type);
CREATE INDEX IF NOT EXISTS idx_sales_stats_period ON sales_statistics(period);
CREATE INDEX IF NOT EXISTS idx_sales_stats_volume_group ON sales_statistics(volume_group);





-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_parts_brand_id ON parts_catalog(brand_id);
CREATE INDEX IF NOT EXISTS idx_parts_main_article ON parts_catalog(main_article);
CREATE INDEX IF NOT EXISTS idx_parts_additional_article ON parts_catalog(additional_article);
CREATE INDEX IF NOT EXISTS idx_price_lists_active ON price_lists(is_active, upload_date);
CREATE INDEX IF NOT EXISTS idx_prices_part ON prices(part_id);
CREATE INDEX IF NOT EXISTS idx_suppliers_region ON suppliers(region_id);
CREATE INDEX IF NOT EXISTS idx_brands_name ON brands(name);