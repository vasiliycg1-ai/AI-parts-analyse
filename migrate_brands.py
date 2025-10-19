import sqlite3
import os

def migrate_brands():
    # Подключаемся к базе
    conn = sqlite3.connect('auto_parts.db')
    conn.row_factory = sqlite3.Row
    
    print("Начинаем миграцию брендов...")
    
    try:
        # 1. Создаем таблицу брендов если её нет
        conn.execute('''
            CREATE TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                country TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 2. Получаем уникальные бренды из каталога
        unique_brands = conn.execute('''
            SELECT DISTINCT brand FROM parts_catalog 
            WHERE brand IS NOT NULL AND brand != '' 
            ORDER BY brand
        ''').fetchall()
        
        print(f"Найдено {len(unique_brands)} уникальных брендов")
        
        # 3. Добавляем бренды в новую таблицу
        brand_map = {}
        for row in unique_brands:
            brand_name = row['brand']
            try:
                cursor = conn.execute(
                    'INSERT INTO brands (name) VALUES (?)',
                    (brand_name,)
                )
                brand_map[brand_name] = cursor.lastrowid
                print(f"Добавлен бренд: {brand_name} (ID: {cursor.lastrowid})")
            except sqlite3.IntegrityError:
                # Если бренд уже существует
                existing = conn.execute(
                    'SELECT id FROM brands WHERE name = ?', 
                    (brand_name,)
                ).fetchone()
                brand_map[brand_name] = existing['id']
                print(f"Бренд уже существует: {brand_name} (ID: {existing['id']})")
        
        # 4. Создаем временную таблицу с новой структурой
        conn.execute('''
            CREATE TABLE IF NOT EXISTS parts_catalog_new (
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
            )
        ''')
        
        # 5. Копируем данные в новую таблицу
        parts = conn.execute('SELECT * FROM parts_catalog').fetchall()
        
        for part in parts:
            brand_id = brand_map.get(part['brand'])
            if brand_id:
                conn.execute('''
                    INSERT INTO parts_catalog_new 
                    (brand_id, main_article, additional_article, name_ru, name_en, weight, volume_coefficient, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    brand_id,
                    part['main_article'],
                    part['additional_article'],
                    part['name_ru'],
                    part['name_en'],
                    part['weight'],
                    part['volume_coefficient'],
                    part['notes'],
                    part['created_at'],
                    part['updated_at']
                ))
        
        print(f"Перенесено {len(parts)} записей в новую таблицу")
        
        # 6. Заменяем старую таблицу на новую
        conn.execute('DROP TABLE parts_catalog')
        conn.execute('ALTER TABLE parts_catalog_new RENAME TO parts_catalog')
        
        # 7. Создаем индексы
        conn.execute('CREATE INDEX IF NOT EXISTS idx_parts_brand_id ON parts_catalog(brand_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_parts_main_article ON parts_catalog(main_article)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_parts_additional_article ON parts_catalog(additional_article)')
        
        conn.commit()
        print("Миграция успешно завершена!")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при миграции: {e}")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    migrate_brands()