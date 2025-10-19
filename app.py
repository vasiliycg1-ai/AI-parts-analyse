import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
import pandas as pd
import sqlite3
from datetime import datetime
from werkzeug.utils import secure_filename

# Сначала создаем app, потом импортируем конфиг
app = Flask(__name__)

# Определяем среду и загружаем конфиг
try:
    from config import config
    env = os.environ.get('FLASK_ENV') or 'development'
    app.config.from_object(config[env])
    print(f"✅ Загружена конфигурация: {env}")
except ImportError:
    # Fallback конфиг если config.py не существует
    app.config['SECRET_KEY'] = 'dev-secret-key-fallback'
    app.config['DATABASE'] = 'auto_parts.db'
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
    app.config['REQUIRE_AUTH'] = False
    app.config['DEBUG'] = True
    print("⚠️  config.py не найден, используется fallback конфигурация")

# Создаем папку для загрузок
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Простые учетные данные (в продакшене заменить на базу данных)
USERS = {
    'admin': {
        'password': 'admin123',
        'name': 'Администратор'
    },
    'user': {
        'password': 'user123',
        'name': 'Пользователь'
    }
}

@app.before_request
def check_authentication():
    """Проверяем авторизацию для продакшена"""
    # Убедимся что конфиг загружен
    if 'REQUIRE_AUTH' not in app.config:
        app.config['REQUIRE_AUTH'] = False
        
    if app.config['REQUIRE_AUTH'] and not session.get('authenticated'):
        # Исключаем статические файлы и страницу логина
        if request.endpoint and request.endpoint not in ['login', 'static']:
            return redirect(url_for('login'))



def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    with app.open_resource('schema.sql', mode='r') as f:
        conn.executescript(f.read())
    
    # Добавляем базовые регионы
    default_regions = ['Китай', 'Япония', 'ОАЭ', 'Германия', 'Россия']
    for region in default_regions:
        try:
            conn.execute('INSERT INTO regions (name) VALUES (?)', (region,))
        except sqlite3.IntegrityError:
            pass

    # Начальные курсы валют
    default_rates = [
        ('USD', 95.0, 'Реальный курс для закупок'),
        ('EUR', 102.0, 'Реальный курс для закупок'),
        ('CNY', 13.0, 'Реальный курс для закупок'),
    ]

    for currency, rate, desc in default_rates:
        try:
            conn.execute(
                'INSERT INTO currency_rates (currency_code, rate_to_rub, description) VALUES (?, ?, ?)',
                (currency, rate, desc)
            )
        except sqlite3.IntegrityError:
            pass

    # Начальные стоимости доставки
    regions = conn.execute('SELECT id FROM regions').fetchall()
    for region in regions:
        try:
            conn.execute(
                'INSERT INTO delivery_costs (region_id, cost_per_kg, min_cost, description) VALUES (?, ?, ?, ?)',
                (region['id'], 150.0, 500.0, 'Базовая стоимость доставки')
            )
        except sqlite3.IntegrityError:
            pass
        
    conn.commit()
    conn.close()

def normalize_article(article):
    """Нормализация артикула: только буквы и цифры в верхнем регистре"""
    if pd.isna(article) or article == '':
        return ''
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', str(article))
    return cleaned.upper()

def find_brand_by_name(brand_name, conn):
    """
    Ищет бренд по названию, учитывая синонимы и регистр.
    Возвращает ID основного бренда или None если не найден.
    """
    if not brand_name:
        return None
    
    # Нормализуем название для поиска
    normalized_name = brand_name.upper().strip()
    
    # Сначала ищем точное совпадение в основных брендах (без учета регистра)
    brand = conn.execute('''
        SELECT id FROM brands 
        WHERE UPPER(TRIM(name)) = ?
    ''', (normalized_name,)).fetchone()
    
    if brand:
        return brand['id']
    
    # Если не нашли в основных, ищем в синонимах (без учета регистра)
    synonym = conn.execute('''
        SELECT brand_id 
        FROM brand_synonyms 
        WHERE UPPER(TRIM(synonym_name)) = ?
    ''', (normalized_name,)).fetchone()
    
    if synonym:
        return synonym['brand_id']
    
    return None


def get_or_create_brand(brand_name, conn):
    """
    Находит или создает бренд, учитывая синонимы и регистр.
    Возвращает ID бренда.
    """
    if not brand_name:
        return None
    
    # Нормализуем название для поиска (верхний регистр, без лишних пробелов)
    normalized_name = brand_name.upper().strip()
    
    # Ищем бренд с учетом синонимов и регистра
    brand_id = find_brand_by_name(normalized_name, conn)
    
    if brand_id:
        return brand_id
    
    # Если бренд не найден, создаем новый с оригинальным названием
    # но сохраняем также нормализованную версию для поиска
    cursor = conn.execute(
        'INSERT INTO brands (name) VALUES (?)', (brand_name.strip(),)
    )
    brand_id = cursor.lastrowid
    
    # Добавляем нормализованное название как синоним
    if normalized_name != brand_name.upper():
        try:
            conn.execute(
                'INSERT INTO brand_synonyms (brand_id, synonym_name) VALUES (?, ?)',
                (brand_id, normalized_name)
            )
        except sqlite3.IntegrityError:
            pass  # Если синоним уже существует
    
    return brand_id

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Страница входа"""
    # Если авторизация не требуется, перенаправляем на главную
    if not app.config.get('REQUIRE_AUTH', False):
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = USERS.get(username)
        if user and user['password'] == password:
            session['authenticated'] = True
            session['username'] = username
            session['user_name'] = user['name']
            flash(f'Добро пожаловать, {user["name"]}!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Неверное имя пользователя или пароль', 'error')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Выход из системы"""
    session.clear()
    flash('Вы вышли из системы', 'info')
    return redirect(url_for('login' if app.config.get('REQUIRE_AUTH', False) else 'index'))



@app.route('/')
def index():
    from datetime import datetime
    return render_template('upload.html', now=datetime.now())

# ================== РЕГИОНЫ ==================
@app.route('/regions', methods=['GET', 'POST'])
def manage_regions():
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            region_name = request.form.get('region_name').strip()
            if region_name:
                try:
                    conn.execute('INSERT INTO regions (name) VALUES (?)', (region_name,))
                    conn.commit()
                    flash(f'Регион "{region_name}" добавлен!', 'success')
                except sqlite3.IntegrityError:
                    flash(f'Регион "{region_name}" уже существует!', 'error')
        
        elif action == 'delete':
            region_id = request.form.get('region_id')
            # Проверяем, нет ли связанных поставщиков
            has_suppliers = conn.execute(
                'SELECT COUNT(*) FROM suppliers WHERE region_id = ?', 
                (region_id,)
            ).fetchone()[0]
            
            if has_suppliers > 0:
                flash('Нельзя удалить регион, у которого есть поставщики!', 'error')
            else:
                conn.execute('DELETE FROM regions WHERE id = ?', (region_id,))
                conn.commit()
                flash('Регион удален!', 'success')
    
    regions = conn.execute('SELECT * FROM regions ORDER BY name').fetchall()
    conn.close()
    return render_template('regions.html', regions=regions)

# ================== БРЕНДЫ ==================
@app.route('/brands')
def manage_brands():
    """Управление брендами"""
    conn = get_db_connection()
    brands = conn.execute('SELECT * FROM brands ORDER BY name').fetchall()
    conn.close()
    return render_template('brands.html', brands=brands)

@app.route('/api/brands', methods=['GET', 'POST'])
def api_brands():
    """API для работы с брендами"""
    conn = get_db_connection()
    
    if request.method == 'GET':
        brands = conn.execute('SELECT * FROM brands ORDER BY name').fetchall()
        conn.close()
        return jsonify([dict(brand) for brand in brands])
    
    elif request.method == 'POST':
        data = request.get_json()
        brand_name = data.get('name', '').strip()
        
        if not brand_name:
            conn.close()
            return jsonify({'error': 'Название бренда не может быть пустым'}), 400
        
        # Проверяем нет ли уже такого бренда (без учета регистра)
        existing_brand = conn.execute(
            'SELECT id FROM brands WHERE UPPER(name) = UPPER(?)', 
            (brand_name,)
        ).fetchone()
        
        if existing_brand:
            conn.close()
            return jsonify({'error': 'Бренд с таким названием уже существует'}), 400
        
        try:
            cursor = conn.execute(
                'INSERT INTO brands (name, description, country) VALUES (?, ?, ?)',
                (brand_name, data.get('description'), data.get('country'))
            )
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'id': cursor.lastrowid})
        except sqlite3.IntegrityError:
            conn.close()
            return jsonify({'error': 'Ошибка при создании бренда'}), 400

@app.route('/api/brand_synonyms', methods=['GET', 'POST', 'DELETE'])
def api_brand_synonyms():
    """API для управления синонимами брендов"""
    conn = get_db_connection()
    
    if request.method == 'GET':
        synonyms = conn.execute('''
            SELECT bs.*, b.name as brand_name 
            FROM brand_synonyms bs 
            JOIN brands b ON bs.brand_id = b.id 
            ORDER BY b.name, bs.synonym_name
        ''').fetchall()
        conn.close()
        return jsonify([dict(synonym) for synonym in synonyms])
    
    elif request.method == 'POST':
        data = request.get_json()
        brand_id = data.get('brand_id')
        synonym_name = data.get('synonym_name', '').strip().upper()  # Нормализуем
        
        if not brand_id or not synonym_name:
            conn.close()
            return jsonify({'error': 'Не указан brand_id или synonym_name'}), 400
        
        # Проверяем нет ли уже такого синонима
        existing_synonym = conn.execute(
            'SELECT id FROM brand_synonyms WHERE UPPER(synonym_name) = ?', 
            (synonym_name,)
        ).fetchone()
        
        if existing_synonym:
            conn.close()
            return jsonify({'error': 'Такой синоним уже существует'}), 400
        
        try:
            conn.execute(
                'INSERT INTO brand_synonyms (brand_id, synonym_name) VALUES (?, ?)',
                (brand_id, synonym_name)
            )
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        except sqlite3.IntegrityError:
            conn.close()
            return jsonify({'error': 'Ошибка при добавлении синонима'}), 400
    
    elif request.method == 'DELETE':
        synonym_id = request.args.get('id')
        if not synonym_id:
            conn.close()
            return jsonify({'error': 'Не указан ID синонима'}), 400
        
        conn.execute('DELETE FROM brand_synonyms WHERE id = ?', (synonym_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})



# ================== ПОСТАВЩИКИ ==================
@app.route('/suppliers', methods=['GET', 'POST'])
def manage_suppliers():
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            name = request.form.get('name').strip()
            region_id = request.form.get('region_id')
            currency = request.form.get('currency', 'RUB')
            contact_info = request.form.get('contact_info', '')
            
            if name and region_id:
                try:
                    conn.execute(
                        'INSERT INTO suppliers (name, region_id, currency, contact_info) VALUES (?, ?, ?, ?)',
                        (name, region_id, currency, contact_info)
                    )
                    conn.commit()
                    flash(f'Поставщик "{name}" добавлен!', 'success')
                except sqlite3.IntegrityError:
                    flash(f'Поставщик "{name}" уже существует!', 'error')
        
        elif action == 'delete':
            supplier_id = request.form.get('supplier_id')
            has_price_lists = conn.execute(
                'SELECT COUNT(*) FROM price_lists WHERE supplier_id = ?', 
                (supplier_id,)
            ).fetchone()[0]
            
            if has_price_lists > 0:
                flash('Нельзя удалить поставщика, у которого есть прайс-листы!', 'error')
            else:
                conn.execute('DELETE FROM suppliers WHERE id = ?', (supplier_id,))
                conn.commit()
                flash('Поставщик удален!', 'success')
    
    suppliers = conn.execute('''
        SELECT s.*, r.name as region_name 
        FROM suppliers s 
        LEFT JOIN regions r ON s.region_id = r.id 
        ORDER BY s.name
    ''').fetchall()
    
    regions = conn.execute('SELECT * FROM regions ORDER BY name').fetchall()
    conn.close()
    
    return render_template('suppliers.html', suppliers=suppliers, regions=regions)

# ================== ПРАЙС-ЛИСТЫ ==================
@app.route('/price_lists')
def manage_price_lists():
    conn = get_db_connection()
    
    price_lists = conn.execute('''
        SELECT pl.*, s.name as supplier_name, r.name as region_name
        FROM price_lists pl
        JOIN suppliers s ON pl.supplier_id = s.id
        JOIN regions r ON s.region_id = r.id
        ORDER BY pl.upload_date DESC
    ''').fetchall()
    
    conn.close()
    return render_template('price_lists.html', price_lists=price_lists)

@app.route('/api/toggle_price_list/<int:price_list_id>', methods=['POST'])
def toggle_price_list(price_list_id):
    conn = get_db_connection()
    current_state = conn.execute(
        'SELECT is_active FROM price_lists WHERE id = ?', 
        (price_list_id,)
    ).fetchone()['is_active']
    
    new_state = 0 if current_state else 1
    conn.execute(
        'UPDATE price_lists SET is_active = ? WHERE id = ?',
        (new_state, price_list_id)
    )
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'new_state': new_state})

@app.route('/api/price_lists/<int:price_list_id>/description', methods=['PUT'])
def api_update_price_list_description(price_list_id):
    """API для обновления описания прайс-листа"""
    data = request.get_json()
    description = data.get('description', '')
    
    conn = get_db_connection()
    conn.execute(
        'UPDATE price_lists SET description = ? WHERE id = ?',
        (description, price_list_id)
    )
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@app.route('/price_lists/<int:price_list_id>/analysis')
def price_list_analysis(price_list_id):
    """Детальный анализ конкретного прайс-листа (в рамках региона)"""
    conn = get_db_connection()
    
    # Получаем информацию о прайс-листе
    price_list_info = conn.execute('''
        SELECT pl.*, s.name as supplier_name, s.region_id, r.name as region_name
        FROM price_lists pl
        JOIN suppliers s ON pl.supplier_id = s.id
        JOIN regions r ON s.region_id = r.id
        WHERE pl.id = ?
    ''', (price_list_id,)).fetchone()
    
    if not price_list_info:
        flash('Прайс-лист не найден', 'error')
        return redirect(url_for('manage_price_lists'))
    
    # Анализ прайс-листа (только в рамках региона)
    query = '''
    WITH CurrentPrices AS (
        -- Цены из анализируемого прайс-листа
        SELECT 
            pc.id as part_id,
            b.name as brand,
            pc.main_article,
            pc.name_ru,
            p.price as current_price,
            s.id as supplier_id,
            s.name as supplier_name,
            s.currency as supplier_currency,
            s.region_id
        FROM parts_catalog pc
        JOIN brands b ON pc.brand_id = b.id
        JOIN prices p ON p.part_id = pc.id
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        WHERE pl.id = ?
    ),
    PreviousPrices AS (
        -- Предыдущие цены от этого же поставщика
        SELECT 
            p.part_id,
            p.price as previous_price,
            pl.upload_date as previous_date,
            ROW_NUMBER() OVER (PARTITION BY p.part_id ORDER BY pl.upload_date DESC) as rn
        FROM prices p
        JOIN price_lists pl ON p.price_list_id = pl.id
        WHERE pl.supplier_id = (SELECT supplier_id FROM CurrentPrices LIMIT 1)
          AND pl.upload_date < (SELECT upload_date FROM price_lists WHERE id = ?)
          AND pl.is_active = 1
    ),
    RegionalMarketPrices AS (
        -- Самые свежие цены от других поставщиков ИЗ ЭТОГО ЖЕ РЕГИОНА
        SELECT 
            pc.id as part_id,
            p.price as market_price_original,
            s.currency as market_currency,
            -- Конвертируем в рубли для сравнения
            CASE 
                WHEN s.currency = 'RUB' THEN p.price
                ELSE p.price * (
                    SELECT rate_to_rub 
                    FROM currency_rates 
                    WHERE currency_code = s.currency 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            END as market_price_rub,
            s.name as market_supplier_name,
            pl.upload_date as market_date,
            ROW_NUMBER() OVER (PARTITION BY pc.id, s.id ORDER BY pl.upload_date DESC) as rn_supplier
        FROM parts_catalog pc
        JOIN prices p ON p.part_id = pc.id
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        WHERE pl.is_active = 1
          AND pl.upload_date >= DATE('now', '-1300 days')  -- !!!
          AND s.region_id = (SELECT region_id FROM CurrentPrices LIMIT 1)  -- Только тот же регион!
          AND s.id != (SELECT supplier_id FROM CurrentPrices LIMIT 1)      -- Исключаем текущего поставщика
    ),
    BestRegionalPrices AS (
        -- Находим лучшую цену в регионе
        SELECT 
            part_id,
            MIN(market_price_rub) as best_regional_price_rub
        FROM RegionalMarketPrices
        WHERE rn_supplier = 1
        GROUP BY part_id
    ),
    RegionalPriceDetails AS (
        -- Получаем детали по лучшим региональным ценам
        SELECT 
            rmp.part_id,
            rmp.market_price_original as best_regional_price_original,
            rmp.market_currency as best_regional_currency,
            rmp.market_price_rub as best_regional_price_rub,
            rmp.market_supplier_name as best_regional_supplier
        FROM RegionalMarketPrices rmp
        JOIN BestRegionalPrices brp ON rmp.part_id = brp.part_id AND rmp.market_price_rub = brp.best_regional_price_rub
        WHERE rmp.rn_supplier = 1
    )
    SELECT 
        cp.part_id,
        cp.brand,
        cp.main_article,
        cp.name_ru,
        cp.current_price,
        cp.supplier_currency,
        cp.supplier_name,
        pp.previous_price,
        pp.previous_date,
        rpd.best_regional_price_original,
        rpd.best_regional_currency,
        rpd.best_regional_price_rub,
        rpd.best_regional_supplier,
        -- Расчет изменений
        CASE 
            WHEN pp.previous_price IS NOT NULL AND pp.previous_price > 0 
            THEN ROUND(((cp.current_price - pp.previous_price) / pp.previous_price) * 100, 2)
            ELSE NULL 
        END as change_vs_previous_percent,
        CASE 
            WHEN rpd.best_regional_price_rub IS NOT NULL AND rpd.best_regional_price_rub > 0 
            THEN ROUND(((
                CASE 
                    WHEN cp.supplier_currency = 'RUB' THEN cp.current_price
                    ELSE cp.current_price * (
                        SELECT rate_to_rub 
                        FROM currency_rates 
                        WHERE currency_code = cp.supplier_currency 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    )
                END
            ) - rpd.best_regional_price_rub) / rpd.best_regional_price_rub * 100, 2)
            ELSE NULL 
        END as change_vs_regional_percent
    FROM CurrentPrices cp
    LEFT JOIN PreviousPrices pp ON cp.part_id = pp.part_id AND pp.rn = 1
    LEFT JOIN RegionalPriceDetails rpd ON cp.part_id = rpd.part_id
    ORDER BY cp.brand, cp.main_article
    '''
    
    analysis_data = conn.execute(query, (price_list_id, price_list_id)).fetchall()
    
    # Статистика по прайс-листу
    stats = {
        'total_items': len(analysis_data),
        'price_increased': len([r for r in analysis_data if r['change_vs_previous_percent'] and r['change_vs_previous_percent'] > 0]),
        'price_decreased': len([r for r in analysis_data if r['change_vs_previous_percent'] and r['change_vs_previous_percent'] < 0]),
        'better_than_regional': len([r for r in analysis_data if r['change_vs_regional_percent'] and r['change_vs_regional_percent'] <= 0]),
        'worse_than_regional': len([r for r in analysis_data if r['change_vs_regional_percent'] and r['change_vs_regional_percent'] > 0]),
        'region_name': price_list_info['region_name']
    }
    
    # Получаем актуальные курсы валют для отображения
    currency_rates_data = conn.execute('''
        SELECT currency_code, rate_to_rub 
        FROM currency_rates 
        ORDER BY created_at DESC
    ''').fetchall()

    currency_rates = {rate['currency_code']: rate['rate_to_rub'] for rate in currency_rates_data}

    conn.close()

    return render_template('price_list_analysis.html', 
                         price_list=dict(price_list_info),
                         analysis_data=analysis_data,
                         stats=stats,
                         currency_rates=currency_rates)  # Добавляем курсы валют

# ================== ЗАГРУЗКА ФАЙЛОВ ==================
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('Не выбран файл', 'error')
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        flash('Не выбран файл', 'error')
        return redirect(url_for('index'))

    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            supplier_id = request.form.get('supplier_id')
            upload_date = request.form.get('upload_date')
            
            if not supplier_id or not upload_date:
                flash('Необходимо выбрать поставщика и дату', 'error')
                return redirect(url_for('index'))

            # Сохраняем файл
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            # Читаем Excel
            df = pd.read_excel(filepath)
            
            # Определяем колонки
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower: column_mapping[col] = 'article'
                elif 'марка' in col_lower: column_mapping[col] = 'brand'
                elif 'название' in col_lower or 'наименование' in col_lower: column_mapping[col] = 'name'
                elif 'цена' in col_lower: column_mapping[col] = 'price'
                elif 'вес' in col_lower: column_mapping[col] = 'weight'
            
            df = df.rename(columns=column_mapping)
            
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            
            df = df.dropna(subset=['article', 'price'])
            
            conn = get_db_connection()
            
            # Создаем запись о прайс-листе
            cursor = conn.execute(
                'INSERT INTO price_lists (supplier_id, upload_date, file_name) VALUES (?, ?, ?)',
                (supplier_id, upload_date, filename)
            )
            price_list_id = cursor.lastrowid
            
            # Обрабатываем каждую строку
            added_count = 0
            updated_count = 0
            new_brands = set()  # Для отслеживания новых брендов
            
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                name = row.get('name', '')
                weight = row.get('weight')
                price = row.get('price')
                
                if not article or not brand_name:
                    continue
                
                # Нормализуем название для поиска (верхний регистр, без лишних пробелов)
                normalized_name = brand_name.upper().strip()
                # Ищем бренд с учетом синонимов и регистра
                brand_id = find_brand_by_name(normalized_name, conn)
                if not brand_id:
                    new_brands.add(brand_name)

                # Находим или создаем бренд
                brand_id = get_or_create_brand(brand_name, conn)
                
                # Ищем деталь в каталоге
                part = conn.execute(
                    '''SELECT * FROM parts_catalog 
                    WHERE brand_id = ? AND (main_article = ? OR additional_article = ?)''',
                    (brand_id, article, article)
                ).fetchone()
                
                if part:
                    part_id = part['id']
                    # Обновляем данные если они пустые
                    update_data = {}
                    if not part['name_ru'] and name:
                        update_data['name_ru'] = name
                    if not part['weight'] and weight:
                        update_data['weight'] = weight
                    
                    if update_data:
                        set_clause = ', '.join([f"{k} = ?" for k in update_data])
                        values = list(update_data.values()) + [part_id]
                        conn.execute(
                            f'UPDATE parts_catalog SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                            values
                        )
                        updated_count += 1
                else:
                    # Создаем новую запись
                    cursor = conn.execute(
                        'INSERT INTO parts_catalog (brand_id, main_article, name_ru, weight) VALUES (?, ?, ?, ?)',
                        (brand_id, article, name, weight)
                    )
                    part_id = cursor.lastrowid
                    added_count += 1
                
                # Сохраняем цену
                conn.execute(
                    'INSERT INTO prices (price_list_id, part_id, price) VALUES (?, ?, ?)',
                    (price_list_id, part_id, price)
                )
            
            conn.commit()
            conn.close()
            
            # Формируем сообщение с информацией о новых брендах
            message_parts = [
                f'Файл успешно обработан!<br>',
                f'Добавлено новых деталей: {added_count}<br>',
                f'Обновлено существующих: {updated_count}<br>',
                f'Всего обработано записей: {len(df)}'
            ]
            
            if new_brands:
                brands_list = ', '.join(sorted(new_brands))
                message_parts.append(f'<br><strong>⚠️ Добавлены новые бренды:</strong><br>{brands_list}')
                message_parts.append('<small class="text-muted">Проверьте нет ли опечаток в названиях брендов</small>')
            
            flash(''.join(message_parts), 'success')
            
            # Удаляем временный файл
            os.remove(filepath)
            
        except Exception as e:
            flash(f'Ошибка при обработке файла: {str(e)}', 'error')
    else:
        flash('Разрешены только файлы Excel (.xlsx, .xls)', 'error')
    
    return redirect(url_for('index'))

# ================== ОЖИДАЕМЫЕ ЦЕНЫ ПРОДАЖИ ==================

@app.route('/expected_prices')
def manage_expected_prices():
    """Главная страница управления ценами продажи"""
    return render_template('expected_prices.html')

@app.route('/api/expected_prices')
def api_expected_prices():
    """API для получения актуальных цен продажи"""
    conn = get_db_connection()
    
    # Получаем самые актуальные цены для каждой детали
    query = '''
    WITH LatestPrices AS (
        SELECT 
            esp.id,
            esp.brand_id,
            b.name as brand_name,
            esp.main_article,
            esp.price_rub,
            esp.effective_date,
            esp.notes,
            esp.updated_at,
            ROW_NUMBER() OVER (PARTITION BY esp.brand_id, esp.main_article ORDER BY esp.effective_date DESC, esp.updated_at DESC) as rn
        FROM expected_sale_prices esp
        JOIN brands b ON esp.brand_id = b.id
    )
    SELECT * FROM LatestPrices WHERE rn = 1
    ORDER BY brand_name, main_article
    '''
    
    prices = conn.execute(query).fetchall()
    conn.close()
    
    return jsonify([dict(price) for price in prices])

@app.route('/api/expected_prices/history/<int:brand_id>/<article>')
def api_expected_prices_history(brand_id, article):
    """API для получения истории цен по конкретной детали"""
    conn = get_db_connection()
    
    history = conn.execute('''
        SELECT esp.*, b.name as brand_name
        FROM expected_sale_prices esp
        JOIN brands b ON esp.brand_id = b.id
        WHERE esp.brand_id = ? AND esp.main_article = ?
        ORDER BY esp.effective_date DESC, esp.updated_at DESC
    ''', (brand_id, article)).fetchall()
    
    conn.close()
    
    return jsonify([dict(item) for item in history])

@app.route('/api/expected_prices/upload', methods=['POST'])
def api_expected_prices_upload():
    """API для загрузки цен из Excel"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file)
            # Маппинг колонок
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower: column_mapping[col] = 'article'
                elif 'марка' in col_lower: column_mapping[col] = 'brand'
                elif 'цена' in col_lower: column_mapping[col] = 'price'
                elif 'дата' in col_lower: column_mapping[col] = 'date'
                elif 'примеч' in col_lower: column_mapping[col] = 'notes'
            df = df.rename(columns=column_mapping)
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            df = df.dropna(subset=['article', 'price', 'brand'])
            # Если дата не указана, используем текущую
            if 'date' not in df.columns:
                df['date'] = datetime.now().date()
            conn = get_db_connection()
            added_count = 0
            updated_count = 0
            new_brands = set() # <-- Опять создаем множество
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                price = row.get('price')
                effective_date = row.get('date', datetime.now().date())
                notes = row.get('notes', '')
                if not article or not brand_name:
                    continue

                # --- ИСПРАВЛЕНО: Проверяем, был ли бренд найден ДО создания ---
                # Сначала пытаемся найти бренд
                existing_brand_id = find_brand_by_name(brand_name, conn)
                if not existing_brand_id:
                    # Если не нашли, get_or_create_brand создаст новый
                    # и мы можем добавить имя в new_brands
                    new_brands.add(brand_name)

                # Теперь находим или создаем бренд
                brand_id = get_or_create_brand(brand_name, conn)

                if not brand_id:
                    # Если get_or_create_brand не возвращает ID, что-то пошло не так
                    print(f"Ошибка: Не удалось получить или создать бренд для '{brand_name}'")
                    continue
                # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                # Добавляем/обновляем цену
                conn.execute('''
                    INSERT INTO expected_sale_prices
                    (brand_id, main_article, price_rub, effective_date, notes)
                    VALUES (?, ?, ?, ?, ?)
                ''', (brand_id, article, price, effective_date, notes))
                added_count += 1

            conn.commit()
            conn.close()

            return jsonify({
                'success': True,
                'added': added_count,
                'updated': updated_count, # <-- В твоём коде это всегда 0, но оставим как есть
                'total': len(df),
                'new_brands': list(new_brands) # <-- Теперь содержит реальные "новые" бренды
            })
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    return jsonify({'error': 'Invalid file format'}), 400


@app.route('/api/expected_prices/<int:price_id>', methods=['PUT', 'DELETE'])
def api_expected_price_item(price_id):
    """API для обновления или удаления цены"""
    conn = get_db_connection()
    
    if request.method == 'PUT':
        data = request.get_json()
        
        # Находим ID бренда по имени
        brand = conn.execute(
            'SELECT id FROM brands WHERE name = ?', (data.get('brand', ''),)
        ).fetchone()
        
        if not brand:
            conn.close()
            return jsonify({'error': 'Бренд не найден'}), 400
        
        # Обновляем цену
        conn.execute('''
            UPDATE expected_sale_prices 
            SET brand_id = ?, main_article = ?, price_rub = ?, effective_date = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            brand['id'],
            data.get('main_article', ''),
            data.get('price_rub', 0),
            data.get('effective_date', datetime.now().date()),
            data.get('notes', ''),
            price_id
        ))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    
    elif request.method == 'DELETE':
        conn.execute('DELETE FROM expected_sale_prices WHERE id = ?', (price_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})

# ================== СТАТИСТИКА ПРОДАЖ ==================

@app.route('/sales_statistics')
def manage_sales_statistics():
    """Главная страница управления статистикой продаж"""
    return render_template('sales_statistics.html')

@app.route('/api/sales_statistics')
def api_sales_statistics():
    """API для получения статистики продаж"""
    data_type = request.args.get('data_type', 'all')
    volume_group = request.args.get('volume_group', 'all')  # ← ДОБАВИЛИ
    search = request.args.get('search', '')
    conn = get_db_connection()
    
    query = '''
    SELECT 
        ss.*,
        b.name as brand_name,
        CASE 
            WHEN ss.data_type = 'own_sales' THEN 'Мои продажи'
            WHEN ss.data_type = 'competitor_sales' THEN 'Продажи конкурентов'
            WHEN ss.data_type = 'analytics_center' THEN 'Аналитический центр'
            ELSE ss.data_type
        END as data_type_name,
        CASE 
            WHEN ss.volume_group = 'top_sales' THEN 'Топ продаж'
            WHEN ss.volume_group = 'good_demand' THEN 'Хороший спрос'
            WHEN ss.volume_group = 'low_demand' THEN 'Низкий спрос'
            WHEN ss.volume_group = 'no_demand' THEN 'Отсутствие спроса'
            ELSE ss.volume_group
        END as volume_group_name
    FROM sales_statistics ss
    JOIN brands b ON ss.brand_id = b.id
    WHERE 1=1
    '''
    
    params = []
    
    if data_type != 'all':
        query += ' AND ss.data_type = ?'
        params.append(data_type)
    
    # ДОБАВЛЯЕМ ФИЛЬТР ПО ГРУППЕ ОБЪЕМА
    if volume_group != 'all':
        query += ' AND ss.volume_group = ?'
        params.append(volume_group)
    
    if search:
        query += ' AND (ss.main_article LIKE ? OR b.name LIKE ?)'
        search_term = f'%{search}%'
        params.extend([search_term, search_term])
    
    query += ' ORDER BY ss.period DESC, b.name, ss.main_article'
    
    stats = conn.execute(query, params).fetchall()
    conn.close()
    
    return jsonify([dict(stat) for stat in stats])
    
@app.route('/api/sales_statistics/upload', methods=['POST'])
def api_sales_statistics_upload():
    """API для загрузки статистики из Excel"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    data_type = request.form.get('data_type', 'own_sales')
    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file)
            # Маппинг колонок в зависимости от типа данных
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower: 
                    column_mapping[col] = 'article'
                elif 'марка' in col_lower: 
                    column_mapping[col] = 'brand'
                elif 'период' in col_lower or 'дата' in col_lower: 
                    column_mapping[col] = 'period'
                elif 'количество' in col_lower or 'продажи' in col_lower or 'шт' in col_lower: 
                    column_mapping[col] = 'quantity'
                elif 'группа' in col_lower: 
                    column_mapping[col] = 'volume_group'
                elif 'запрос' in col_lower: 
                    column_mapping[col] = 'requests'
                elif 'источник' in col_lower: 
                    column_mapping[col] = 'source'
                elif 'примеч' in col_lower: 
                    column_mapping[col] = 'notes'
            df = df.rename(columns=column_mapping)
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            # Валидация обязательных полей
            required_fields = ['article', 'brand', 'period']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                return jsonify({'error': f'Отсутствуют обязательные колонки: {", ".join(missing_fields)}'}), 400
            df = df.dropna(subset=['article', 'brand', 'period'])
            conn = get_db_connection()
            added_count = 0
            updated_count = 0
            new_brands = set()
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                period = row.get('period')
                quantity = row.get('quantity')
                volume_group = row.get('volume_group', '')
                requests = row.get('requests')
                source = row.get('source', '')
                notes = row.get('notes', '')
                if not article or not brand_name:
                    continue
                # Преобразуем период в дату
                try:
                    if isinstance(period, str):
                        period_date = datetime.strptime(period, '%Y-%m-%d').date()
                    else:
                        period_date = period.date() if hasattr(period, 'date') else datetime.now().date()
                except:
                    period_date = datetime.now().date()

                # --- ИСПРАВЛЕНО: Проверяем, был ли бренд найден ДО создания ---
                existing_brand_id = find_brand_by_name(brand_name, conn)
                if not existing_brand_id:
                    new_brands.add(brand_name)

                # Теперь находим или создаем бренд
                brand_id = get_or_create_brand(brand_name, conn)

                if not brand_id:
                    # Если get_or_create_brand не возвращает ID, что-то пошло не так
                    print(f"Ошибка: Не удалось получить или создать бренд для '{brand_name}'")
                    continue
                # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                # Нормализуем группу объема
                volume_group_normalized = None
                if volume_group:
                    volume_lower = str(volume_group).lower()
                    if 'топ' in volume_lower or 'top' in volume_lower:
                        volume_group_normalized = 'top_sales'
                    elif 'хорош' in volume_lower or 'good' in volume_lower:
                        volume_group_normalized = 'good_demand'
                    elif 'низк' in volume_lower or 'low' in volume_lower:
                        volume_group_normalized = 'low_demand'
                    elif 'отсут' in volume_lower or 'no' in volume_lower:
                        volume_group_normalized = 'no_demand'
                # Проверяем существование записи
                existing = conn.execute('''
                    SELECT id FROM sales_statistics 
                    WHERE brand_id = ? AND main_article = ? AND data_type = ? AND period = ?
                ''', (brand_id, article, data_type, period_date)).fetchone()
                if existing:
                    # Обновляем существующую запись
                    conn.execute('''
                        UPDATE sales_statistics 
                        SET quantity = ?, volume_group = ?, requests_per_month = ?, source_name = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (quantity, volume_group_normalized, requests, source, notes, existing['id']))
                    updated_count += 1
                else:
                    # Добавляем новую запись
                    conn.execute('''
                        INSERT INTO sales_statistics 
                        (brand_id, main_article, data_type, period, quantity, volume_group, requests_per_month, source_name, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (brand_id, article, data_type, period_date, quantity, volume_group_normalized, requests, source, notes))
                    added_count += 1
            conn.commit()
            conn.close()
            return jsonify({
                'success': True,
                'added': added_count,
                'updated': updated_count,
                'total': len(df),
                'new_brands': list(new_brands)
            })
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    return jsonify({'error': 'Invalid file format'}), 400    


@app.route('/api/sales_statistics/<int:stat_id>', methods=['PUT', 'DELETE'])
def api_sales_statistics_item(stat_id):
    """API для обновления или удаления статистики"""
    conn = get_db_connection()
    
    if request.method == 'PUT':
        data = request.get_json()
        
        # Находим ID бренда по имени
        brand = conn.execute(
            'SELECT id FROM brands WHERE name = ?', (data.get('brand', ''),)
        ).fetchone()
        
        if not brand:
            conn.close()
            return jsonify({'error': 'Бренд не найден'}), 400
        
        # Обновляем статистику
        conn.execute('''
            UPDATE sales_statistics 
            SET brand_id = ?, main_article = ?, data_type = ?, period = ?, 
                quantity = ?, volume_group = ?, requests_per_month = ?, 
                source_name = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            brand['id'],
            data.get('main_article', ''),
            data.get('data_type', 'own_sales'),
            data.get('period', datetime.now().date()),
            data.get('quantity'),
            data.get('volume_group'),
            data.get('requests_per_month'),
            data.get('source_name', ''),
            data.get('notes', ''),
            stat_id
        ))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    
    elif request.method == 'DELETE':
        conn.execute('DELETE FROM sales_statistics WHERE id = ?', (stat_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})

@app.route('/api/sales_statistics/aggregated')
def api_sales_statistics_aggregated():
    """API для агрегированной статистики по деталям"""
    conn = get_db_connection()
    
    query = '''
    WITH LatestStats AS (
        SELECT 
            ss.brand_id,
            ss.main_article,
            ss.data_type,
            ss.quantity,
            ss.volume_group,
            ss.requests_per_month,
            ss.period,
            ROW_NUMBER() OVER (PARTITION BY ss.brand_id, ss.main_article, ss.data_type ORDER BY ss.period DESC) as rn
        FROM sales_statistics ss
    )
    SELECT 
        b.name as brand_name,
        ls.main_article,
        ls.data_type,
        ls.quantity,
        ls.volume_group,
        ls.requests_per_month,
        ls.period
    FROM LatestStats ls
    JOIN brands b ON ls.brand_id = b.id
    WHERE ls.rn = 1
    ORDER BY b.name, ls.main_article, ls.data_type
    '''
    
    stats = conn.execute(query).fetchall()
    conn.close()
    
    return jsonify([dict(stat) for stat in stats])

    
#  ================== КАТАЛОГ ==================
@app.route('/catalog')
def catalog_management():
    """Главная страница управления каталогом"""
    conn = get_db_connection()
    
    # Получаем список брендов для фильтра
    brands = conn.execute(
        'SELECT * FROM brands ORDER BY name'
    ).fetchall()
    
    conn.close()
    return render_template('catalog.html', brands=brands)

@app.route('/api/catalog')
def api_catalog():
    """API для получения данных каталога с фильтрацией"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    brand_filter = request.args.get('brand', '')
    article_filter = request.args.get('article', '')
    
    conn = get_db_connection()
    
    # Базовый запрос
    query = '''
    SELECT pc.*, b.name as brand_name 
    FROM parts_catalog pc 
    JOIN brands b ON pc.brand_id = b.id 
    WHERE 1=1
    '''
    params = []
    
    # Добавляем фильтры
    if brand_filter:
        query += ' AND b.name = ?'
        params.append(brand_filter)
    
    if article_filter:
        query += ' AND (pc.main_article LIKE ? OR pc.additional_article LIKE ? OR pc.name_ru LIKE ?)'
        search_term = f'%{article_filter}%'
        params.extend([search_term, search_term, search_term])
    
    # Добавляем пагинацию
    query += ' ORDER BY b.name, pc.main_article LIMIT ? OFFSET ?'
    params.extend([per_page, (page - 1) * per_page])
    
    parts = conn.execute(query, params).fetchall()
    
    # Получаем общее количество для пагинации
    count_query = '''
    SELECT COUNT(*) 
    FROM parts_catalog pc 
    JOIN brands b ON pc.brand_id = b.id 
    WHERE 1=1
    '''
    count_params = []
    
    if brand_filter:
        count_query += ' AND b.name = ?'
        count_params.append(brand_filter)
    
    if article_filter:
        count_query += ' AND (pc.main_article LIKE ? OR pc.additional_article LIKE ? OR pc.name_ru LIKE ?)'
        search_term = f'%{article_filter}%'
        count_params.extend([search_term, search_term, search_term])
    
    total_count = conn.execute(count_query, count_params).fetchone()[0]
    
    conn.close()
    
    # Преобразуем в словарь
    parts_list = [dict(part) for part in parts]
    
    return jsonify({
        'parts': parts_list,
        'total_pages': (total_count + per_page - 1) // per_page,
        'current_page': page,
        'total_count': total_count
    })

@app.route('/api/catalog/upload', methods=['POST'])
def api_catalog_upload():
    """API для загрузки каталога из Excel"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file)
            # Маппинг колонок
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower and 'доп' not in col_lower: 
                    column_mapping[col] = 'main_article'
                elif 'доп' in col_lower and 'артикул' in col_lower: 
                    column_mapping[col] = 'additional_article'
                elif 'марка' in col_lower: column_mapping[col] = 'brand'
                elif 'название' in col_lower and 'англ' not in col_lower: column_mapping[col] = 'name_ru'
                elif 'название' in col_lower and 'англ' in col_lower: column_mapping[col] = 'name_en'
                elif 'вес' in col_lower: column_mapping[col] = 'weight'
                elif 'коэф' in col_lower and 'объем' in col_lower: column_mapping[col] = 'volume_coefficient'
                elif 'примеч' in col_lower: column_mapping[col] = 'notes'
            df = df.rename(columns=column_mapping)
            # Очистка данных
            if 'main_article' in df.columns:
                df['main_article'] = df['main_article'].apply(normalize_article)
            if 'additional_article' in df.columns:
                df['additional_article'] = df['additional_article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            conn = get_db_connection()
            added_count = 0
            updated_count = 0
            new_brands = set()  # Для отслеживания новых брендов
            for _, row in df.iterrows():
                if pd.isna(row.get('main_article')) or pd.isna(row.get('brand')):
                    continue

                # --- ИСПРАВЛЕНО: Проверяем, был ли бренд найден ДО создания ---
                brand_name = row.get('brand', '')
                existing_brand_id = find_brand_by_name(brand_name, conn)
                if not existing_brand_id:
                    new_brands.add(brand_name)

                # Теперь находим или создаем бренд
                brand_id = get_or_create_brand(brand_name, conn)

                if not brand_id:
                    # Если get_or_create_brand не возвращает ID, что-то пошло не так
                    print(f"Ошибка: Не удалось получить или создать бренд для '{brand_name}'")
                    continue
                # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                # Проверяем существование записи
                existing = conn.execute(
                    'SELECT id FROM parts_catalog WHERE brand_id = ? AND main_article = ?',
                    (brand_id, row.get('main_article', ''))
                ).fetchone()
                if existing:
                    # Обновляем существующую запись
                    update_data = {}
                    for field in ['additional_article', 'name_ru', 'name_en', 'weight', 'volume_coefficient', 'notes']:
                        if field in row and not pd.isna(row[field]):
                            update_data[field] = row[field]
                    if update_data:
                        set_clause = ', '.join([f"{k} = ?" for k in update_data])
                        values = list(update_data.values()) + [existing['id']]
                        conn.execute(
                            f'UPDATE parts_catalog SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                            values
                        )
                        updated_count += 1
                else:
                    # Добавляем новую запись
                    conn.execute(
                        '''INSERT INTO parts_catalog 
                        (brand_id, main_article, additional_article, name_ru, name_en, weight, volume_coefficient, notes) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                        (
                            brand_id,
                            row.get('main_article', ''),
                            row.get('additional_article', ''),
                            row.get('name_ru', ''),
                            row.get('name_en', ''),
                            row.get('weight'),
                            row.get('volume_coefficient'),
                            row.get('notes', '')
                        )
                    )
                    added_count += 1
            conn.commit()
            conn.close()
            return jsonify({
                'success': True,
                'added': added_count,
                'updated': updated_count,
                'total': len(df),
                'new_brands': list(new_brands)  # Возвращаем список новых брендов
            })
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    return jsonify({'error': 'Invalid file format'}), 400

# ... (вставь это внутрь main.py, например, после api_catalog_upload и перед if __name__ == '__main__':) ...
@app.route('/match_brands')
def match_brands_page():
    """Отображает страницу для сопоставления артикулов с брендами"""
    return render_template('match_brands.html')

@app.route('/api/match_brands', methods=['POST'])
def api_match_brands():
    """API для сопоставления артикулов с брендами из загруженного файла"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file)
            # Предположим, что колонка с артикулами может называться по-разному, ищем первую подходящую
            article_col = None
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower or 'article' in col_lower:
                    article_col = col
                    break

            if not article_col:
                # Если не найдена колонка с "артикул", используем первую колонку
                article_col = df.columns[0]
                print(f"Предупреждение: Колонка с артикулом не найдена, используется первая колонка: {article_col}")

            # Очищаем и нормализуем артикулы
            df[article_col] = df[article_col].apply(normalize_article)

            # Убираем строки с пустыми артикулами
            df = df.dropna(subset=[article_col])
            df = df[df[article_col] != '']

            conn = get_db_connection()
            # Создаем словарь {артикул: [список брендов]}
            article_to_brands = {}
            for _, row in df.iterrows():
                article = row[article_col]
                if article not in article_to_brands:
                    # Ищем все бренды, связанные с этим артикулом (main_article или additional_article)
                    brands = conn.execute('''
                        SELECT DISTINCT b.name
                        FROM parts_catalog pc
                        JOIN brands b ON pc.brand_id = b.id
                        WHERE pc.main_article = ? OR pc.additional_article = ?
                    ''', (article, article)).fetchall()
                    brand_list = [brand['name'] for brand in brands]
                    article_to_brands[article] = brand_list

            conn.close()

            # Формируем результат с добавленными колонками
            results = []
            for _, row in df.iterrows():
                article = row[article_col]
                brands_found = article_to_brands.get(article, [])
                primary_brand = brands_found[0] if brands_found else 'НЕ НАЙДЕН'
                other_brands = ', '.join(brands_found[1:]) if len(brands_found) > 1 else '' # Соединяем остальные бренды через запятую

                # Копируем все колонки из исходного файла
                result_row = row.to_dict()
                result_row['brand_matched'] = primary_brand
                result_row['other_brands'] = other_brands
                results.append(result_row)

            # Возвращаем результат в виде JSON
            return jsonify({
                'success': True,
                'data': results,
                'total_articles': len(df),
                'not_found_count': len([r for r in results if r['brand_matched'] == 'НЕ НАЙДЕН'])
            })

        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500

    return jsonify({'error': 'Invalid file format'}), 400

# ... (вставь это внутрь main.py, после функции api_match_brands, перед if __name__ == '__main__':) ...
@app.route('/api/match_brands/export', methods=['POST'])
def api_match_brands_export():
    """API для экспорта результатов сопоставления в Excel"""
    try:
        # Получаем данные из тела POST-запроса
        data = request.get_json()
        if not data or 'data' not in data:
            return jsonify({'error': 'Нет данных для экспорта'}), 400

        results = data['data']
        if not results:
            return jsonify({'error': 'Нет данных для экспорта'}), 400

        # Создаем DataFrame из результатов
        df_to_export = pd.DataFrame(results)

        # Генерируем имя файла
        filename = f"matched_brands_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        # Сохраняем DataFrame в байтовый поток
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_to_export.to_excel(writer, sheet_name='Результаты', index=False)
        processed_data = output.getvalue()

        # Возвращаем файл как прикрепленный файл
        from flask import send_file
        output.seek(0) # Смещаем указатель в начало потока
        return send_file(
            output,
            as_attachment=True,
            download_name=filename, # Используем download_name вместо attachment_filename
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        return jsonify({'error': f'Ошибка при создании Excel-файла: {str(e)}'}), 500

    
@app.route('/api/catalog/<int:part_id>', methods=['GET', 'PUT', 'DELETE'])
def api_catalog_item(part_id):
    """API для работы с отдельной записью каталога"""
    conn = get_db_connection()
    
    if request.method == 'GET':
        part = conn.execute('''
            SELECT pc.*, b.name as brand_name 
            FROM parts_catalog pc 
            JOIN brands b ON pc.brand_id = b.id 
            WHERE pc.id = ?
        ''', (part_id,)).fetchone()
        conn.close()
        
        if part:
            return jsonify(dict(part))
        else:
            return jsonify({'error': 'Part not found'}), 404
    
    elif request.method == 'PUT':
        data = request.get_json()
        
        # Находим ID бренда по имени
        brand = conn.execute(
            'SELECT id FROM brands WHERE name = ?', (data.get('brand', ''),)
        ).fetchone()
        
        if not brand:
            conn.close()
            return jsonify({'error': 'Бренд не найден'}), 400
        
        # Обновляем запись
        conn.execute(
            '''UPDATE parts_catalog SET 
            brand_id = ?, main_article = ?, additional_article = ?, 
            name_ru = ?, name_en = ?, weight = ?, 
            volume_coefficient = ?, notes = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?''',
            (
                brand['id'],
                data.get('main_article', ''),
                data.get('additional_article', ''),
                data.get('name_ru', ''),
                data.get('name_en', ''),
                data.get('weight'),
                data.get('volume_coefficient'),
                data.get('notes', ''),
                part_id
            )
        )
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    
    elif request.method == 'DELETE':
        # Проверяем, нет ли связанных цен
        price_count = conn.execute('SELECT COUNT(*) FROM prices WHERE part_id = ?', (part_id,)).fetchone()[0]
        
        if price_count > 0:
            conn.close()
            return jsonify({'error': 'Нельзя удалить деталь, у которой есть цены'}), 400
        
        conn.execute('DELETE FROM parts_catalog WHERE id = ?', (part_id,))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})

def check_currency_rates(conn):
    """Проверяет наличие необходимых курсов валют"""
    suppliers_currencies = conn.execute('''
        SELECT DISTINCT currency FROM suppliers WHERE currency != 'RUB'
    ''').fetchall()
    
    missing_rates = []
    for supplier in suppliers_currencies:
        rate = conn.execute('''
            SELECT currency_code FROM currency_rates 
            WHERE currency_code = ? 
            ORDER BY created_at DESC LIMIT 1
        ''', (supplier['currency'],)).fetchone()
        
        if not rate:
            missing_rates.append(supplier['currency'])
    
    return missing_rates

# ================== АНАЛИЗ ЦЕН ==================
@app.route('/analysis')
def analysis():
    conn = get_db_connection()

    # Проверяем курсы валют
    missing_rates = check_currency_rates(conn)
    if missing_rates:
        flash(f'⚠️ Отсутствуют курсы для валют: {", ".join(missing_rates)}. Некоторые цены могут отображаться некорректно.', 'warning')
    
    query = '''
    WITH LatestSupplierPrices AS (
        SELECT 
            pc.id as part_id,
            b.name as brand,
            pc.main_article,
            pc.name_ru,
            p.price as original_price,
            s.currency as original_currency,
            -- Конвертируем в рубли (исправленная версия)
            CASE 
                WHEN s.currency = 'RUB' THEN p.price
                ELSE p.price * (
                    SELECT rate_to_rub 
                    FROM currency_rates 
                    WHERE currency_code = s.currency 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            END as price_rub,
            pl.upload_date,
            s.name as supplier_name,
            r.name as region_name,
            ROW_NUMBER() OVER (PARTITION BY pc.id, s.id ORDER BY pl.upload_date DESC) as rn_supplier
        FROM parts_catalog pc
        JOIN brands b ON pc.brand_id = b.id
        JOIN prices p ON p.part_id = pc.id
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        JOIN regions r ON s.region_id = r.id
        WHERE pl.is_active = 1
    ),
    BestPrices AS (
        SELECT 
            part_id,
            brand,
            main_article,
            name_ru,
            price_rub as best_price_rub,
            original_price as best_original_price,
            original_currency as best_currency,
            supplier_name as best_supplier,
            upload_date as best_date,
            region_name as best_region,
            ROW_NUMBER() OVER (
                PARTITION BY part_id 
                ORDER BY price_rub ASC, upload_date DESC, supplier_name
            ) as rn_best
        FROM LatestSupplierPrices
        WHERE rn_supplier = 1
    )
    SELECT 
        part_id,
        brand,
        main_article,
        name_ru,
        best_price_rub,
        best_original_price,
        best_currency,
        best_supplier,
        best_date,
        best_region,
        (SELECT GROUP_CONCAT(
            supplier_name || ' (' || 
            original_price || ' ' || original_currency || 
            CASE 
                WHEN original_currency != 'RUB' THEN ' ≈ ' || ROUND(price_rub, 2) || ' руб.'
                ELSE ' руб.'
            END || ')'
        ) 
         FROM LatestSupplierPrices lsp 
         WHERE lsp.part_id = bp.part_id AND lsp.rn_supplier = 1) as all_suppliers
    FROM BestPrices bp
    WHERE rn_best = 1
    ORDER BY brand, main_article
    '''
    
    
    parts = conn.execute(query).fetchall()
    
    # Получаем уникальные значения для фильтров
    brands = conn.execute('SELECT DISTINCT name FROM brands WHERE name IS NOT NULL ORDER BY name').fetchall()
    suppliers = conn.execute('SELECT DISTINCT name FROM suppliers ORDER BY name').fetchall()
    regions = conn.execute('SELECT DISTINCT name FROM regions ORDER BY name').fetchall()
    
    conn.close()
    
    return render_template('analysis.html', 
                         parts=parts,
                         brands=[b['name'] for b in brands],
                         suppliers=[s['name'] for s in suppliers],
                         regions=[r['name'] for r in regions])

# ================== СРАВНЕНИЕ ПОСТАВЩИКОВ ==================
@app.route('/supplier_comparison')
def supplier_comparison():
    conn = get_db_connection()
    
    # Получаем всех поставщиков для выпадающих списков
    suppliers = conn.execute('''
        SELECT s.id, s.name, r.name as region_name 
        FROM suppliers s 
        JOIN regions r ON s.region_id = r.id 
        ORDER BY s.name
    ''').fetchall()
    
    conn.close()
    
    return render_template('supplier_comparison.html', suppliers=suppliers)
@app.route('/api/supplier_comparison')
def api_supplier_comparison():
    supplier1_id = request.args.get('supplier1')
    supplier2_id = request.args.get('supplier2')
    show_all = request.args.get('show_all', 'false').lower() == 'true'
    
    if not supplier1_id:
        return jsonify({'error': 'Не выбран поставщик 1'}), 400
    
    conn = get_db_connection()
    
    # Упрощенный и исправленный запрос
    query = '''
    WITH Supplier1Prices AS (
        SELECT 
            pc.id as part_id,
            b.name as brand,
            pc.main_article,
            pc.name_ru,
            p.price as price1_original,
            s.currency as currency1,
            -- Конвертируем в рубли
            CASE 
                WHEN s.currency = 'RUB' THEN p.price
                ELSE p.price * (
                    SELECT rate_to_rub 
                    FROM currency_rates 
                    WHERE currency_code = s.currency 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            END as price1_rub,
            pl.upload_date as date1,
            s.name as supplier1_name,
            ROW_NUMBER() OVER (PARTITION BY pc.id ORDER BY pl.upload_date DESC) as rn1
        FROM parts_catalog pc
        JOIN brands b ON pc.brand_id = b.id
        JOIN prices p ON p.part_id = pc.id
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        WHERE pl.is_active = 1 AND s.id = ?
    ),
    Supplier2Prices AS (
        SELECT 
            pc.id as part_id,
            p.price as price2_original,
            s.currency as currency2,
            -- Конвертируем в рубли
            CASE 
                WHEN s.currency = 'RUB' THEN p.price
                ELSE p.price * (
                    SELECT rate_to_rub 
                    FROM currency_rates 
                    WHERE currency_code = s.currency 
                    ORDER BY created_at DESC 
                    LIMIT 1
                )
            END as price2_rub,
            pl.upload_date as date2,
            s.name as supplier2_name,
            ROW_NUMBER() OVER (PARTITION BY pc.id ORDER BY pl.upload_date DESC) as rn2
        FROM parts_catalog pc
        JOIN brands b ON pc.brand_id = b.id
        JOIN prices p ON p.part_id = pc.id
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        WHERE pl.is_active = 1 AND s.id = ?
    )
    SELECT 
        s1.part_id,
        s1.brand,
        s1.main_article,
        s1.name_ru,
        s1.price1_original,
        s1.currency1,
        s1.price1_rub,
        s1.date1,
        s1.supplier1_name,
        s2.price2_original,
        s2.currency2,
        s2.price2_rub,
        s2.date2,
        s2.supplier2_name,
        CASE 
            WHEN s2.price2_rub IS NOT NULL AND s1.price1_rub IS NOT NULL AND s1.price1_rub > 0 
            THEN ROUND(((s2.price2_rub - s1.price1_rub) / s1.price1_rub) * 100, 2)
            ELSE NULL 
        END as price_diff_percent,
        CASE 
            WHEN s2.part_id IS NOT NULL THEN 1 
            ELSE 0 
        END as has_intersection
    FROM Supplier1Prices s1
    LEFT JOIN Supplier2Prices s2 ON s1.part_id = s2.part_id AND s2.rn2 = 1
    WHERE s1.rn1 = 1
    '''
    
    if not show_all and supplier2_id:
        query += " AND s2.part_id IS NOT NULL"
    
    query += " ORDER BY s1.brand, s1.main_article"
    
    params = [supplier1_id]
    if supplier2_id:
        params.append(supplier2_id)
    else:
        params.append(supplier1_id)
    
    try:
        results = conn.execute(query, params).fetchall()
        
        # Преобразуем в словарь для JSON
        comparison_data = []
        for row in results:
            comparison_data.append(dict(row))
        
        conn.close()
        return jsonify(comparison_data)
        
    except Exception as e:
        conn.close()
        return jsonify({'error': f'Ошибка базы данных: {str(e)}'}), 500


@app.route('/currency_rates')
def manage_currency_rates():
    """Управление курсами валют"""
    conn = get_db_connection()
    rates = conn.execute('SELECT * FROM currency_rates ORDER BY currency_code').fetchall()
    conn.close()
    return render_template('currency_rates.html', rates=rates)

@app.route('/delivery_costs')
def manage_delivery_costs():
    """Управление стоимостью доставки"""
    conn = get_db_connection()
    
    delivery_costs = conn.execute('''
        SELECT dc.*, r.name as region_name 
        FROM delivery_costs dc
        JOIN regions r ON dc.region_id = r.id
        ORDER BY r.name
    ''').fetchall()
    
    regions = conn.execute('SELECT * FROM regions ORDER BY name').fetchall()
    conn.close()
    
    return render_template('delivery_costs.html', 
                         delivery_costs=delivery_costs, 
                         regions=regions)

@app.route('/api/currency_rates', methods=['GET', 'POST', 'PUT'])
def api_currency_rates():
    """API для управления курсами валют"""
    conn = get_db_connection()
    
    if request.method == 'GET':
        rates = conn.execute('SELECT * FROM currency_rates ORDER BY currency_code').fetchall()
        conn.close()
        return jsonify([dict(rate) for rate in rates])
    
    elif request.method == 'POST':
        data = request.get_json()
        currency_code = data.get('currency_code', '').upper()
        rate_to_rub = data.get('rate_to_rub')
        description = data.get('description', '')
        
        if not currency_code or not rate_to_rub:
            conn.close()
            return jsonify({'error': 'Валюта и курс обязательны'}), 400
        
        try:
            conn.execute('''
                INSERT INTO currency_rates (currency_code, rate_to_rub, description)
                VALUES (?, ?, ?)
            ''', (currency_code, rate_to_rub, description))
            
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        except sqlite3.IntegrityError:
            conn.close()
            return jsonify({'error': 'Курс для этой валюты уже существует'}), 400
    
    elif request.method == 'PUT':
        data = request.get_json()
        currency_code = data.get('currency_code', '').upper()
        rate_to_rub = data.get('rate_to_rub')
        description = data.get('description', '')
        
        if not currency_code or not rate_to_rub:
            conn.close()
            return jsonify({'error': 'Валюта и курс обязательны'}), 400
        
        conn.execute('''
            UPDATE currency_rates 
            SET rate_to_rub = ?, description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE currency_code = ?
        ''', (rate_to_rub, description, currency_code))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True})

@app.route('/api/delivery_costs', methods=['GET', 'POST', 'PUT'])
def api_delivery_costs():
    """API для управления стоимостью доставки"""
    conn = get_db_connection()
    
    if request.method == 'GET':
        costs = conn.execute('''
            SELECT dc.*, r.name as region_name 
            FROM delivery_costs dc
            JOIN regions r ON dc.region_id = r.id
            ORDER BY r.name
        ''').fetchall()
        conn.close()
        return jsonify([dict(cost) for cost in costs])
    
    elif request.method == 'POST':
        data = request.get_json()
        region_id = data.get('region_id')
        cost_per_kg = data.get('cost_per_kg')
        min_cost = data.get('min_cost')
        description = data.get('description', '')
        
        if not region_id or not cost_per_kg or not min_cost:
            conn.close()
            return jsonify({'error': 'Все поля обязательны'}), 400
        
        # Проверяем нет ли уже стоимости для этого региона
        existing = conn.execute(
            'SELECT id FROM delivery_costs WHERE region_id = ?', 
            (region_id,)
        ).fetchone()
        
        if existing:
            conn.close()
            return jsonify({'error': 'Стоимость доставки для этого региона уже существует'}), 400
        
        try:
            conn.execute('''
                INSERT INTO delivery_costs (region_id, cost_per_kg, min_cost, description)
                VALUES (?, ?, ?, ?)
            ''', (region_id, cost_per_kg, min_cost, description))
            
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            conn.close()
            return jsonify({'error': f'Ошибка: {str(e)}'}), 500
    
    elif request.method == 'PUT':
        data = request.get_json()
        cost_id = data.get('id')
        cost_per_kg = data.get('cost_per_kg')
        min_cost = data.get('min_cost')
        description = data.get('description', '')
        
        if not cost_id or not cost_per_kg or not min_cost:
            conn.close()
            return jsonify({'error': 'Все поля обязательны'}), 400
        
        conn.execute('''
            UPDATE delivery_costs 
            SET cost_per_kg = ?, min_cost = ?, description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (cost_per_kg, min_cost, description, cost_id))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True})


# ================== API ДЛЯ ВЫПАДАЮЩИХ СПИСКОВ ==================
@app.route('/api/suppliers')
def api_suppliers():
    conn = get_db_connection()
    suppliers = conn.execute('''
        SELECT s.id, s.name, r.name as region_name 
        FROM suppliers s 
        JOIN regions r ON s.region_id = r.id 
        ORDER BY s.name
    ''').fetchall()
    conn.close()
    
    suppliers_list = [{'id': s['id'], 'name': f"{s['name']} ({s['region_name']})"} for s in suppliers]
    return jsonify(suppliers_list)



if __name__ == '__main__':
    if not os.path.exists(app.config['DATABASE']):
        init_db()
    
    print(f"🚀 Запуск в режиме: {'production' if app.config.get('REQUIRE_AUTH') else 'development'}")
    print(f"🔐 Авторизация: {'ВКЛ' if app.config.get('REQUIRE_AUTH') else 'ВЫКЛ'}")
    
    app.run(debug=app.config.get('DEBUG', True), host='0.0.0.0', port=5000)
    