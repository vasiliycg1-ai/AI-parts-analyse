import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, send_file
from flask_caching import Cache
import pandas as pd
import sqlite3
from datetime import datetime
from werkzeug.utils import secure_filename
from io import BytesIO  # ← ДОБАВИТЬ ЭТОТ ИМПОРТ
from openpyxl import Workbook  # ← И ЭТОТ ТОЖЕ
from openpyxl.styles import Font  # ← И ЭТОТ ДЛЯ ФОРМАТИРОВАНИЯ

# Сначала создаем app, потом импортируем конфиг
app = Flask(__name__)

cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
cache.init_app(app)

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

def get_or_create_part_in_catalog(brand_name, article, conn):
    """Находит или создает деталь в каталоге, возвращает part_id"""
    if not brand_name or not article:
        return None
    
    # Находим или создаем бренд
    brand_id = get_or_create_brand(brand_name, conn)
    if not brand_id:
        return None
    
    # Ищем деталь в каталоге
    part = conn.execute(
        'SELECT id FROM parts_catalog WHERE brand_id = ? AND main_article = ?',
        (brand_id, article)
    ).fetchone()
    
    if part:
        return part['id']
    else:
        # Создаем новую запись в каталоге
        cursor = conn.execute(
            'INSERT INTO parts_catalog (brand_id, main_article) VALUES (?, ?)',
            (brand_id, article)
        )
        return cursor.lastrowid

def normalize_volume_group(volume_group):
    """Нормализует группу объема"""
    if not volume_group:
        return None
    
    volume_lower = str(volume_group).lower()
    if 'топ' in volume_lower or 'top' in volume_lower:
        return 'top_sales'
    elif 'хорош' in volume_lower or 'good' in volume_lower:
        return 'good_demand'
    elif 'низк' in volume_lower or 'low' in volume_lower:
        return 'low_demand'
    elif 'отсут' in volume_lower or 'no' in volume_lower:
        return 'no_demand'
    return None


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

@app.route('/api/order/calculate_supplier', methods=['POST'])
def api_order_calculate_supplier():
    """API для расчета цен по конкретному поставщику"""
    data = request.get_json()
    order_items = data.get('items', [])
    supplier_id = data.get('supplier_id')
    coefficient = float(data.get('coefficient', 0.835))
    
    if not supplier_id:
        return jsonify({'error': 'Supplier ID is required'}), 400
    
    conn = get_db_connection()
    
    try:
        # Получаем информацию о поставщике
        supplier_info = conn.execute('''
            SELECT s.id, s.name, s.currency, r.name as region_name
            FROM suppliers s
            JOIN regions r ON s.region_id = r.id
            WHERE s.id = ?
        ''', (supplier_id,)).fetchone()
        
        if not supplier_info:
            return jsonify({'error': 'Supplier not found'}), 404
        
        # Получаем курсы валют и стоимость доставки
        currency_rates = get_currency_rates(conn)
        delivery_costs = get_delivery_costs(conn)
        
        # Рассчитываем цены для выбранного поставщика
        for item in order_items:
            supplier_price_data = calculate_supplier_price(
                item, supplier_info, currency_rates, 
                delivery_costs, coefficient, conn
            )
            item['specific_supplier'] = supplier_price_data
        
        return jsonify({
            'success': True,
            'order_data': {
                'items': order_items,
                'coefficient': coefficient,
                'specific_supplier': {
                    'id': supplier_info['id'],
                    'name': supplier_info['name'],
                    'currency': supplier_info['currency'],
                    'region': supplier_info['region_name']
                }
            }
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка расчета: {str(e)}'}), 500
    finally:
        conn.close()

def calculate_supplier_price(item, supplier_info, currency_rates, delivery_costs, coefficient, conn):
    """Рассчитываем цену для конкретного поставщика"""
    brand_name = item.get('brand', '')
    article = item.get('article', '')
    weight = item.get('catalog_weight', 0) or 0
    
    # Ищем цену у конкретного поставщика
    price_data = conn.execute('''
        SELECT 
            p.price,
            pl.upload_date
        FROM prices p
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        JOIN parts_catalog pc ON p.part_id = pc.id
        JOIN brands b ON pc.brand_id = b.id
        WHERE b.name = ? AND pc.main_article = ? AND s.id = ? AND pl.is_active = 1
        ORDER BY pl.upload_date DESC
        LIMIT 1
    ''', (brand_name, article, supplier_info['id'])).fetchone()
    
    if not price_data:
        return {
            'price_original': None,
            'currency': supplier_info['currency'],
            'price_rub': None,
            'supplier': supplier_info['name'],
            'profit_percent': None,
            'has_data': False
        }
    
    # Рассчитываем стоимость доставки
    delivery_cost = calculate_delivery_cost(weight, delivery_costs.get(supplier_info['region_name']))
    
    # Конвертируем в рубли
    price_rub = convert_to_rub(
        price_data['price'], 
        supplier_info['currency'], 
        currency_rates,
        delivery_cost,
        coefficient
    )
    
    # Рассчитываем прибыль
    profit_percent = None
    sale_price = item.get('sale_price') or item.get('custom_sale_price')
    if sale_price and price_rub:
        profit_percent = (sale_price / price_rub - 1) * 100
    
    return {
        'price_original': price_data['price'],
        'currency': supplier_info['currency'],
        'price_rub': price_rub,
        'supplier': supplier_info['name'],
        'profit_percent': round(profit_percent, 1) if profit_percent else None,
        'upload_date': price_data['upload_date'],
        'has_data': True
    }

@app.route('/api/suppliers/list')
def api_suppliers_list():
    """API для получения списка всех поставщиков"""
    conn = get_db_connection()
    
    suppliers = conn.execute('''
        SELECT s.id, s.name, s.currency, r.name as region_name
        FROM suppliers s
        JOIN regions r ON s.region_id = r.id
        ORDER BY r.name, s.name
    ''').fetchall()
    
    conn.close()
    
    return jsonify([dict(supplier) for supplier in suppliers])



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


@app.route('/api/validate_upload', methods=['POST'])
def api_validate_upload():
    """API для проверки файла перед загрузкой"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    data_type = request.form.get('data_type', 'own_sales')
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
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
                elif 'количество' in col_lower or 'продажи' in col_lower: 
                    column_mapping[col] = 'quantity'
                elif 'группа' in col_lower: 
                    column_mapping[col] = 'volume_group'
                elif 'запрос' in col_lower: 
                    column_mapping[col] = 'requests'
                elif 'источник' in col_lower: 
                    column_mapping[col] = 'source'
            
            df = df.rename(columns=column_mapping)
            
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            
            # Валидация обязательных полей
            required_fields = ['article', 'brand']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                return jsonify({'error': f'Отсутствуют обязательные колонки: {", ".join(missing_fields)}'}), 400
            
            df = df.dropna(subset=['article', 'brand'])
            
            conn = get_db_connection()
            
            # Анализ данных
            analysis = {
                'total_rows': len(df),
                'existing_parts': 0,
                'new_brands': set(),
                'new_parts': set(),
                'errors': []
            }
            
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                
                if not article or not brand_name:
                    continue
                
                # Проверяем существование бренда
                brand = conn.execute(
                    'SELECT id FROM brands WHERE name = ?', (brand_name,)
                ).fetchone()
                
                if not brand:
                    analysis['new_brands'].add(brand_name)
                    analysis['new_parts'].add(f"{brand_name} - {article}")
                    continue
                
                # Проверяем существование детали в каталоге
                part = conn.execute(
                    'SELECT id FROM parts_catalog WHERE brand_id = ? AND main_article = ?',
                    (brand['id'], article)
                ).fetchone()
                
                if part:
                    analysis['existing_parts'] += 1
                else:
                    analysis['new_parts'].add(f"{brand_name} - {article}")
            
            conn.close()
            
            # Преобразуем множества в списки для JSON
            analysis['new_brands'] = list(analysis['new_brands'])
            analysis['new_parts'] = list(analysis['new_parts'])
            
            return jsonify({
                'success': True,
                'analysis': analysis,
                'file_valid': True
            })
            
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file format'}), 400
    
@app.route('/api/validate_price_list', methods=['POST'])
def api_validate_price_list():
    """API для проверки прайс-листа перед загрузкой"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            # Сохраняем временный файл для анализа
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'temp_' + filename)
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
            
            # Валидация обязательных полей
            required_fields = ['article', 'brand', 'price']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                # Удаляем временный файл
                os.remove(filepath)
                return jsonify({'error': f'Отсутствуют обязательные колонки: {", ".join(missing_fields)}'}), 400
            
            df_valid = df.dropna(subset=['article', 'price'])
            df_invalid = df[df['article'].isna() | df['price'].isna()]
            
            conn = get_db_connection()
            
            # Анализ данных
            analysis = {
                'total_rows': len(df),
                'valid_rows': len(df_valid),
                'invalid_rows': len(df_invalid),
                'existing_parts': 0,
                'new_brands': set(),
                'new_parts': set(),
                'existing_parts_list': [],
                'price_stats': {
                    'min_price': df_valid['price'].min() if len(df_valid) > 0 else 0,
                    'max_price': df_valid['price'].max() if len(df_valid) > 0 else 0,
                    'avg_price': df_valid['price'].mean() if len(df_valid) > 0 else 0
                }
            }
            
            for _, row in df_valid.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                price = row.get('price')
                
                if not article or not brand_name:
                    continue
                
                # Проверяем существование бренда
                brand = conn.execute(
                    'SELECT id FROM brands WHERE name = ?', (brand_name,)
                ).fetchone()
                
                if not brand:
                    analysis['new_brands'].add(brand_name)
                    analysis['new_parts'].add(f"{brand_name} - {article}")
                    continue
                
                # Проверяем существование детали в каталоге
                part = conn.execute(
                    'SELECT id, name_ru FROM parts_catalog WHERE brand_id = ? AND main_article = ?',
                    (brand['id'], article)
                ).fetchone()
                
                if part:
                    analysis['existing_parts'] += 1
                    analysis['existing_parts_list'].append({
                        'brand': brand_name,
                        'article': article,
                        'name': part['name_ru'] or '-',
                        'price': price
                    })
                else:
                    analysis['new_parts'].add(f"{brand_name} - {article}")
            
            conn.close()
            
            # Удаляем временный файл
            os.remove(filepath)
            
            # Преобразуем множества в списки для JSON
            analysis['new_brands'] = list(analysis['new_brands'])
            analysis['new_parts'] = list(analysis['new_parts'])
            
            return jsonify({
                'success': True,
                'analysis': analysis,
                'file_valid': True
            })
            
        except Exception as e:
            # Удаляем временный файл в случае ошибки
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file format'}), 400

# ===== ЗАКАЗЫ =====
@app.route('/purchase_order')
def purchase_order():
    """Страница подготовки заказа"""
    return render_template('purchase_order.html')

@app.route('/api/order/upload', methods=['POST'])
def api_order_upload():
    """API для загрузки файла заказа"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    order_name = request.form.get('order_name', '')
    
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not order_name:
        return jsonify({'error': 'Order name is required'}), 400
    
    if file and file.filename.endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file)
            
            # Маппинг колонок
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'марка' in col_lower: column_mapping[col] = 'brand'
                elif 'артикул' in col_lower: column_mapping[col] = 'article'
                elif 'количество' in col_lower or 'кол-во' in col_lower: column_mapping[col] = 'quantity'
            
            df = df.rename(columns=column_mapping)
            
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            
            # Валидация обязательных полей
            required_fields = ['brand', 'article', 'quantity']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                return jsonify({'error': f'Отсутствуют обязательные колонки: {", ".join(missing_fields)}'}), 400
            
            df = df.dropna(subset=['brand', 'article', 'quantity'])
            
            conn = get_db_connection()
            order_items = []
            
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                quantity = int(row.get('quantity', 1))
                
                if not article or not brand_name:
                    continue
                
                # Ищем деталь в каталоге
                part_data = conn.execute('''
                    SELECT pc.id, pc.main_article, pc.name_ru, pc.weight, b.name as brand_name
                    FROM parts_catalog pc
                    JOIN brands b ON pc.brand_id = b.id
                    WHERE b.name = ? AND pc.main_article = ?
                ''', (brand_name, article)).fetchone()
                
                # Ищем цену продажи
                sale_price_data = conn.execute('''
                    SELECT esp.price_rub, esp.effective_date
                    FROM expected_sale_prices esp
                    JOIN parts_catalog pc ON esp.part_id = pc.id
                    JOIN brands b ON pc.brand_id = b.id
                    WHERE b.name = ? AND pc.main_article = ?
                    ORDER BY esp.effective_date DESC
                    LIMIT 1
                ''', (brand_name, article)).fetchone()
                
                # Ищем статистику
                stats_data = conn.execute('''
                    SELECT ss.data_type, ss.quantity
                    FROM sales_statistics ss
                    JOIN parts_catalog pc ON ss.part_id = pc.id
                    JOIN brands b ON pc.brand_id = b.id
                    WHERE b.name = ? AND pc.main_article = ?
                    ORDER BY ss.period DESC
                    LIMIT 5
                ''', (brand_name, article)).fetchall()
                
                # Формируем статистику
                statistics = format_statistics(stats_data)
                
                order_items.append({
                    'brand': brand_name,
                    'article': article,
                    'name': part_data['name_ru'] if part_data else None,
                    'catalog_weight': part_data['weight'] if part_data else None,
                    'quantity': quantity,
                    'sale_price': sale_price_data['price_rub'] if sale_price_data else None,
                    'sale_price_date': sale_price_data['effective_date'] if sale_price_data else None,
                    'statistics': statistics
                })
            
            conn.close()
            
            return jsonify({
                'success': True,
                'order_data': {
                    'name': order_name,
                    'items': order_items,
                    'coefficient': 0.835
                }
            })
            
        except Exception as e:
            return jsonify({'error': f'Ошибка обработки файла: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file format'}), 400

def format_statistics(stats_data):
    """Форматирование статистики для отображения"""
    if not stats_data:
        return None
    
    own_sales = []
    competitor_sales = []
    analytics_data = []
    
    for stat in stats_data:
        if stat['data_type'] == 'own_sales' and stat['quantity']:
            own_sales.append(stat['quantity'])
        elif stat['data_type'] == 'competitor_sales' and stat['quantity']:
            competitor_sales.append(stat['quantity'])
        elif stat['data_type'] == 'analytics_center':
            analytics_data.append(stat)
    
    # Если есть точные данные по продажам
    if own_sales or competitor_sales:
        own_str = sum(own_sales) if own_sales else '0'
        competitor_str = sum(competitor_sales) if competitor_sales else '0'
        return f"{own_str}/{competitor_str}"
    
    # Если есть аналитика
    if analytics_data:
        latest_analytics = analytics_data[0]
        if latest_analytics.get('volume_group'):
            volume_groups = {
                'top_sales': 'Топ',
                'good_demand': 'Хор',
                'low_demand': 'Низк',
                'no_demand': 'Нет'
            }
            group = volume_groups.get(latest_analytics['volume_group'], latest_analytics['volume_group'])
            requests = latest_analytics.get('requests_per_month', '')
            return f"{group}" + (f"/{requests}" if requests else "")
    
    return None

@app.route('/api/order/update_item', methods=['POST'])
def api_order_update_item():
    """API для обновления данных отдельной позиции"""
    data = request.get_json()
    
    brand_name = data.get('brand', '')
    article = data.get('article', '')
    weight = data.get('weight')
    sale_price = data.get('sale_price')
    update_catalog = data.get('update_catalog', False)
    update_price = data.get('update_price', False)
    
    if not brand_name or not article:
        return jsonify({'error': 'Brand and article are required'}), 400
    
    conn = get_db_connection()
    
    try:
        # Находим деталь в каталоге
        part_data = conn.execute('''
            SELECT pc.id, pc.weight as current_weight
            FROM parts_catalog pc
            JOIN brands b ON pc.brand_id = b.id
            WHERE b.name = ? AND pc.main_article = ?
        ''', (brand_name, article)).fetchone()
        
        if not part_data:
            return jsonify({'error': 'Деталь не найдена в каталоге'}), 404
        
        part_id = part_data['id']
        updates_made = []
        
        # Обновляем вес в каталоге если нужно
        if update_catalog and weight is not None:
            conn.execute('''
                UPDATE parts_catalog 
                SET weight = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (weight, part_id))
            updates_made.append(f'вес обновлен: {weight} кг')
        
        # Обновляем цену продажи если нужно
        if update_price and sale_price is not None:
            conn.execute('''
                INSERT INTO expected_sale_prices (part_id, price_rub, effective_date)
                VALUES (?, ?, DATE('now'))
            ''', (part_id, sale_price))
            updates_made.append(f'цена продажи обновлена: {sale_price} руб')
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': f'Данные обновлены: {", ".join(updates_made)}',
            'part_id': part_id
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'Ошибка обновления: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/api/order/calculate', methods=['POST'])
def api_order_calculate():
    """API для расчета цен по регионам"""
    data = request.get_json()
    order_items = data.get('items', [])
    coefficient = float(data.get('coefficient', 0.835))
    
    conn = get_db_connection()
    
    try:
        # Получаем актуальные курсы валют
        currency_rates = get_currency_rates(conn)
        
        # Получаем стоимость доставки по регионам
        delivery_costs = get_delivery_costs(conn)
        
        # Рассчитываем цены для каждого элемента
        for item in order_items:
            item['regions'] = calculate_region_prices(
                item, currency_rates, delivery_costs, coefficient, conn
            )
        
        return jsonify({
            'success': True,
            'order_data': {
                'items': order_items,
                'coefficient': coefficient
            }
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка расчета: {str(e)}'}), 500
    finally:
        conn.close()

def get_currency_rates(conn):
    """Получаем актуальные курсы валют"""
    rates = conn.execute('''
        SELECT currency_code, rate_to_rub 
        FROM currency_rates 
        ORDER BY created_at DESC
    ''').fetchall()
    
    return {rate['currency_code']: rate['rate_to_rub'] for rate in rates}

def get_delivery_costs(conn):
    """Получаем стоимость доставки по регионам"""
    costs = conn.execute('''
        SELECT r.name as region_name, dc.cost_per_kg, dc.min_cost
        FROM delivery_costs dc
        JOIN regions r ON dc.region_id = r.id
        WHERE r.name IN ('Китай', 'ОАЭ', 'Япония')
    ''').fetchall()
    
    return {cost['region_name']: cost for cost in costs}

def calculate_region_prices(item, currency_rates, delivery_costs, coefficient, conn):
    """Рассчитываем цены для всех регионов"""
    regions_data = {}
    brand_name = item.get('brand', '')
    article = item.get('article', '')
    weight = item.get('catalog_weight', 0) or 0
    
    # Для каждого региона получаем лучшую цену
    for region_name in ['Китай', 'ОАЭ', 'Япония']:
        region_data = calculate_single_region_price(
            brand_name, article, region_name, weight, 
            currency_rates, delivery_costs, coefficient, conn
        )
        regions_data[region_name.lower()] = region_data

    # Находим регион с лучшей ценой
    find_best_region(regions_data, item.get('sale_price'))
    
    return regions_data

def calculate_single_region_price(brand_name, article, region_name, weight, 
                                currency_rates, delivery_costs, coefficient, conn):
    """Рассчитываем цену для одного региона"""
    # Получаем лучшую цену в регионе
    best_price_data = get_best_region_price(brand_name, article, region_name, conn)
    
    if not best_price_data:
        return {
            'price_original': None,
            'currency': None,
            'price_rub': None,
            'supplier': None,
            'profit_percent': None,
            'is_best_price': False,
            'is_high_profit': False
        }
    
    # Рассчитываем стоимость доставки
    delivery_cost = calculate_delivery_cost(weight, delivery_costs.get(region_name))
    
    # Конвертируем в рубли
    price_rub = convert_to_rub(
        best_price_data['price'], 
        best_price_data['currency'], 
        currency_rates,
        delivery_cost,
        coefficient
    )
    
    return {
        'price_original': best_price_data['price'],
        'currency': best_price_data['currency'],
        'price_rub': price_rub,
        'supplier': best_price_data['supplier_name'],
        'profit_percent': None,  # Рассчитаем позже
        'is_best_price': False,
        'is_high_profit': False
    }

def get_best_region_price(brand_name, article, region_name, conn):
    """Находим лучшую цену в указанном регионе (сначала свежие цены от поставщиков, потом минимальную)"""
    query = '''
    WITH LatestSupplierPrices AS (
        -- Сначала получаем самые свежие цены от каждого поставщика
        SELECT 
            p.price,
            s.currency,
            s.name as supplier_name,
            pl.upload_date,
            ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY pl.upload_date DESC) as rn
        FROM prices p
        JOIN price_lists pl ON p.price_list_id = pl.id
        JOIN suppliers s ON pl.supplier_id = s.id
        JOIN regions r ON s.region_id = r.id
        JOIN parts_catalog pc ON p.part_id = pc.id
        JOIN brands b ON pc.brand_id = b.id
        WHERE b.name = ? AND pc.main_article = ? AND r.name = ? AND pl.is_active = 1
    ),
    BestRegionPrice AS (
        -- Затем выбираем минимальную цену среди свежих цен поставщиков
        SELECT 
            price,
            currency,
            supplier_name,
            upload_date,
            ROW_NUMBER() OVER (ORDER BY price ASC, upload_date DESC) as rn_best
        FROM LatestSupplierPrices
        WHERE rn = 1  -- Только самые свежие цены от каждого поставщика
    )
    SELECT * FROM BestRegionPrice WHERE rn_best = 1
    '''
    
    return conn.execute(query, (brand_name, article, region_name)).fetchone()

@app.route('/api/order/export_supplier', methods=['POST'])
def api_order_export_supplier():
    """API для экспорта заказа для поставщиков в их валюте"""
    data = request.get_json()
    order_data = data.get('order_data', {})
    supplier_region = data.get('supplier_region', 'Китай')  # По умолчанию Китай
    
    try:
        conn = get_db_connection()
        
        # Получаем курсы валют
        currency_rates = get_currency_rates(conn)
        
        # Создаем DataFrame для экспорта
        export_data = []
        
        for item in order_data.get('items', []):
            region_data = item.get('regions', {}).get(supplier_region, {})
            
            if not region_data or region_data.get('price_original') is None:
                continue  # Пропускаем позиции без данных по выбранному региону
            
            # Получаем валюту региона
            currency = region_data.get('currency', 'USD')
            price_original = region_data.get('price_original')
            supplier_name = region_data.get('supplier', '')
            
            # Форматируем для поставщика
            row = {
                'Марка': item.get('brand', ''),
                'Артикул': item.get('article', ''),
                'Название': item.get('name', ''),
                'Количество': item.get('quantity', 0),
                f'Цена_({currency})': price_original,
                'Поставщик': supplier_name,
                'Вес_кг': item.get('catalog_weight') or item.get('custom_weight')
            }
            
            # Добавляем расчетную стоимость доставки если нужно
            weight = item.get('catalog_weight') or item.get('custom_weight')
            if weight:
                delivery_cost = calculate_delivery_cost(weight, get_delivery_costs(conn).get(supplier_region))
                row['Доставка_руб'] = delivery_cost
            
            export_data.append(row)
        
        conn.close()
        
        if not export_data:
            return jsonify({'error': f'Нет данных по региону {supplier_region}'}), 400
        
        # Создаем DataFrame
        df = pd.DataFrame(export_data)
        
        # Создаем Excel файл в памяти
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Заказ_поставщику', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Заказ_поставщику']
            
            # Настраиваем ширину колонок
            column_widths = {
                'A': 15, 'B': 20, 'C': 35, 'D': 12, 'E': 15, 'F': 25, 'G': 10, 'H': 15
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            # Добавляем заголовки
            worksheet.insert_rows(1, 4)
            worksheet['A1'] = f"ЗАКАЗ ДЛЯ ПОСТАВЩИКА: {supplier_region}"
            worksheet['A2'] = f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            worksheet['A3'] = f"Курс валют: {', '.join([f'{curr}: {rate}' for curr, rate in currency_rates.items()])}"
            worksheet['A4'] = "ВСЕ ЦЕНЫ УКАЗАНЫ В ВАЛЮТЕ ПОСТАВЩИКА"
        
        output.seek(0)
        
        filename = f"заказ_{supplier_region}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Supplier export error: {str(e)}")
        return jsonify({'error': f'Ошибка экспорта: {str(e)}'}), 500
    
@app.route('/api/order/export_supplier_detailed', methods=['POST'])
def api_order_export_supplier_detailed():
    """Расширенный экспорт с расчетом сумм"""
    data = request.get_json()
    order_data = data.get('order_data', {})
    supplier_region = data.get('supplier_region', 'Китай')
    
    try:
        conn = get_db_connection()
        currency_rates = get_currency_rates(conn)
        
        export_data = []
        total_quantity = 0
        total_value_original = 0
        total_value_rub = 0
        
        for item in order_data.get('items', []):
            region_data = item.get('regions', {}).get(supplier_region, {})
            
            if not region_data or region_data.get('price_original') is None:
                continue
            
            currency = region_data.get('currency', 'USD')
            price_original = region_data.get('price_original')
            price_rub = region_data.get('price_rub')
            quantity = item.get('quantity', 0)
            supplier_name = region_data.get('supplier', '')
            
            # Рассчитываем суммы
            item_value_original = price_original * quantity
            item_value_rub = price_rub * quantity if price_rub else None
            
            total_quantity += quantity
            total_value_original += item_value_original
            total_value_rub += item_value_rub if item_value_rub else 0
            
            row = {
                'Марка': item.get('brand', ''),
                'Артикул': item.get('article', ''),
                'Название': item.get('name', ''),
                'Количество': quantity,
                f'Цена_({currency})': round(price_original, 2),
                f'Сумма_({currency})': round(item_value_original, 2),
                'Цена_руб': round(price_rub, 2) if price_rub else '',
                'Сумма_руб': round(item_value_rub, 2) if item_value_rub else '',
                'Поставщик': supplier_name,
                'Вес_кг': item.get('catalog_weight') or item.get('custom_weight')
            }
            
            export_data.append(row)
        
        conn.close()
        
        if not export_data:
            return jsonify({'error': f'Нет данных по региону {supplier_region}'}), 400
        
        # Добавляем итоговую строку
        if export_data:
            export_data.append({
                'Марка': 'ИТОГО:',
                'Артикул': '',
                'Название': '',
                'Количество': total_quantity,
                f'Цена_({currency})': '',
                f'Сумма_({currency})': round(total_value_original, 2),
                'Цена_руб': '',
                'Сумма_руб': round(total_value_rub, 2),
                'Поставщик': '',
                'Вес_кг': ''
            })
        
        # Создаем DataFrame
        df = pd.DataFrame(export_data)
        
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Заказ_поставщику', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Заказ_поставщику']
            
            # Настраиваем ширину колонок
            column_widths = {
                'A': 15, 'B': 20, 'C': 35, 'D': 12, 'E': 15, 'F': 15, 
                'G': 15, 'H': 15, 'I': 25, 'J': 10
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            # Добавляем заголовки и форматируем итоговую строку
            worksheet.insert_rows(1, 5)
            worksheet['A1'] = f"КОММЕРЧЕСКОЕ ПРЕДЛОЖЕНИЕ: {supplier_region}"
            worksheet['A2'] = f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            worksheet['A3'] = f"Курсы валют: {', '.join([f'{curr}: {rate}' for curr, rate in currency_rates.items()])}"
            worksheet['A4'] = f"Общая сумма: {round(total_value_original, 2)} {currency} ≈ {round(total_value_rub, 2)} руб."
            worksheet['A5'] = "ВСЕ ЦЕНЫ УКАЗАНЫ В ВАЛЮТЕ ПОСТАВЩИКА"
            
            # Выделяем итоговую строку жирным
            if len(export_data) > 0:
                last_row = len(export_data) + 6  # +5 из-за заголовков
                for col in 'ABCDEFGHIJ':
                    cell = f"{col}{last_row}"
                    worksheet[cell].font = Font(bold=True)
        
        output.seek(0)
        
        filename = f"коммерческое_предложение_{supplier_region}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Detailed supplier export error: {str(e)}")
        return jsonify({'error': f'Ошибка экспорта: {str(e)}'}), 500

@app.route('/api/order/export_specific_supplier', methods=['POST'])
def api_order_export_specific_supplier():
    """API для экспорта заказа с ценами конкретного поставщика"""
    data = request.get_json()
    order_data = data.get('order_data', {})
    
    try:
        conn = get_db_connection()
        currency_rates = get_currency_rates(conn)
        
        export_data = []
        total_quantity = 0
        total_value_original = 0
        total_value_rub = 0
        
        for item in order_data.get('items', []):
            supplier_data = item.get('specific_supplier', {})
            
            # Пропускаем позиции без данных по выбранному поставщику
            if not supplier_data or not supplier_data.get('has_data'):
                continue
            
            currency = supplier_data.get('currency', 'USD')
            price_original = supplier_data.get('price_original')
            price_rub = supplier_data.get('price_rub')
            quantity = item.get('quantity', 0)
            supplier_name = supplier_data.get('supplier', '')
            
            # Рассчитываем суммы
            item_value_original = price_original * quantity
            item_value_rub = price_rub * quantity if price_rub else None
            
            total_quantity += quantity
            total_value_original += item_value_original
            total_value_rub += item_value_rub if item_value_rub else 0
            
            row = {
                'Марка': item.get('brand', ''),
                'Артикул': item.get('article', ''),
                'Название': item.get('name', ''),
                'Количество': quantity,
                f'Цена_({currency})': round(price_original, 2),
                f'Сумма_({currency})': round(item_value_original, 2),
                'Цена_руб': round(price_rub, 2) if price_rub else '',
                'Сумма_руб': round(item_value_rub, 2) if item_value_rub else '',
                'Поставщик': supplier_name,
                'Вес_кг': item.get('catalog_weight') or item.get('custom_weight'),
                'Прибыль_%': supplier_data.get('profit_percent', '')
            }
            
            export_data.append(row)
        
        conn.close()
        
        if not export_data:
            return jsonify({'error': 'Нет данных по выбранному поставщику'}), 400
        
        # Добавляем итоговую строку
        if export_data:
            export_data.append({
                'Марка': 'ИТОГО:',
                'Артикул': '',
                'Название': '',
                'Количество': total_quantity,
                f'Цена_({currency})': '',
                f'Сумма_({currency})': round(total_value_original, 2),
                'Цена_руб': '',
                'Сумма_руб': round(total_value_rub, 2),
                'Поставщик': '',
                'Вес_кг': '',
                'Прибыль_%': ''
            })
        
        # Создаем DataFrame
        df = pd.DataFrame(export_data)
        
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Заказ_поставщику', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Заказ_поставщику']
            
            # Настраиваем ширину колонок
            column_widths = {
                'A': 15, 'B': 20, 'C': 35, 'D': 12, 'E': 15, 'F': 15, 
                'G': 15, 'H': 15, 'I': 25, 'J': 10, 'K': 12
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            # Добавляем заголовки и форматируем итоговую строку
            worksheet.insert_rows(1, 5)
            worksheet['A1'] = f"КОММЕРЧЕСКОЕ ПРЕДЛОЖЕНИЕ: {supplier_name}"
            worksheet['A2'] = f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            worksheet['A3'] = f"Курсы валют: {', '.join([f'{curr}: {rate}' for curr, rate in currency_rates.items()])}"
            worksheet['A4'] = f"Общая сумма: {round(total_value_original, 2)} {currency} ≈ {round(total_value_rub, 2)} руб."
            worksheet['A5'] = f"Коэффициент: {order_data.get('coefficient', 0.835)}"
            
            # Выделяем итоговую строку жирным
            if len(export_data) > 0:
                last_row = len(export_data) + 6  # +5 из-за заголовков
                for col in 'ABCDEFGHIJK':
                    cell = f"{col}{last_row}"
                    worksheet[cell].font = Font(bold=True)
        
        output.seek(0)
        
        filename = f"заказ_{supplier_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Specific supplier export error: {str(e)}")
        return jsonify({'error': f'Ошибка экспорта: {str(e)}'}), 500

    
    
def calculate_delivery_cost(weight, delivery_data):
    """Рассчитываем стоимость доставки"""
    if not delivery_data:
        return 0
    
    cost_by_weight = weight * delivery_data['cost_per_kg']
    return max(delivery_data['min_cost'], cost_by_weight)

def convert_to_rub(price, currency, currency_rates, delivery_cost, coefficient):
    """Конвертируем цену в рубли по формуле"""
    if currency == 'RUB':
        price_rub = price
    else:
        rate = currency_rates.get(currency, 1)
        price_rub = price * rate
    
    # Формула: (Цена в валюте × Курс + Доставка) / Коэффициент
    return (price_rub + delivery_cost) / coefficient

def find_best_region(regions_data, sale_price):
    """Находим лучший регион и рассчитываем прибыль"""
    if not sale_price:
        return
    
    # Находим минимальную цену среди регионов с данными
    valid_regions = {name: data for name, data in regions_data.items() 
                    if data['price_rub'] is not None}
    
    if not valid_regions:
        return
    
    best_region_name = min(valid_regions.keys(), 
                          key=lambda x: valid_regions[x]['price_rub'])
    
    # Рассчитываем прибыль для всех регионов и отмечаем лучший
    for region_name, data in regions_data.items():
        if data['price_rub']:
            profit = (sale_price / data['price_rub'] - 1) * 100
            data['profit_percent'] = round(profit, 1)
            data['is_best_price'] = (region_name == best_region_name)
            data['is_high_profit'] = (profit > 15)  # Прибыль более 15%
            

@app.route('/api/order/save', methods=['POST'])
def api_order_save():
    """API для сохранения заказа и обновления данных"""
    data = request.get_json()
    order_name = data.get('order_name', '')
    order_items = data.get('items', [])
    coefficient = data.get('coefficient', 0.835)
    
    if not order_name:
        return jsonify({'error': 'Order name is required'}), 400
    
    conn = get_db_connection()
    
    try:
        # Сохраняем заказ
        cursor = conn.execute('''
            INSERT INTO purchase_orders (order_name, order_date, coefficient)
            VALUES (?, DATE('now'), ?)
        ''', (order_name, coefficient))
        order_id = cursor.lastrowid
        
        # Сохраняем ВСЕ позиции заказа
        saved_count = 0
        for item in order_items:
            # Находим part_id для детали
            part_id = find_part_id(item.get('brand'), item.get('article'), conn)
            
            if not part_id:
                # Если деталь не найдена, пропускаем или создаем?
                continue
            
            quantity = item.get('quantity', 1)
            custom_weight = item.get('custom_weight')
            custom_sale_price = item.get('custom_sale_price')
            update_catalog = item.get('update_catalog', False)
            update_price = item.get('update_price', False)
            
            # Сохраняем позицию заказа
            conn.execute('''
                INSERT INTO order_items (order_id, part_id, quantity, custom_weight, custom_sale_price)
                VALUES (?, ?, ?, ?, ?)
            ''', (order_id, part_id, quantity, custom_weight, custom_sale_price))
            saved_count += 1
            
            # Обновляем каталог если нужно
            if update_catalog and custom_weight is not None:
                conn.execute('''
                    UPDATE parts_catalog 
                    SET weight = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (custom_weight, part_id))
            
            # Обновляем цену продажи если нужно
            if update_price and custom_sale_price is not None:
                conn.execute('''
                    INSERT INTO expected_sale_prices (part_id, price_rub, effective_date)
                    VALUES (?, ?, DATE('now'))
                ''', (part_id, custom_sale_price))
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'order_id': order_id,
            'saved_count': saved_count,
            'message': f'Заказ "{order_name}" успешно сохранен ({saved_count} позиций)'
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'Ошибка сохранения: {str(e)}'}), 500
    finally:
        conn.close()

def find_part_id(brand_name, article, conn):
    """Находит part_id по бренду и артикулу"""
    if not brand_name or not article:
        return None
    
    part_data = conn.execute('''
        SELECT pc.id
        FROM parts_catalog pc
        JOIN brands b ON pc.brand_id = b.id
        WHERE b.name = ? AND pc.main_article = ?
    ''', (brand_name, article)).fetchone()
    
    return part_data['id'] if part_data else None

@app.route('/api/orders/list')
def api_orders_list():
    """API для получения списка сохраненных заказов"""
    conn = get_db_connection()
    
    try:
        orders = conn.execute('''
            SELECT 
                po.id,
                po.order_name,
                po.order_date,
                po.coefficient,
                po.created_at,
                COUNT(oi.id) as items_count
            FROM purchase_orders po
            LEFT JOIN order_items oi ON po.id = oi.order_id
            GROUP BY po.id
            ORDER BY po.created_at DESC
        ''').fetchall()
        
        return jsonify({
            'success': True,
            'orders': [dict(order) for order in orders]
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка загрузки: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/api/order/load/<int:order_id>')
def api_order_load(order_id):
    """API для загрузки сохраненного заказа"""
    conn = get_db_connection()
    
    try:
        # Получаем основную информацию о заказе
        order_info = conn.execute('''
            SELECT * FROM purchase_orders WHERE id = ?
        ''', (order_id,)).fetchone()
        
        if not order_info:
            return jsonify({'error': 'Заказ не найден'}), 404
        
        # Получаем позиции заказа
        order_items = conn.execute('''
            SELECT 
                oi.*,
                b.name as brand_name,
                pc.main_article,
                pc.name_ru,
                pc.weight as catalog_weight
            FROM order_items oi
            JOIN parts_catalog pc ON oi.part_id = pc.id
            JOIN brands b ON pc.brand_id = b.id
            WHERE oi.order_id = ?
        ''', (order_id,)).fetchall()
        
        print(f"DEBUG: Загружен заказ {order_id}, позиций: {len(order_items)}")  # Отладочная информация
        
        # Формируем данные для фронтенда
        items_data = []
        for item in order_items:
            # Ищем актуальную цену продажи
            sale_price_data = conn.execute('''
                SELECT price_rub, effective_date 
                FROM expected_sale_prices 
                WHERE part_id = ? 
                ORDER BY effective_date DESC 
                LIMIT 1
            ''', (item['part_id'],)).fetchone()

            # Ищем статистику
            stats_data = conn.execute('''
                SELECT ss.data_type, ss.quantity
                FROM sales_statistics ss
                WHERE part_id= ?
                ORDER BY ss.period DESC
                LIMIT 5
            ''', (item['part_id'],)).fetchall()
            
            # Формируем статистику
            statistics = format_statistics(stats_data)

            
            item_data = {
                'brand': item['brand_name'],
                'article': item['main_article'],
                'name': item['name_ru'],
                'catalog_weight': item['catalog_weight'],
                'custom_weight': item['custom_weight'],
                'quantity': item['quantity'],
                'sale_price': sale_price_data['price_rub'] if sale_price_data else None,
                'sale_price_date': sale_price_data['effective_date'] if sale_price_data else None,
                'custom_sale_price': item['custom_sale_price'],
                'statistics': statistics,
                'part_id': item['part_id']
            }
            
            items_data.append(item_data)
            print(f"DEBUG: Позиция - {item['brand_name']} {item['main_article']}")  # Отладочная информация
        
        return jsonify({
            'success': True,
            'order_data': {
                'name': order_info['order_name'] + ' (загружен)',
                'items': items_data,
                'coefficient': order_info['coefficient'],
                'original_order_id': order_id
            }
        })
        
    except Exception as e:
        print(f"DEBUG: Ошибка загрузки заказа {order_id}: {str(e)}")  # Отладочная информация
        return jsonify({'error': f'Ошибка загрузки: {str(e)}'}), 500
    finally:
        conn.close()    
    
@app.route('/api/order/find_part', methods=['POST'])
def api_order_find_part():
    """API для поиска детали по бренду и артикулу"""
    data = request.get_json()
    brand_name = data.get('brand', '')
    article = data.get('article', '')
    
    if not brand_name or not article:
        return jsonify({'error': 'Brand and article are required'}), 400
    
    conn = get_db_connection()
    
    try:
        # Ищем деталь в каталоге
        part_data = conn.execute('''
            SELECT pc.id, pc.main_article, pc.name_ru, pc.weight, b.name as brand_name
            FROM parts_catalog pc
            JOIN brands b ON pc.brand_id = b.id
            WHERE b.name = ? AND pc.main_article = ?
        ''', (brand_name, article)).fetchone()
        
        # Ищем цену продажи
        sale_price_data = conn.execute('''
            SELECT esp.price_rub, effective_date
            FROM expected_sale_prices esp
            JOIN parts_catalog pc ON esp.part_id = pc.id
            JOIN brands b ON pc.brand_id = b.id
            WHERE b.name = ? AND pc.main_article = ?
            ORDER BY esp.effective_date DESC
            LIMIT 1
        ''', (brand_name, article)).fetchone()
        
        if not part_data:
            return jsonify({
                'success': True,
                'found': False,
                'message': 'Деталь не найдена в каталоге'
            })
        
        return jsonify({
            'success': True,
            'found': True,
            'part_data': {
                'id': part_data['id'],
                'brand': part_data['brand_name'],
                'article': part_data['main_article'],
                'name': part_data['name_ru'],
                'weight': part_data['weight'],
                'sale_price': sale_price_data['price_rub'] if sale_price_data else None,
                'sale_price_date': sale_price_data['effective_date'] if sale_price_data else None,

            }
        })
        
    except Exception as e:
        return jsonify({'error': f'Ошибка поиска: {str(e)}'}), 500
    finally:
        conn.close()
        
        
@app.route('/api/order/export', methods=['POST'])
def api_order_export():
    """API для экспорта заказа в Excel с датами цен"""
    data = request.get_json()
    order_data = data.get('order_data', {})
    
    try:
        # Создаем DataFrame для экспорта
        export_data = []
        
        for item in order_data.get('items', []):
            # Форматируем дату цены
            price_date = item.get('sale_price_date')
            if price_date:
                try:
                    price_date_str = datetime.strptime(price_date, '%Y-%m-%d').strftime('%d.%m.%Y')
                    days_ago = (datetime.now() - datetime.strptime(price_date, '%Y-%m-%d')).days
                    price_date_display = f"{price_date_str} ({days_ago} дн. назад)"
                except:
                    price_date_display = price_date
            else:
                price_date_display = 'Нет данных'
            
            row = {
                'Марка': item.get('brand', ''),
                'Артикул': item.get('article', ''),
                'Название': item.get('name', ''),
                'Вес_кг': item.get('catalog_weight') or item.get('custom_weight'),
                'Количество': item.get('quantity', 0),
                'Цена_продажи_руб': item.get('sale_price') or item.get('custom_sale_price'),
                'Дата_цены_продажи': price_date_display,
                'Статистика': item.get('statistics', '')
            }
            
            # Добавляем данные по регионам
            regions = item.get('regions', {})
            for region_name in ['китай', 'оаэ', 'япония']:
                region_data = regions.get(region_name, {})
                row[f'Цена_{region_name}_руб'] = region_data.get('price_rub')
                row[f'Прибыль_{region_name}_%'] = region_data.get('profit_percent')
                row[f'Поставщик_{region_name}'] = region_data.get('supplier', '')
            
            export_data.append(row)
        
        # Создаем DataFrame
        df = pd.DataFrame(export_data)
        
        # Создаем Excel файл в памяти
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Заказ', index=False)
            
            # Получаем workbook и worksheet для форматирования
            workbook = writer.book
            worksheet = writer.sheets['Заказ']
            
            # Настраиваем ширину колонок
            column_widths = {
                'A': 15, 'B': 20, 'C': 30, 'D': 10, 'E': 12, 'F': 15, 'G': 20, 'H': 15,
                'I': 15, 'J': 15, 'K': 15, 'L': 15, 'M': 15, 'N': 15, 'O': 20, 'P': 20, 'Q': 20
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            # Добавляем заголовки
            worksheet.insert_rows(1, 4)
            worksheet['A1'] = f"Заказ: {order_data.get('name', 'Без названия')}"
            worksheet['A2'] = f"Коэффициент: {order_data.get('coefficient', 0.835)}"
            worksheet['A3'] = f"Дата экспорта: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            worksheet['A4'] = "Цветовая маркировка: 🔵 - цена старше 2 недель, 🔴 - старше месяца"
        
        output.seek(0)
        
        # Возвращаем файл
        filename = f"заказ_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Export error: {str(e)}")
        return jsonify({'error': f'Ошибка экспорта: {str(e)}'}), 500        

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
    """API для получения актуальных цен продажи с пагинацией и фильтрами"""
    # Параметры пагинации
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page
    
    # Параметры фильтров
    brand_filter = request.args.get('brand', '').strip()
    article_filter = request.args.get('article', '').strip()
    name_filter = request.args.get('name', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    price_from = request.args.get('price_from', type=float)
    price_to = request.args.get('price_to', type=float)
    
    conn = get_db_connection()
    
    # Базовый запрос
    base_query = '''
        FROM expected_sale_prices p
        JOIN parts_catalog pc ON p.part_id = pc.id
        JOIN brands b ON pc.brand_id = b.id
        WHERE p.id IN (
            SELECT id FROM (
                SELECT 
                    id,
                    part_id,
                    ROW_NUMBER() OVER (PARTITION BY part_id ORDER BY effective_date DESC, created_at DESC) as rn
                FROM expected_sale_prices
            ) WHERE rn = 1
        )
    '''
    
    params = []
    
    # Добавляем фильтры
    if brand_filter:
        base_query += ' AND b.name LIKE ?'
        params.append(f'%{brand_filter}%')
    
    if article_filter:
        base_query += ' AND pc.main_article LIKE ?'
        params.append(f'%{article_filter}%')
    
    if name_filter:
        base_query += ' AND pc.name_ru LIKE ?'
        params.append(f'%{name_filter}%')
    
    if date_from:
        base_query += ' AND p.effective_date >= ?'
        params.append(date_from)
    
    if date_to:
        base_query += ' AND p.effective_date <= ?'
        params.append(date_to)
    
    if price_from is not None:
        base_query += ' AND p.price_rub >= ?'
        params.append(price_from)
    
    if price_to is not None:
        base_query += ' AND p.price_rub <= ?'
        params.append(price_to)
    
    # Подсчет общего количества
    count_query = 'SELECT COUNT(DISTINCT p.id) ' + base_query
    total_count = conn.execute(count_query, params).fetchone()[0]
    
    # Запрос данных с пагинацией
    data_query = '''
        SELECT 
            p.id,
            p.part_id,
            b.name AS brand_name,
            pc.main_article,
            pc.name_ru,
            p.price_rub,
            p.effective_date,
            p.notes,
            p.created_at,
            p.updated_at
    ''' + base_query + ' ORDER BY b.name, pc.main_article LIMIT ? OFFSET ?'
    
    params.extend([per_page, offset])
    
    prices = conn.execute(data_query, params).fetchall()
    conn.close()
    
    return jsonify({
        'prices': [dict(price) for price in prices],
        'total_count': total_count,
        'current_page': page,
        'total_pages': (total_count + per_page - 1) // per_page
    })


@app.route('/api/expected_prices/history/<int:part_id>')
def api_expected_prices_history(part_id):
    """API для получения истории цен по конкретной детали"""
    conn = get_db_connection()
    try:
        # Получаем текущую цену
        current_price = conn.execute('''
            SELECT price_rub FROM expected_sale_prices
            WHERE part_id = ?
            ORDER BY effective_date DESC, created_at DESC
            LIMIT 1
        ''', (part_id,)).fetchone()
        
        current_price_value = current_price['price_rub'] if current_price else None
        
        # Получаем историю
        history = conn.execute('''
            SELECT 
                id,
                price_rub,
                effective_date,
                notes,
                updated_at
            FROM expected_sale_prices
            WHERE part_id = ?
            ORDER BY effective_date DESC, updated_at DESC
        ''', (part_id,)).fetchall()
        
        # Преобразуем в словарь и добавляем разницу в процентах
        history_data = []
        for item in history:
            item_dict = dict(item)
            if current_price_value:
                difference = ((current_price_value - item['price_rub']) / item['price_rub']) * 100
                item_dict['difference_percent'] = round(difference, 2)
            else:
                item_dict['difference_percent'] = None
            history_data.append(item_dict)
        
        return jsonify(history_data)
    finally:
        conn.close()

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
            new_brands = set()
            new_parts = set()
            
            for _, row in df.iterrows():
                article = row.get('article', '')
                brand_name = row.get('brand', '')
                price = row.get('price')
                effective_date = row.get('date', datetime.now().date())
                notes = row.get('notes', '')
                
                if not article or not brand_name:
                    continue
                
                # НАХОДИМ ИЛИ СОЗДАЕМ ДЕТАЛЬ В КАТАЛОГЕ
                part_id = get_or_create_part_in_catalog(brand_name, article, conn)
                
                if not part_id:
                    continue
                
                # Добавляем цену с привязкой к каталогу
                conn.execute('''
                    INSERT INTO expected_sale_prices 
                    (part_id, price_rub, effective_date, notes)
                    VALUES (?, ?, ?, ?)
                ''', (part_id, price, effective_date, notes))
                
                added_count += 1
            
            conn.commit()
            conn.close()
            
            return jsonify({
                'success': True,
                'added': added_count,
                'total': len(df)
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
        
        # Получаем part_id из каталога по бренду и артикулу
        part_id = get_or_create_part_in_catalog(data.get('brand', ''), data.get('main_article', ''), conn)
        
        if not part_id:
            conn.close()
            return jsonify({'error': 'Не удалось найти или создать деталь в каталоге'}), 400
        
        # Обновляем цену
        conn.execute('''
            UPDATE expected_sale_prices 
            SET part_id = ?, price_rub = ?, effective_date = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            part_id,
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

# В app.py

@app.route('/api/sales_statistics')
def api_sales_statistics():
    """API для получения статистики продаж с пагинацией"""
    # 1. Получаем параметры страницы из запроса
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int) # 50 записей на страницу по умолчанию
    offset = (page - 1) * per_page

    # Получаем параметры фильтров
    data_type = request.args.get('data_type', 'all')
    volume_group = request.args.get('volume_group', 'all')
    search = request.args.get('search', '').strip()
    conn = get_db_connection()

    # 2. Базовый запрос и фильтры (как у вас, но без ORDER BY и LIMIT)
    base_query = '''
    FROM sales_statistics ss
    JOIN parts_catalog pc ON ss.part_id = pc.id
    JOIN brands b ON pc.brand_id = b.id
    WHERE 1=1
    '''
    params = []

    if data_type != 'all':
        base_query += ' AND ss.data_type = ?'
        params.append(data_type)

    if volume_group != 'all':
        base_query += ' AND ss.volume_group = ?'
        params.append(volume_group)

    if search:
        base_query += ' AND (pc.main_article LIKE ? OR b.name LIKE ?)'
        search_term = f'%{search}%'
        params.extend([search_term, search_term])

    # 3. Выполняем запрос для подсчета ОБЩЕГО количества записей
    total_count_query = 'SELECT COUNT(ss.id) ' + base_query
    total_count = conn.execute(total_count_query, params).fetchone()[0]

    # 4. Выполняем основной запрос с LIMIT и OFFSET для получения только одной страницы
    data_query = '''
    SELECT
        ss.*,
        b.name as brand_name,
        pc.main_article,
        pc.name_ru
    ''' + base_query + ' ORDER BY ss.period DESC, b.name, pc.main_article LIMIT ? OFFSET ?'
    params.extend([per_page, offset])

    stats = conn.execute(data_query, params).fetchall()
    conn.close()

    # 5. Возвращаем структурированный ответ с данными и мета-информацией о страницах
    return jsonify({
        'stats': [dict(stat) for stat in stats],
        'total_count': total_count,
        'current_page': page,
        'total_pages': (total_count + per_page - 1) // per_page
    })
    
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
            
            # Маппинг колонок
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'артикул' in col_lower: column_mapping[col] = 'article'
                elif 'марка' in col_lower: column_mapping[col] = 'brand'
                elif 'период' in col_lower or 'дата' in col_lower: column_mapping[col] = 'period'
                elif 'количество' in col_lower: column_mapping[col] = 'quantity'
                elif 'группа' in col_lower: column_mapping[col] = 'volume_group'
                elif 'запрос' in col_lower: column_mapping[col] = 'requests'
                elif 'источник' in col_lower: column_mapping[col] = 'source'
                elif 'примеч' in col_lower: column_mapping[col] = 'notes'
            
            df = df.rename(columns=column_mapping)
            
            # Очистка данных
            if 'article' in df.columns:
                df['article'] = df['article'].apply(normalize_article)
            if 'brand' in df.columns:
                df['brand'] = df['brand'].fillna('').astype(str).str.strip()
            
            df = df.dropna(subset=['article', 'brand', 'period'])
            
            conn = get_db_connection()
            added_count = 0
            updated_count = 0
            
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
                
                # НАХОДИМ ИЛИ СОЗДАЕМ ДЕТАЛЬ В КАТАЛОГЕ
                part_id = get_or_create_part_in_catalog(brand_name, article, conn)
                
                if not part_id:
                    continue
                
                # Нормализуем группу объема
                volume_group_normalized = normalize_volume_group(volume_group)
                
                # Проверяем существование записи
                existing = conn.execute('''
                    SELECT id FROM sales_statistics 
                    WHERE part_id = ? AND data_type = ? AND period = ?
                ''', (part_id, data_type, period_date)).fetchone()
                
                if existing:
                    # Обновляем существующую запись
                    conn.execute('''
                        UPDATE sales_statistics 
                        SET quantity = ?, volume_group = ?, requests_per_month = ?, 
                            source_name = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (quantity, volume_group_normalized, requests, source, notes, existing['id']))
                    updated_count += 1
                else:
                    # Добавляем новую запись
                    conn.execute('''
                        INSERT INTO sales_statistics 
                        (part_id, data_type, period, quantity, volume_group, 
                         requests_per_month, source_name, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (part_id, data_type, period_date, quantity, volume_group_normalized, 
                          requests, source, notes))
                    added_count += 1
            
            conn.commit()
            conn.close()
            
            return jsonify({
                'success': True,
                'added': added_count,
                'updated': updated_count,
                'total': len(df)
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

    
# ================== КАТАЛОГ ==================
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
@cache.cached(timeout=300)  # 5 минут
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
    