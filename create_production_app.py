from app import app
from config import ProductionConfig

if __name__ == '__main__':
    app.config.from_object(ProductionConfig)
    
    # Для продакшена используем Waitress вместо dev-сервера
    from waitress import serve
    print("Запуск продакшен сервера на http://localhost:8080")
    serve(app, host='0.0.0.0', port=8080)