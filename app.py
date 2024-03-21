import secrets
from datetime import timedelta
from flask_cors import CORS
from flask import Flask
from flask_mail import Mail
from flask_jwt_extended import JWTManager    

# mail = Mail()

def create_app() -> Flask:

    app = Flask(__name__)
    # mail.init_app(app)
    app.config["SECRET_KEY"] = secrets.token_hex()
    app.config["JWT_SECRET_KEY"] = "secret" 
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=1)
    app.config["JWT_TOKEN_LOCATION"] = ["cookies", "headers"] 
    app.config["JWT_COOKIE_CSRF_PROTECT"] = True
    app.config["JWT_COOKIE_SECURE"] = False #TODO: CHANGE THIS TO TRUE  
    app.config["JWT_CSRF_METHODS"] = ["GET", "POST", "PUT", "PATCH", "DELETE"]
    app.config["JWT_COOKIE_DOMAIN"] = "localhost"
    JWTManager(app)
    CORS(app, supports_credentials=True)
    # app.config["CELERY"] = {
    #     "broker": "pyamqp://guest@localhost//",
    #     "result_backend": "mongodb://localhost:27017/celery",
    #     "task_ignore_result": True
    # }
    # celery_init_app(app)

    from api.saviors.router import bp as saviors_bp
    app.register_blueprint(saviors_bp, url_prefix="/saviors")
    
    from api.users.router import bp as users_bp
    app.register_blueprint(users_bp, url_prefix="/users")
    
    from api.partners.router import bp as partners_bp
    app.register_blueprint(partners_bp, url_prefix="/partners")
    
    from api.factors.router import bp as factors_bp
    app.register_blueprint(factors_bp, url_prefix="/factors")

    from api.products.router import bp as products_bp
    app.register_blueprint(products_bp, url_prefix="/products")
    
    from api.main.router import bp as base_bp 
    app.register_blueprint(base_bp, url_prefix="/")
    
    
    return app
