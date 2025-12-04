"""
SolAnalyzer - Main Flask Application
====================================
Web application for analyzing Solana wallet trading performance.
"""

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from celery.result import AsyncResult
from dotenv import load_dotenv
from flask import (
    Flask, flash, jsonify, redirect, render_template, 
    request, session, url_for
)
from flask_login import (
    LoginManager, UserMixin, current_user, 
    login_required, login_user, logout_user
)
from flask_mail import Mail, Message
from flask_migrate import Migrate
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from itsdangerous import SignatureExpired, URLSafeTimedSerializer
from solana.rpc.api import Client
from solana.transaction import Signature
from werkzeug.security import check_password_hash, generate_password_hash
from wtforms import PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

from celery_config import redis_client
from GraficoUltimaVersion import analizar_wallet, endpoint_manager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Flask Application Factory
# =============================================================================

db = SQLAlchemy()


def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Core configuration
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
        'SQLALCHEMY_DATABASE_URI', 
        'sqlite:///data/db/solanalyzer.db'
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    
    # Mail configuration
    app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
    app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
    app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL', 'False') == 'True'
    app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
    app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')
    
    # Initialize extensions
    db.init_app(app)
    
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'login'
    
    Migrate(app, db)
    
    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))
    
    with app.app_context():
        db.create_all()
        logger.info("Database tables created")
    
    return app


# Create application instance
app = create_app()
socketio = SocketIO(app)
mail = Mail(app)

# Serializer for password reset tokens
s = URLSafeTimedSerializer(app.config['SECRET_KEY'])

# Solana connection
connection = Client(os.getenv('SOLANA_CLIENT_URL', 'https://api.mainnet-beta.solana.com'))


# =============================================================================
# Models
# =============================================================================

class User(UserMixin, db.Model):
    """User model for authentication and subscription management."""
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    last_activity = db.Column(db.DateTime, nullable=True)
    session_id = db.Column(db.String(128), nullable=True)
    subscription_expiration = db.Column(db.DateTime, nullable=True)
    referral_code = db.Column(db.String(64), unique=True, nullable=True)
    referred_by = db.Column(db.String(64), nullable=True)
    referral_count = db.Column(db.Integer, default=0)
    referral_fee = db.Column(db.Float, default=0.0)
    wallet_address = db.Column(db.String(128), nullable=True)
    
    def set_password(self, password):
        """Hash and set user password."""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Verify password against hash."""
        return check_password_hash(self.password_hash, password)


# =============================================================================
# Forms
# =============================================================================

class RegistrationForm(FlaskForm):
    """User registration form."""
    username = StringField('Username', validators=[DataRequired(), Length(min=2, max=20)])
    email = StringField('Email', validators=[DataRequired(), Email()])
    confirm_email = StringField('Confirm Email', validators=[
        DataRequired(), Email(), EqualTo('email', message='Emails must match')
    ])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)])
    confirm_password = PasswordField('Confirm Password', validators=[
        DataRequired(), EqualTo('password')
    ])
    submit = SubmitField('Sign Up')


class LoginForm(FlaskForm):
    """User login form."""
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')


# =============================================================================
# Authentication Routes
# =============================================================================

@app.route("/register", methods=['GET', 'POST'])
def register():
    """Handle user registration."""
    form = RegistrationForm()
    
    if form.validate_on_submit():
        username_exists = User.query.filter_by(username=form.username.data).first()
        email_exists = User.query.filter_by(email=form.email.data).first()
        
        if username_exists:
            flash('This username already exists, choose another.', 'danger')
        elif email_exists:
            flash('This email already exists, choose another.', 'danger')
        else:
            referral_code = str(uuid.uuid4())[:8]
            referred_by = request.args.get('ref')
            
            user = User(
                username=form.username.data,
                email=form.email.data,
                referral_code=referral_code,
                referred_by=referred_by
            )
            user.set_password(form.password.data)
            
            db.session.add(user)
            db.session.commit()
            
            # Update referrer's count
            if referred_by:
                referrer = User.query.filter_by(referral_code=referred_by).first()
                if referrer:
                    referrer.referral_count += 1
                    db.session.commit()
            
            flash('Your account has been created! You are now able to log in', 'success')
            return redirect(url_for('login'))
    
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    """Handle user login."""
    form = LoginForm()
    
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        
        if user and user.check_password(form.password.data):
            # Force logout previous session if exists
            if user.session_id:
                socketio.emit(
                    'force_logout',
                    {'message': 'You have been logged out due to another login.'},
                    room=user.session_id
                )
            
            logout_user()
            login_user(user)
            
            user.last_activity = datetime.now(timezone.utc)
            user.session_id = str(uuid.uuid4())
            db.session.commit()
            
            session['session_id'] = user.session_id
            
            flash('Login Successful!', 'success')
            return redirect(url_for('index_view'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    
    return render_template('login.html', title='Login', form=form)


@app.route('/logout', methods=['POST'])
@login_required
def logout():
    """Handle user logout."""
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))


# =============================================================================
# Password Reset Routes
# =============================================================================

@app.route('/reset_password_request', methods=['GET', 'POST'])
def reset_password_request():
    """Handle password reset request."""
    if request.method == 'POST':
        email = request.form['email']
        user = User.query.filter_by(email=email).first()
        
        if user:
            token = s.dumps(email, salt='password-reset-salt')
            reset_url = url_for('reset_password', token=token, _external=True)
            
            msg = Message('Password Reset Request', recipients=[email])
            msg.body = f'''Hey {user.username},

This is the link to reset your password: {reset_url}

SolAnalyzer Team.
'''
            mail.send(msg)
            flash('A password reset link has been sent to your email address.', 'info')
            return redirect(url_for('login'))
        else:
            flash('Email address not found.', 'danger')
    
    return render_template('reset_password_request.html')


@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """Handle password reset with token."""
    try:
        email = s.loads(token, salt='password-reset-salt', max_age=3600)
    except SignatureExpired:
        flash('The password reset link has expired.', 'danger')
        return redirect(url_for('reset_password_request'))
    
    if request.method == 'POST':
        password = request.form['password']
        confirm_password = request.form['confirm_password']
        
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return redirect(url_for('reset_password', token=token))
        
        user = User.query.filter_by(email=email).first()
        if user:
            user.set_password(password)
            db.session.commit()
            flash('Your password has been updated.', 'success')
            return redirect(url_for('login'))
        else:
            flash('User not found.', 'danger')
            return redirect(url_for('reset_password_request'))
    
    return render_template('reset_password.html', token=token)


# =============================================================================
# Main Application Routes
# =============================================================================

@app.route('/')
def home():
    """Home page route."""
    if current_user.is_authenticated:
        return redirect(url_for('index_view'))
    return render_template('home.html', title='Welcome to SolAnalyzer')


@app.route('/index')
@login_required
def index_view():
    """Main dashboard view."""
    now = datetime.now(timezone.utc)
    subscription_expiration = current_user.subscription_expiration
    time_left = None
    
    referral_link = url_for('register', ref=current_user.referral_code, _external=True)
    referrals = User.query.filter_by(referred_by=current_user.referral_code).all()
    
    if subscription_expiration:
        subscription_expiration = subscription_expiration.replace(tzinfo=timezone.utc)
        time_left = subscription_expiration - now
    
    return render_template(
        'index.html',
        now=now,
        time_left=time_left,
        username=current_user.username,
        subscription_expiration=subscription_expiration,
        referral_link=referral_link,
        referral_count=current_user.referral_count,
        referral_fee=current_user.referral_fee,
        withdraw_wallet=current_user.wallet_address,
        referrals=referrals
    )


# =============================================================================
# Analysis Routes
# =============================================================================

@app.route('/submit-task', methods=['POST'])
@login_required
def submit_task():
    """Submit a wallet analysis task."""
    data = request.get_json()
    wallet = data['wallet']
    days_to_analyze = data['days_to_analyze']
    
    try:
        task = analizar_wallet.delay(wallet, int(days_to_analyze))
        return jsonify({'task_id': task.id}), 202
    except Exception as e:
        logger.error(f"Error submitting task: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/task-status/<task_id>', methods=['GET'])
@login_required
def task_status(task_id):
    """Get status of an analysis task."""
    task_result = AsyncResult(task_id, app=analizar_wallet)
    
    if task_result.ready():
        return jsonify({
            'state': task_result.state,
            'result': task_result.get()
        })
    else:
        return jsonify({'state': task_result.state})


@app.route('/release-endpoint', methods=['POST'])
def release_endpoint():
    """Release an endpoint when task is cancelled."""
    import json
    
    data = request.get_data(as_text=True)
    json_data = json.loads(data)
    task_id = json_data.get('task_id')
    
    logger.debug(f"Release endpoint request for Task ID: {task_id}")
    
    if not task_id:
        return jsonify({'status': 'error', 'message': 'Task ID is required'}), 400
    
    task = AsyncResult(task_id, app=analizar_wallet)
    
    if not task:
        return jsonify({'status': 'error', 'message': 'Task not found'}), 404
    
    # Mark task as revoked in Redis
    redis_client.set(f"task_revoked:{task_id}", 1)
    logger.debug(f"Task {task_id} marked as revoked")
    
    # Release endpoint if available
    if task.info and isinstance(task.info, dict):
        endpoint_url = task.info.get('endpoint_url')
        if endpoint_url:
            endpoint_manager.release_endpoint(endpoint_url)
            logger.debug(f"Endpoint released: {endpoint_url}")
    
    return jsonify({'status': 'success', 'message': 'Endpoint released successfully'})


# =============================================================================
# Wallet Routes
# =============================================================================

@app.route('/connect_wallet', methods=['POST'])
def connect_wallet():
    """Connect Phantom wallet."""
    data = request.get_json()
    phantom_wallet_address = data.get('publicKey')
    
    if current_user.is_authenticated:
        session['phantom_wallet_address'] = phantom_wallet_address
    
    session['wallet_addressphantom'] = phantom_wallet_address
    
    return jsonify({
        'status': 'success',
        'message': 'Wallet connected successfully.',
        'wallet_addressphantom': phantom_wallet_address
    })


@app.route('/disconnect_wallet', methods=['POST'])
@login_required
def disconnect_wallet():
    """Disconnect Phantom wallet."""
    session.pop('wallet_addressphantom', None)
    return jsonify({'status': 'success', 'message': 'Wallet disconnected successfully.'})


@app.route('/save_withdraw_wallet', methods=['POST'])
@login_required
def save_withdraw_wallet():
    """Save user's withdrawal wallet address."""
    data = request.get_json()
    wallet = data.get('wallet')
    
    if not wallet:
        return jsonify({'status': 'error', 'message': 'Wallet address is required'}), 400
    
    current_user.wallet_address = wallet
    db.session.commit()
    
    return jsonify({'status': 'success', 'message': 'Withdrawal wallet saved successfully'})


# =============================================================================
# Subscription Routes
# =============================================================================

@app.route('/update_subscription', methods=['POST'])
@login_required
def update_subscription():
    """Update user subscription after payment verification."""
    import time
    
    data = request.get_json()
    signature_str = data.get('signature')
    
    if not signature_str:
        return jsonify({'status': 'error', 'message': 'Signature is required'}), 400
    
    try:
        # Initial delay for transaction confirmation
        time.sleep(15)
        
        signature = Signature.from_string(signature_str)
        logger.info(f"Verifying signature: {signature}")
        
        max_retries = 10
        retry_delay = 6
        
        for attempt in range(max_retries):
            response = connection.get_transaction(
                signature, "jsonParsed", max_supported_transaction_version=0
            )
            result = response.value
            
            logger.info(f"Attempt {attempt + 1}: Transaction result: {result is not None}")
            
            if result:
                break
            
            logger.info(f"Transaction not found. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        
        if not result:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found or not confirmed'
            }), 400
        
        # Update subscription
        current_user.subscription_expiration = datetime.now(timezone.utc) + timedelta(days=30)
        db.session.commit()
        
        # Add referral fee if applicable
        if current_user.referred_by:
            referrer = User.query.filter_by(referral_code=current_user.referred_by).first()
            if referrer:
                referrer.referral_fee += 0.1 * 0.5  # 10% of 0.5 SOL
                db.session.commit()
        
        return jsonify({'status': 'success', 'message': 'Subscription updated successfully'})
    
    except Exception as e:
        logger.error(f"Error updating subscription: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/pay_fee', methods=['POST'])
def pay_fee():
    """Process referral fee payment."""
    import time
    
    data = request.get_json()
    user_id = data.get('user_id')
    signature_str = data.get('signature')
    
    if not user_id or not signature_str:
        return jsonify({
            'status': 'error',
            'message': 'User ID and signature are required'
        }), 400
    
    try:
        time.sleep(15)
        
        signature = Signature.from_string(signature_str)
        
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            response = connection.get_transaction(
                signature, "jsonParsed", max_supported_transaction_version=0
            )
            result = response.value
            
            if result:
                break
            
            time.sleep(retry_delay)
        
        if not result:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found or not confirmed'
            }), 400
        
        user = User.query.get(user_id)
        if user:
            user.referral_fee = 0.0
            db.session.commit()
            return jsonify({'status': 'success', 'message': 'Fee paid successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'User not found'}), 404
    
    except Exception as e:
        logger.error(f"Error processing fee payment: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# =============================================================================
# Admin Routes
# =============================================================================

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    """Admin login page."""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        admin_username = os.getenv('ADMIN_USERNAME')
        admin_password_hash = os.getenv('ADMIN_PASSWORD_HASH')
        
        if username == admin_username and check_password_hash(admin_password_hash, password):
            session['admin_logged_in'] = True
            return redirect(url_for('admin_panel'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'danger')
    
    return render_template('admin_login.html')


@app.route('/admin/logout')
def admin_logout():
    """Admin logout."""
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


@app.route('/admin/panel')
def admin_panel():
    """Admin panel."""
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))
    
    users = User.query.all()
    return render_template('admin_panel.html', users=users)


@app.route('/admin/update_subscription', methods=['POST'])
def admin_update_subscription():
    """Admin: Update user subscription."""
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))
    
    user_id = request.form.get('user_id')
    days = request.form.get('days')
    
    user = User.query.get(user_id)
    if user:
        user.subscription_expiration = datetime.now(timezone.utc) + timedelta(days=int(days))
        db.session.commit()
    else:
        flash('User not found', 'danger')
    
    return redirect(url_for('admin_panel'))


@app.route('/admin/delete_user', methods=['POST'])
def admin_delete_user():
    """Admin: Delete user."""
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))
    
    user_id = request.form.get('user_id')
    user = User.query.get(user_id)
    
    if user:
        db.session.delete(user)
        db.session.commit()
    else:
        flash('User not found', 'danger')
    
    return redirect(url_for('admin_panel'))


@app.route('/admin/update_referral_fee', methods=['POST'])
def admin_update_referral_fee():
    """Admin: Update user referral fee."""
    if not session.get('admin_logged_in'):
        return redirect(url_for('admin_login'))
    
    user_id = request.form.get('user_id')
    referral_fee = request.form.get('referral_fee')
    
    user = User.query.get(user_id)
    if user:
        user.referral_fee = float(referral_fee)
        db.session.commit()
    else:
        flash('User not found.', 'danger')
    
    return redirect(url_for('admin_panel'))


# =============================================================================
# Request Hooks
# =============================================================================

@app.before_request
def clear_wallet_session():
    """Clear phantom wallet from session on each request."""
    if 'wallet_addressphantom' in session:
        session.pop('wallet_addressphantom', None)


@app.before_request
def update_last_activity():
    """Update user's last activity timestamp."""
    if current_user.is_authenticated:
        current_user.last_activity = datetime.now(timezone.utc)
        db.session.commit()


@app.before_request
def limit_active_sessions():
    """Limit users to single active session."""
    if current_user.is_authenticated:
        now = datetime.now(timezone.utc)
        
        if current_user.last_activity:
            last_activity_aware = current_user.last_activity.replace(tzinfo=timezone.utc)
        else:
            last_activity_aware = now
        
        # Check for inactive session (30 minutes)
        if last_activity_aware < now - timedelta(minutes=30):
            logout_user()
            flash('Your session has expired due to inactivity.', 'info')
            return redirect(url_for('login'))
        
        # Check for session hijacking
        if 'session_id' in session and session['session_id'] != current_user.session_id:
            socketio.emit(
                'force_logout',
                {'message': 'You have been logged out due to another login.'},
                room=session['session_id']
            )
            logout_user()
            flash('You have been logged out due to another login.', 'info')
            return redirect(url_for('login'))


# =============================================================================
# WebSocket Events
# =============================================================================

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection."""
    if current_user.is_authenticated:
        session_id = session.get('session_id')
        if session_id:
            join_room(session_id)


@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection."""
    if current_user.is_authenticated:
        session_id = session.get('session_id')
        if session_id:
            leave_room(session_id)


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', debug=True)
