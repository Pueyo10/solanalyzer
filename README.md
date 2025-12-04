# 🔍 SolAnalyzer

<div align="center">

![SolAnalyzer](static/img/logo.png)

**A powerful Solana wallet analyzer for tracking P&L (Profit & Loss)**

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/Flask-3.0-green.svg)](https://flask.palletsprojects.com/)
[![Solana](https://img.shields.io/badge/Solana-Mainnet-purple.svg)](https://solana.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[English](#english) | [Español](#español)

</div>

---

## English

### 📋 Description

SolAnalyzer is a web application that allows you to analyze Solana blockchain wallets to track their trading performance. It provides detailed insights into:

- **Buy/Sell transactions** for each token
- **Profit/Loss calculations** in SOL and USD
- **Transaction history** with dates and fees
- **Direct links** to DexScreener for each token

### ✨ Features

- 🔐 **User Authentication** - Secure login and registration system
- 💳 **Subscription System** - Pay with Phantom Wallet (0.5 SOL/month)
- 📊 **Detailed Analytics** - Complete P&L breakdown per token
- 🔗 **Referral Program** - Earn 10% from referred users' subscriptions
- ⚡ **Async Processing** - Uses Celery for background task processing
- 🔄 **Real-time Updates** - WebSocket support for live notifications

### 🛠️ Tech Stack

- **Backend**: Flask, SQLAlchemy, Flask-Login, Flask-SocketIO
- **Task Queue**: Celery with RabbitMQ
- **Cache/Store**: Redis
- **Database**: SQLite
- **Blockchain**: Solana Web3.js, Solders
- **Frontend**: Jinja2 Templates, CSS3, JavaScript

### 📦 Installation

#### Option 1: Using Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/SolAnalyzer.git
cd SolAnalyzer

# Copy environment file
cp .env.example .env

# Edit .env with your configuration
nano .env

# Start with Docker Compose
docker-compose up --build
```

#### Option 2: Manual Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/SolAnalyzer.git
cd SolAnalyzer

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env

# Edit .env with your configuration
nano .env

# Install RabbitMQ and Redis (see Prerequisites)

# Run database migrations
flask db upgrade

# Start Redis server
redis-server

# Start RabbitMQ server
rabbitmq-server

# Start Celery worker (new terminal)
celery -A GraficoUltimaVersion worker --loglevel=info -c 4

# Start Flask application
python analyzer.py
```

### 📋 Prerequisites

- **Python 3.12+**
- **Redis** - For caching and Celery backend
- **RabbitMQ** - For Celery message broker
- **Solana RPC Endpoints** - Get free endpoints from [QuickNode](https://quicknode.com), [Helius](https://helius.dev), or [Alchemy](https://alchemy.com)

### ⚙️ Configuration

Create a `.env` file based on `.env.example`:

```env
SECRET_KEY=your-secret-key
SQLALCHEMY_DATABASE_URI=sqlite:///data/db/solanalyzer.db
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
CELERY_RESULT_BACKEND=redis://:password@localhost:6379/0
# ... see .env.example for all options
```

### 🚀 Usage

1. Open your browser and navigate to `http://localhost:5000`
2. Register a new account or login
3. Subscribe to access the analyzer
4. Enter a Solana wallet address and days to analyze
5. View detailed P&L results

### 📁 Project Structure

```
SolAnalyzer/
├── analyzer.py              # Main Flask application
├── GraficoUltimaVersion.py  # Celery tasks for wallet analysis
├── celery_config.py         # Celery configuration
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Docker Compose configuration
├── Dockerfile               # Docker image definition
├── static/                  # Static files (CSS, images)
├── templates/               # Jinja2 HTML templates
└── data/                    # Database and Redis data
```

### 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Español

### 📋 Descripción

SolAnalyzer es una aplicación web que permite analizar wallets de la blockchain de Solana para rastrear su rendimiento en trading. Proporciona información detallada sobre:

- **Transacciones de compra/venta** para cada token
- **Cálculos de ganancias/pérdidas** en SOL y USD
- **Historial de transacciones** con fechas y comisiones
- **Enlaces directos** a DexScreener para cada token

### ✨ Características

- 🔐 **Autenticación de Usuarios** - Sistema seguro de login y registro
- 💳 **Sistema de Suscripción** - Paga con Phantom Wallet (0.5 SOL/mes)
- 📊 **Análisis Detallado** - Desglose completo de P&L por token
- 🔗 **Programa de Referidos** - Gana 10% de las suscripciones de usuarios referidos
- ⚡ **Procesamiento Asíncrono** - Usa Celery para tareas en segundo plano
- 🔄 **Actualizaciones en Tiempo Real** - Soporte WebSocket para notificaciones en vivo

### 🛠️ Stack Tecnológico

- **Backend**: Flask, SQLAlchemy, Flask-Login, Flask-SocketIO
- **Cola de Tareas**: Celery con RabbitMQ
- **Cache/Almacén**: Redis
- **Base de Datos**: SQLite
- **Blockchain**: Solana Web3.js, Solders
- **Frontend**: Plantillas Jinja2, CSS3, JavaScript

### 📦 Instalación

#### Opción 1: Usando Docker (Recomendado)

```bash
# Clonar el repositorio
git clone https://github.com/yourusername/SolAnalyzer.git
cd SolAnalyzer

# Copiar archivo de entorno
cp .env.example .env

# Editar .env con tu configuración
nano .env

# Iniciar con Docker Compose
docker-compose up --build
```

#### Opción 2: Instalación Manual

```bash
# Clonar el repositorio
git clone https://github.com/yourusername/SolAnalyzer.git
cd SolAnalyzer

# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Copiar archivo de entorno
cp .env.example .env

# Editar .env con tu configuración
nano .env

# Instalar RabbitMQ y Redis (ver Prerrequisitos)

# Ejecutar migraciones de base de datos
flask db upgrade

# Iniciar servidor Redis
redis-server

# Iniciar servidor RabbitMQ
rabbitmq-server

# Iniciar worker de Celery (nueva terminal)
celery -A GraficoUltimaVersion worker --loglevel=info -c 4

# Iniciar aplicación Flask
python analyzer.py
```

### 📋 Prerrequisitos

- **Python 3.12+**
- **Redis** - Para caché y backend de Celery
- **RabbitMQ** - Para broker de mensajes de Celery
- **Endpoints RPC de Solana** - Obtén endpoints gratuitos de [QuickNode](https://quicknode.com), [Helius](https://helius.dev), o [Alchemy](https://alchemy.com)

### ⚙️ Configuración

Crea un archivo `.env` basado en `.env.example`:

```env
SECRET_KEY=tu-clave-secreta
SQLALCHEMY_DATABASE_URI=sqlite:///data/db/solanalyzer.db
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
CELERY_RESULT_BACKEND=redis://:password@localhost:6379/0
# ... ver .env.example para todas las opciones
```

### 🚀 Uso

1. Abre tu navegador y navega a `http://localhost:5000`
2. Regístrate con una nueva cuenta o inicia sesión
3. Suscríbete para acceder al analizador
4. Ingresa una dirección de wallet de Solana y los días a analizar
5. Visualiza los resultados detallados de P&L

### 📁 Estructura del Proyecto

```
SolAnalyzer/
├── analyzer.py              # Aplicación principal Flask
├── GraficoUltimaVersion.py  # Tareas Celery para análisis de wallets
├── celery_config.py         # Configuración de Celery
├── requirements.txt         # Dependencias de Python
├── docker-compose.yml       # Configuración de Docker Compose
├── Dockerfile               # Definición de imagen Docker
├── static/                  # Archivos estáticos (CSS, imágenes)
├── templates/               # Plantillas HTML Jinja2
└── data/                    # Datos de base de datos y Redis
```

### 🤝 Contribuciones

¡Las contribuciones son bienvenidas! No dudes en enviar un Pull Request.

### 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

<div align="center">

**Made with ❤️ for the Solana community**

[⬆ Back to top](#-solanalyzer)

</div>

