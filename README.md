# MongoDB & DataStax HCD Compatibility Demo

A comprehensive web application demonstrating seamless database portability between **MongoDB Atlas** and **DataStax HCD (Hyper Converged Database)** using their compatible Data APIs.

![Application Screenshot](https://img.shields.io/badge/Status-Production%20Ready-green)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green)
![DataStax](https://img.shields.io/badge/DataStax-HCD-blue)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-2.3+-red)

## 🎯 Demo Purpose

This application showcases how applications built for MongoDB can seamlessly migrate to DataStax HCD without any code changes, demonstrating true database portability through compatible Data APIs.

## ✨ Features

- **🔄 Live Database Switching**: Toggle between MongoDB and HCD instantly via UI toggle
- **👥 User Management**: Create, view, and delete user profiles
- **🎨 Modern UI**: Responsive Bootstrap interface with real-time database status
- **🔗 Consistent Schema**: UUID-based document structure across both databases
- **⚡ Real-time Operations**: Instant CRUD operations with confirmation dialogs
- **📊 Database Monitoring**: Visual indicators showing active database type
- **🚀 Zero Downtime Switching**: Change databases without restarting the application

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Interface │────│  Database Layer  │────│   MongoDB OR    │
│   (Flask + UI)  │    │   (Abstraction)  │    │   DataStax HCD  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- MongoDB Atlas account OR DataStax HCD instance
- Git

### 1. Clone Repository
```bash
git clone https://github.com/shiragannavar/MongoDB-DataStax-Compatibility-Demo.git
cd MongoDB-DataStax-Compatibility-Demo
```

### 2. Setup Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Database
Copy `.env.example` to `.env` and configure your database:

```bash
cp .env.example .env
```

#### For MongoDB Atlas:
```env
DATABASE_TYPE=mongodb
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
MONGODB_DATABASE=user_profiles
```

#### For DataStax HCD:
```env
DATABASE_TYPE=hcd
HCD_API_ENDPOINT=http://<your-hcd-endpoint>:8181
HCD_USERNAME=<your_username>
HCD_PASSWORD=<your_password>
HCD_KEYSPACE=default_keyspace
```

### 4. Run Application
```bash
python app.py
```

Visit `http://localhost:5001` to see the application in action!

## 🔄 Live Database Switching Demo

### Real-time Database Toggle
1. **Visit the application** at `http://localhost:5001`
2. **Locate the toggle switch** in the Database Status card
3. **Toggle left (unchecked)**: MongoDB (green badge)
4. **Toggle right (checked)**: HCD (blue badge)
5. **Watch the magic**: Instant database switching without restart!

### Traditional Configuration Method
Alternatively, you can still switch databases via configuration:
1. Update `.env`: Change `DATABASE_TYPE=mongodb` or `DATABASE_TYPE=hcd`
2. Restart the application
3. Verify the database badge updates accordingly

### Verify Compatibility
- **Live switching**: Toggle between databases in real-time
- **Same interface**: Identical functionality regardless of database
- **Consistent data**: UUID-based schema works across both databases
- **Zero code changes**: Same application logic for both databases

## 📋 API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard with user list |
| `/create_user` | GET/POST | User creation form and handler |
| `/delete_user/<id>` | POST | Delete user by UUID |
| `/api/users` | GET | JSON API - Get all users |
| `/api/db_info` | GET | Current database connection info |
| `/api/switch_database` | POST | Switch between MongoDB and HCD |

## 🏗️ Project Structure

```
├── app.py                 # Flask web application
├── database.py            # Database abstraction layer
├── requirements.txt       # Python dependencies
├── .env.example          # Environment template
├── README.md             # This file
├── templates/            # Jinja2 HTML templates
│   ├── base.html        # Base layout
│   ├── index.html       # User dashboard
│   └── create_user.html # User creation form
└── static/              # Frontend assets
    ├── css/style.css    # Custom styles
    └── js/main.js       # JavaScript functionality
```

## 🔧 Technical Implementation

### Database Abstraction Layer
The `DatabaseManager` class in `database.py` provides a unified interface:

```python
class DatabaseManager:
    def __init__(self):
        self.db_type = os.getenv('DATABASE_TYPE', 'mongodb')
        self._setup_connection()
    
    def create_user(self, user_data):
        # Works with both MongoDB and HCD
    
    def get_all_users(self):
        # Consistent across databases
    
    def delete_user(self, user_id):
        # Same logic, different clients
```

### Consistent Schema
Both databases use identical document structure:
```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30,
  "city": "New York",
  "created_at": "2025-09-03T08:35:00.000Z"
}
```

## 🎯 Key Demo Points

1. **Live Database Switching**: Toggle between databases without restarting
2. **Zero Code Changes**: Same application logic works with both databases
3. **Visual Confirmation**: UI clearly shows which database is active
4. **Identical Functionality**: All features work exactly the same way
5. **Schema Consistency**: UUID-based documents ensure portability
6. **Production Ready**: Error handling, validation, and modern UI

## 🛠️ Technologies Used

- **Backend**: Python 3.10, Flask 2.3
- **Database Clients**: PyMongo (MongoDB), AstraPy (HCD)
- **Frontend**: Bootstrap 5, JavaScript ES6
- **Database**: MongoDB Atlas, DataStax HCD
- **Deployment**: Environment-based configuration

## 📝 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_TYPE` | Database type (`mongodb` or `hcd`) | `mongodb` |
| `MONGODB_URI` | MongoDB connection string | `mongodb+srv://<username>:<password>@<cluster>.mongodb.net/` |
| `MONGODB_DATABASE` | MongoDB database name | `user_profiles` |
| `HCD_API_ENDPOINT` | HCD Data API endpoint | `http://localhost:8181` |
| `HCD_USERNAME` | HCD username | `<your_username>` |
| `HCD_PASSWORD` | HCD password | `<your_password>` |
| `HCD_KEYSPACE` | HCD keyspace name | `default_keyspace` |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit changes: `git commit -am 'Add feature'`
4. Push to branch: `git push origin feature-name`
5. Submit a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Links

- [MongoDB Atlas](https://www.mongodb.com/atlas)
- [DataStax HCD Documentation](https://docs.datastax.com/en/astra-db-serverless/)
- [Data API Comparison Guide](https://docs.datastax.com/en/astra-db-se---

**Built with ❤️ to demonstrate the power of database portability**
