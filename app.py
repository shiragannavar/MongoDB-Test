from flask import Flask, render_template, request, jsonify, redirect, url_for
from database import DatabaseManager
import uuid
from datetime import datetime

app = Flask(__name__)
db_manager = DatabaseManager()

@app.route('/')
def index():
    """Main page showing all users"""
    try:
        users = db_manager.get_all_users()
        db_info = db_manager.get_database_info()
        return render_template('index.html', users=users, db_info=db_info)
    except Exception as e:
        return render_template('index.html', users=[], db_info={"type": "error", "status": str(e)})

@app.route('/create_user', methods=['GET', 'POST'])
def create_user():
    """Create a new user"""
    if request.method == 'POST':
        try:
            user_data = {
                'name': request.form['name'],
                'email': request.form['email'],
                'age': int(request.form['age']),
                'city': request.form['city'],
                'created_at': datetime.utcnow().isoformat()
            }
            
            created_user = db_manager.create_user(user_data)
            return redirect(url_for('index'))
        except Exception as e:
            return render_template('create_user.html', error=str(e))
    
    return render_template('create_user.html')

@app.route('/delete_user/<user_id>', methods=['POST'])
def delete_user(user_id):
    """Delete a user"""
    try:
        success = db_manager.delete_user(user_id)
        if success:
            return jsonify({'success': True, 'message': 'User deleted successfully'})
        else:
            return jsonify({'success': False, 'message': 'User not found'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/users')
def api_users():
    """API endpoint to get all users"""
    try:
        users = db_manager.get_all_users()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/db_info')
def api_db_info():
    """API endpoint to get database info"""
    try:
        db_info = db_manager.get_database_info()
        return jsonify(db_info)
    except Exception as e:
        return jsonify({'type': 'error', 'status': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
