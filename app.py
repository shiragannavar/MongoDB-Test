from flask import Flask, render_template, request, jsonify, redirect, url_for
from database import DatabaseManager
from telecom_data_handler import TelecomDataHandler, create_sample_subscriber
import uuid
import os
from datetime import datetime

app = Flask(__name__)
db_manager = DatabaseManager()
telecom_handler = TelecomDataHandler()

@app.route('/')
def index():
    """Main page showing subscribers"""
    try:
        # Create fresh instances to get current database type
        current_db_manager = DatabaseManager()
        current_telecom_handler = TelecomDataHandler()
        
        subscribers = current_telecom_handler.get_all_subscribers(limit=100)
        subscriber_stats = current_telecom_handler.get_database_stats()
        db_info = current_db_manager.get_database_info()
        return render_template('index.html', 
                             subscribers=subscribers,
                             subscriber_stats=subscriber_stats,
                             db_info=db_info)
    except Exception as e:
        return render_template('index.html', 
                             subscribers=[],
                             subscriber_stats={},
                             db_info={'type': 'error', 'status': str(e)})

@app.route('/api/db_info')
def api_db_info():
    """API endpoint to get database info"""
    try:
        current_db_manager = DatabaseManager()
        db_info = current_db_manager.get_database_info()
        return jsonify(db_info)
    except Exception as e:
        return jsonify({'type': 'error', 'status': str(e)})

@app.route('/api/switch_database', methods=['POST'])
def switch_database():
    """Switch between MongoDB and HCD databases"""
    try:
        data = request.get_json()
        new_db_type = data.get('database_type')
        
        if new_db_type not in ['mongodb', 'hcd']:
            return jsonify({'success': False, 'message': 'Invalid database type'})
        
        # Update .env file
        env_path = '.env'
        env_vars = {}
        
        # Read existing .env file
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        
        # Update DATABASE_TYPE
        env_vars['DATABASE_TYPE'] = new_db_type
        
        # Write back to .env file
        with open(env_path, 'w') as f:
            for key, value in env_vars.items():
                f.write(f'{key}={value}\n')
        
        # Update environment variable for current session
        os.environ['DATABASE_TYPE'] = new_db_type
        
        # Reinitialize database manager
        global db_manager
        db_manager = DatabaseManager()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully switched to {new_db_type.upper()}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/sync_subscribers_to_hcd', methods=['POST'])
def sync_subscribers_to_hcd():
    """Sync all MongoDB subscriber records to DataStax HCD"""
    try:
        current_db_manager = DatabaseManager()
        result = current_db_manager.sync_subscribers_to_hcd()
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'message': f'Subscriber sync failed: {str(e)}'})

# Telecom Subscriber API Endpoints
@app.route('/api/subscribers')
def api_subscribers():
    """Get all subscribers"""
    try:
        current_telecom_handler = TelecomDataHandler()
        limit = request.args.get('limit', 100, type=int)
        subscribers = current_telecom_handler.get_all_subscribers(limit=limit)
        return jsonify({'success': True, 'subscribers': subscribers, 'count': len(subscribers)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/stats')
def api_subscriber_stats():
    """Get subscriber statistics"""
    try:
        current_telecom_handler = TelecomDataHandler()
        stats = current_telecom_handler.get_database_stats()
        return jsonify({'success': True, 'stats': stats})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/active')
def api_active_subscribers():
    """Get all active subscribers"""
    try:
        subscribers = telecom_handler.get_active_subscribers()
        return jsonify({'success': True, 'subscribers': subscribers, 'count': len(subscribers)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/provider/<provider>')
def api_subscribers_by_provider(provider):
    """Get subscribers by provider"""
    try:
        subscribers = telecom_handler.find_subscribers_by_provider(provider)
        return jsonify({'success': True, 'subscribers': subscribers, 'count': len(subscribers)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/<hash_msisdn>')
def api_subscriber_details(hash_msisdn):
    """Get subscriber details by hash MSISDN"""
    try:
        subscriber = telecom_handler.find_subscriber_by_hash(hash_msisdn)
        if subscriber:
            return jsonify({'success': True, 'subscriber': subscriber})
        else:
            return jsonify({'success': False, 'message': 'Subscriber not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/<hash_msisdn>/products')
def api_subscriber_products(hash_msisdn):
    """Get subscriber products"""
    try:
        products = telecom_handler.get_subscriber_products(hash_msisdn)
        return jsonify({'success': True, 'products': products, 'count': len(products)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/<hash_msisdn>/services')
def api_subscriber_services(hash_msisdn):
    """Get subscriber services"""
    try:
        services = telecom_handler.get_subscriber_services(hash_msisdn)
        return jsonify({'success': True, 'services': services, 'count': len(services)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers', methods=['POST'])
def api_create_subscriber():
    """Create a new subscriber"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400
        
        subscriber = telecom_handler.insert_subscriber(data)
        return jsonify({'success': True, 'subscriber': subscriber}), 201
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/subscribers/sample', methods=['POST'])
def api_create_sample_subscriber():
    """Create a sample subscriber for testing"""
    try:
        sample_data = create_sample_subscriber()
        subscriber = telecom_handler.insert_subscriber(sample_data)
        return jsonify({'success': True, 'subscriber': subscriber}), 201
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/subscribers/<hash_msisdn>', methods=['PUT'])
def api_update_subscriber_status(hash_msisdn):
    """Update subscriber status"""
    try:
        data = request.get_json()
        status = data.get('status')
        if not status:
            return jsonify({'success': False, 'message': 'Status is required'}), 400
        
        success = telecom_handler.update_subscriber_status(hash_msisdn, status)
        if success:
            return jsonify({'success': True, 'message': 'Status updated successfully'})
        else:
            return jsonify({'success': False, 'message': 'Subscriber not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/subscribers/delete/<hash_msisdn>', methods=['DELETE'])
def delete_subscriber_api(hash_msisdn):
    """API endpoint to delete a subscriber"""
    try:
        success = telecom_handler.delete_subscriber(hash_msisdn)
        if success:
            return jsonify({"success": True, "message": "Subscriber deleted successfully"})
        else:
            return jsonify({"success": False, "message": "Subscriber not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

# Telecom Plans API endpoints
@app.route('/api/plans', methods=['GET'])
def get_plans_api():
    """API endpoint to get all telecom plans"""
    try:
        limit = request.args.get('limit', 100, type=int)
        plans = telecom_handler.get_all_plans(limit=limit)
        return jsonify({"success": True, "plans": plans})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/plans/<plan_id>', methods=['GET'])
def get_plan_api(plan_id):
    """API endpoint to get a specific plan"""
    try:
        plan = telecom_handler.get_plan_by_id(plan_id)
        if plan:
            return jsonify({"success": True, "plan": plan})
        else:
            return jsonify({"success": False, "message": "Plan not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/plans', methods=['POST'])
def create_plan_api():
    """API endpoint to create a new plan"""
    try:
        plan_data = request.get_json()
        result = telecom_handler.insert_plan(plan_data)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/plans/<plan_id>/status', methods=['PUT'])
def update_plan_status_api(plan_id):
    """API endpoint to update plan status"""
    try:
        data = request.get_json()
        is_active = data.get('isActive', True)
        success = telecom_handler.update_plan_status(plan_id, is_active)
        if success:
            return jsonify({"success": True, "message": "Plan status updated successfully"})
        else:
            return jsonify({"success": False, "message": "Plan not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/plans/delete/<plan_id>', methods=['DELETE'])
def delete_plan_api(plan_id):
    """API endpoint to delete a plan"""
    try:
        success = telecom_handler.delete_plan(plan_id)
        if success:
            return jsonify({"success": True, "message": "Plan deleted successfully"})
        else:
            return jsonify({"success": False, "message": "Plan not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
