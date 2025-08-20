from flask import Flask, render_template, jsonify
import requests
import os
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Abacus AI configuration
ABACUS_API_KEY = os.environ.get('ABACUS_API_KEY')
FEATURE_GROUP_ID = 'c1c94c2da'
DATASET_ID = '158d3193e8'
ABACUS_BASE_URL = 'https://cloud.abacus.ai/api/v0'

def get_abacus_data():
    """Fetch data from Abacus AI dataset"""
    try:
        headers = {
            'Authorization': f'Bearer {ABACUS_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Try to get dataset info first
        dataset_url = f'{ABACUS_BASE_URL}/datasets/{DATASET_ID}'
        response = requests.get(dataset_url, headers=headers)
        
        if response.status_code == 200:
            dataset_info = response.json()
            
            # Try to get the actual data - this endpoint might vary based on Abacus AI's API
            # You may need to adjust this based on their actual API documentation
            data_url = f'{ABACUS_BASE_URL}/datasets/{DATASET_ID}/data'
            data_response = requests.get(data_url, headers=headers)
            
            if data_response.status_code == 200:
                return {
                    'success': True,
                    'dataset_info': dataset_info,
                    'data': data_response.json(),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'success': False,
                    'error': f'Failed to fetch data: {data_response.status_code}',
                    'dataset_info': dataset_info
                }
        else:
            return {
                'success': False,
                'error': f'Failed to fetch dataset info: {response.status_code}'
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': f'Error connecting to Abacus AI: {str(e)}'
        }

@app.route('/')
def index():
    """Main page displaying the dataset"""
    data_result = get_abacus_data()
    return render_template('index.html', data_result=data_result)

@app.route('/api/data')
def api_data():
    """API endpoint to get raw data as JSON"""
    return jsonify(get_abacus_data())

@app.route('/health')
def health_check():
    """Health check endpoint for deployment"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'feature_group_id': FEATURE_GROUP_ID,
        'dataset_id': DATASET_ID
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
