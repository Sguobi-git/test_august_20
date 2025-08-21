from flask import Flask, render_template, jsonify
import requests
import os
from datetime import datetime

app = Flask(__name__)

# Abacus AI configuration
ABACUS_API_KEY = os.environ.get('ABACUS_API_KEY')
FEATURE_GROUP_ID = 'c1c94c2da'
DATASET_ID = '158d3193e8'
ABACUS_BASE_URL = 'https://api.abacus.ai/api/v0'

def get_abacus_data():
    """Fetch data from Abacus AI dataset"""
    try:
        headers = {
            'apiKey': ABACUS_API_KEY,
            'Content-Type': 'application/json'
        }
        
        # Try to get dataset info first
        dataset_url = f'{ABACUS_BASE_URL}/describeDataset'
        params = {'datasetId': DATASET_ID}
        response = requests.get(dataset_url, headers=headers, params=params)
        
        if response.status_code == 200:
            dataset_info = response.json()
            
            # Try to get feature group data
            feature_group_url = f'{ABACUS_BASE_URL}/describeFeatureGroup'
            fg_params = {'featureGroupId': FEATURE_GROUP_ID}
            fg_response = requests.get(feature_group_url, headers=headers, params=fg_params)
            
            result = {
                'success': True,
                'dataset_info': dataset_info,
                'timestamp': datetime.now().isoformat()
            }
            
            if fg_response.status_code == 200:
                result['feature_group_info'] = fg_response.json()
            
            # Try to get some sample data from the feature group
            try:
                sample_url = f'{ABACUS_BASE_URL}/getFeatureGroupVersionLogs'
                sample_params = {'featureGroupId': FEATURE_GROUP_ID}
                sample_response = requests.get(sample_url, headers=headers, params=sample_params)
                if sample_response.status_code == 200:
                    result['sample_data'] = sample_response.json()
            except:
                pass  # Sample data is optional
            
            return result
        else:
            return {
                'success': False,
                'error': f'Failed to fetch dataset info: {response.status_code} - {response.text}',
                'api_response': response.text if response.text else 'No response content'
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
