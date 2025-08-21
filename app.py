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

from flask import Flask, render_template, jsonify
import os
from datetime import datetime

# Import Abacus AI SDK
try:
    from abacusai import ApiClient
    ABACUS_AVAILABLE = True
except ImportError:
    ABACUS_AVAILABLE = False
    print("Warning: abacusai package not installed. Install with: pip install abacusai")

app = Flask(__name__)

# Abacus AI configuration
ABACUS_API_KEY = os.environ.get('ABACUS_API_KEY')
FEATURE_GROUP_ID = 'c1c94c2da'
DATASET_ID = '158d3193e8'

def get_abacus_data():
    """Fetch data from Abacus AI using the official SDK"""
    try:
        # Check if SDK is available
        if not ABACUS_AVAILABLE:
            return {
                'success': False,
                'error': 'Abacus AI SDK not installed',
                'fix': 'Add "abacusai" to your requirements.txt'
            }
        
        # Check if API key is set
        if not ABACUS_API_KEY:
            return {
                'success': False,
                'error': 'ABACUS_API_KEY environment variable is not set',
                'troubleshooting': [
                    'Go to your Abacus AI dashboard',
                    'Generate an API key',
                    'Set it as ABACUS_API_KEY environment variable'
                ]
            }
        
        # Initialize the API client
        client = ApiClient(ABACUS_API_KEY)
        
        # Test API connection with a simple call
        try:
            projects = client.list_projects()
            api_test_passed = True
        except Exception as e:
            if "403" in str(e) or "Forbidden" in str(e):
                return {
                    'success': False,
                    'error': 'API authentication failed (403 Forbidden)',
                    'details': str(e),
                    'troubleshooting': [
                        'Verify your API key is correct',
                        'Check that the API key belongs to the right organization',
                        'Ensure the API key has sufficient permissions',
                        'Try generating a new API key from your Abacus AI dashboard'
                    ]
                }
            else:
                return {
                    'success': False,
                    'error': f'API connection failed: {str(e)}',
                    'troubleshooting': [
                        'Check your internet connection',
                        'Verify the API key is valid',
                        'Try again in a few minutes'
                    ]
                }
        
        # Try multiple data extraction methods based on your working code
        result = {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'api_test': 'PASSED',
            'available_projects': len(projects) if projects else 0
        }
        
        # Method 1: Try to get dataset data directly
        try:
            # Try pandas data extraction (this often works best)
            if hasattr(client, 'get_dataset_data_as_pandas'):
                pandas_data = client.get_dataset_data_as_pandas(DATASET_ID)
                if pandas_data is not None and hasattr(pandas_data, 'shape'):
                    result['dataset_data'] = {
                        'method': 'pandas',
                        'shape': pandas_data.shape,
                        'columns': list(pandas_data.columns) if hasattr(pandas_data, 'columns') else [],
                        'sample_data': pandas_data.head().to_dict('records') if hasattr(pandas_data, 'head') else str(pandas_data)[:500]
                    }
        except Exception as e:
            result['dataset_data_error'] = str(e)
        
        # Method 2: Try to get feature group info
        try:
            feature_group = client.describe_feature_group(FEATURE_GROUP_ID)
            result['feature_group_info'] = {
                'table_name': getattr(feature_group, 'table_name', 'Unknown'),
                'dataset_id': getattr(feature_group, 'dataset_id', 'Unknown'),
                'features': len(getattr(feature_group, 'features', [])),
                'feature_names': [f.name for f in getattr(feature_group, 'features', [])][:10]  # First 10 features
            }
        except Exception as e:
            result['feature_group_error'] = str(e)
        
        # Method 3: Try streaming data
        try:
            if hasattr(client, 'get_recent_feature_group_streamed_data'):
                streaming_data = client.get_recent_feature_group_streamed_data(FEATURE_GROUP_ID)
                result['streaming_data'] = {
                    'available': True,
                    'type': str(type(streaming_data)),
                    'data_preview': str(streaming_data)[:200] if streaming_data else None
                }
        except Exception as e:
            result['streaming_data_error'] = str(e)
        
        # Method 4: List available datasets for reference
        try:
            datasets = client.list_datasets()
            result['available_datasets'] = [
                {
                    'id': d.dataset_id,
                    'name': getattr(d, 'name', 'Unknown'),
                    'source': getattr(d, 'source_type', 'Unknown')
                }
                for d in (datasets[:5] if datasets else [])  # First 5 datasets
            ]
        except Exception as e:
            result['datasets_error'] = str(e)
        
        # Add project information for context
        try:
            result['projects'] = [
                {
                    'id': p.project_id,
                    'name': p.name,
                    'use_case': getattr(p, 'use_case', 'Unknown')
                }
                for p in (projects[:5] if projects else [])  # First 5 projects
            ]
        except Exception as e:
            result['projects_error'] = str(e)
        
        return result
            
    except Exception as e:
        return {
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'troubleshooting': [
                'Check that abacusai package is installed: pip install abacusai',
                'Verify your API key is set correctly',
                'Check your dataset and feature group IDs'
            ]
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
