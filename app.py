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
                if pandas_data is not None:
                    # Debug info
                    debug_info = {
                        'has_shape': hasattr(pandas_data, 'shape'),
                        'type': str(type(pandas_data)),
                        'length': len(pandas_data) if hasattr(pandas_data, '__len__') else 'No length'
                    }
                    
                    if hasattr(pandas_data, 'shape'):
                        # Handle the header row issue - line 5 contains column titles (index 4), data starts line 6 (index 5)
                        if len(pandas_data) > 5:
                            # Extract actual column headers from line 5 (index 4)
                            actual_headers = pandas_data.iloc[4].tolist()
                            # Clean up header names
                            actual_headers = [str(h).strip() if str(h) != 'nan' and str(h) != 'None' else f'Column_{i}' for i, h in enumerate(actual_headers)]
                            
                            # Extract data starting from line 6 (index 5)
                            data_rows = pandas_data.iloc[5:].copy()
                            data_rows.columns = actual_headers
                            
                            # Remove rows that are entirely empty
                            data_rows = data_rows.dropna(how='all')
                            
                            # Convert to records for display
                            sample_records = data_rows.head(10).to_dict('records')
                            
                            result['dataset_data'] = {
                                'method': 'pandas_processed',
                                'shape': data_rows.shape,
                                'columns': actual_headers,
                                'sample_data': sample_records,
                                'total_rows': len(data_rows),
                                'header_detection': 'Line 5 (index 4)',
                                'data_start': 'Line 6 (index 5)',
                                'debug_info': debug_info
                            }
                        else:
                            # Dataset too small
                            result['dataset_data'] = {
                                'method': 'pandas_small',
                                'shape': pandas_data.shape,
                                'columns': list(pandas_data.columns),
                                'sample_data': pandas_data.to_dict('records'),
                                'note': f'Dataset only has {len(pandas_data)} rows - cannot apply line 5/6 logic',
                                'debug_info': debug_info
                            }
                    else:
                        # Not a DataFrame
                        result['dataset_data'] = {
                            'method': 'pandas_not_dataframe',
                            'data_type': str(type(pandas_data)),
                            'data_preview': str(pandas_data)[:500],
                            'debug_info': debug_info
                        }
                else:
                    result['dataset_data_error'] = 'get_dataset_data_as_pandas returned None'
            else:
                result['dataset_data_error'] = 'get_dataset_data_as_pandas method not available'
        except Exception as e:
            result['dataset_data_error'] = f'Pandas extraction failed: {str(e)}'
        
        # Method 1b: Try alternative data extraction methods
        try:
            # Try to get raw dataset data if pandas doesn't work
            if hasattr(client, 'describe_dataset_data') and 'dataset_data' not in result:
                dataset_data = client.describe_dataset_data(DATASET_ID)
                result['raw_dataset_info'] = {
                    'method': 'describe_dataset_data',
                    'type': str(type(dataset_data)),
                    'data_preview': str(dataset_data)[:500] if dataset_data else None
                }
        except Exception as e:
            result['raw_dataset_error'] = str(e)
        
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
