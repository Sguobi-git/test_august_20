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
        
        # Method 1: Try multiple data extraction approaches
        data_extraction_attempts = []
        
        # Try different methods to get dataset data
        methods_to_try = [
            'get_dataset_data_as_pandas',
            'describe_dataset_data', 
            'get_dataset_data',
            'export_dataset_to_pandas',
            'get_recent_feature_group_streamed_data'
        ]
        
        for method_name in methods_to_try:
            try:
                if hasattr(client, method_name):
                    method = getattr(client, method_name)
                    if method_name == 'get_recent_feature_group_streamed_data':
                        data = method(FEATURE_GROUP_ID)
                    else:
                        data = method(DATASET_ID)
                    
                    data_extraction_attempts.append({
                        'method': method_name,
                        'success': True,
                        'data_type': str(type(data)),
                        'has_data': data is not None
                    })
                    
                    if data is not None:
                        # Process the data based on its type
                        if hasattr(data, 'shape') and hasattr(data, 'iloc'):
                            # It's a pandas DataFrame
                            if len(data) > 5:
                                # Extract headers from line 5 (index 4), data from line 6+ (index 5+)
                                actual_headers = data.iloc[4].tolist()
                                actual_headers = [str(h).strip() if str(h) not in ['nan', 'None', 'NaN'] else f'Column_{i}' for i, h in enumerate(actual_headers)]
                                
                                data_rows = data.iloc[5:].copy()
                                data_rows.columns = actual_headers
                                data_rows = data_rows.dropna(how='all')
                                
                                result['dataset_data'] = {
                                    'method': f'{method_name}_processed',
                                    'shape': data_rows.shape,
                                    'columns': actual_headers,
                                    'sample_data': data_rows.head(10).to_dict('records'),
                                    'total_rows': len(data_rows),
                                    'header_source': 'Line 5 (index 4)',
                                    'data_start': 'Line 6+ (index 5+)'
                                }
                                break
                            else:
                                result['dataset_data'] = {
                                    'method': f'{method_name}_small',
                                    'shape': data.shape,
                                    'columns': list(data.columns),
                                    'sample_data': data.to_dict('records'),
                                    'note': f'Dataset has {len(data)} rows - too small for line 5/6 processing'
                                }
                                break
                        elif isinstance(data, (list, tuple)):
                            # It's a list or tuple
                            if len(data) > 5:
                                headers = data[4] if isinstance(data[4], (list, tuple)) else [f'Column_{i}' for i in range(len(data[4]) if hasattr(data[4], '__len__') else 9)]
                                sample_rows = data[5:15] if len(data) > 15 else data[5:]
                                
                                result['dataset_data'] = {
                                    'method': f'{method_name}_list',
                                    'total_rows': len(data) - 5,
                                    'columns': headers,
                                    'sample_data': sample_rows,
                                    'header_source': 'Index 4 (line 5)',
                                    'data_start': 'Index 5+ (line 6+)'
                                }
                                break
                        else:
                            # Unknown data type - show what we got
                            result['dataset_data'] = {
                                'method': f'{method_name}_unknown',
                                'data_type': str(type(data)),
                                'data_preview': str(data)[:500],
                                'data_length': len(data) if hasattr(data, '__len__') else 'No length'
                            }
                            break
                else:
                    data_extraction_attempts.append({
                        'method': method_name,
                        'success': False,
                        'error': 'Method not available'
                    })
            except Exception as e:
                data_extraction_attempts.append({
                    'method': method_name,
                    'success': False,
                    'error': str(e)
                })
        
        # If no data extraction worked, show what we tried
        if 'dataset_data' not in result:
            result['data_extraction_attempts'] = data_extraction_attempts
            result['dataset_data_error'] = 'No data extraction method succeeded'
        
        # Method 2: Try ChatLLM approach (based on your working test code)
        try:
            # You have ChatLLM projects available, let's try that approach
            chatllm_projects = [p for p in projects if getattr(p, 'use_case', '') == 'CHAT_LLM']
            
            if chatllm_projects:
                # Use the first available ChatLLM project
                project_id = chatllm_projects[0].project_id
                
                # Create chat session
                session = client.create_chat_session(project_id)
                
                # Ask for structured data from your booth checklist
                response = client.get_chat_response(
                    session.chat_session_id,
                    "Show me the data from the New_Booth_Check_List_EXACT_COPY table. Display it as a structured table with proper column headers. Show the first 10 rows of actual data, skipping any header or metadata rows."
                )
                
                result['dataset_data'] = {
                    'method': 'chatllm_query',
                    'project_id': project_id,
                    'project_name': chatllm_projects[0].name,
                    'chat_session_id': session.chat_session_id,
                    'response': response.content,
                    'note': 'Data extracted via ChatLLM conversation - may need manual parsing'
                }
                
        except Exception as e:
            result['chatllm_error'] = f'ChatLLM approach failed: {str(e)}'
        
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
