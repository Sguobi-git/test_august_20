from flask import Flask, render_template, jsonify
import requests
import os
from datetime import datetime

app = Flask(__name__)

# Abacus AI configuration
ABACUS_API_KEY = os.environ.get('ABACUS_API_KEY')
FEATURE_GROUP_ID = '236a2273a'  # Updated to new feature group
DATASET_ID = '7a88a4bc0'  # Updated to new dataset
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
        
        # Method 1: Use SQL query (the working method)
        try:
            # Use the correct table name for your new dataset
            sql_query = "SELECT * FROM Booth_Check_List_EXACT_COPY LIMIT 20"
            data = client.execute_feature_group_sql(sql_query)
            
            if data is not None and hasattr(data, 'shape'):
                # Process the data - row 4 contains headers, data starts from row 5
                if len(data) > 4:
                    # Extract headers from row 4 (index 4)
                    headers = data.iloc[4].tolist()
                    headers = [str(h).strip() if str(h) not in ['None', 'nan', 'NaN'] else f'Column_{i}' for i, h in enumerate(headers)]
                    
                    # Extract data from row 5 onwards
                    data_rows = data.iloc[5:].copy()
                    data_rows.columns = headers
                    
                    # Remove completely empty rows
                    data_rows = data_rows.dropna(how='all')
                    
                    # Convert to records for display
                    sample_records = data_rows.head(10).to_dict('records')
                    
                    result['dataset_data'] = {
                        'method': 'execute_feature_group_sql',
                        'table_name': 'Booth_Check_List_EXACT_COPY',
                        'shape': data_rows.shape,
                        'columns': headers,
                        'sample_data': sample_records,
                        'total_rows': len(data_rows),
                        'header_source': 'Row 5 (index 4)',
                        'data_start': 'Row 6+ (index 5+)',
                        'note': 'Successfully extracted booth checklist data using SQL query'
                    }
                else:
                    result['dataset_data'] = {
                        'method': 'execute_feature_group_sql',
                        'table_name': 'Booth_Check_List_EXACT_COPY', 
                        'raw_shape': data.shape,
                        'raw_data': data.head().to_dict('records'),
                        'note': 'Data has fewer than 5 rows'
                    }
            else:
                result['dataset_data_error'] = 'SQL query returned None or invalid data'
                
        except Exception as e:
            result['dataset_data_error'] = f'SQL query failed: {str(e)}'
        
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
