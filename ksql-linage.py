import requests
import json
from typing import Dict, List, Any, Optional
import argparse

class KsqlDBLineage:
    def __init__(self, ksql_url: str, username: str = None, password: str = None, 
                 api_key: str = None, api_secret: str = None):
        self.ksql_url = f"{ksql_url}/ksql"
        self.headers = {
            "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
            "Accept": "application/vnd.ksql.v1+json"
        }
        self.auth = None
        
        if username and password:
            self.auth = (username, password)
        elif api_key and api_secret:
            # For Confluent Cloud, often the API key/secret are passed in headers
            self.headers["Authorization"] = f"Basic {api_key}:{api_secret}"

    def execute_ksql(self, ksql: str) -> Optional[Dict[str, Any]]:
        """Execute ksqlDB query and return results"""
        try:
            print(f"Executing: {ksql[:100]}...")  # Debug log
            
            payload = {
                "ksql": ksql,
                "streamsProperties": {}
            }
            
            response = requests.post(
                self.ksql_url,
                headers=self.headers,
                auth=self.auth,
                json=payload,
                timeout=30,
                verify=False  # Temporarily for debugging SSL issues
            )
            
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                print(f"Error: HTTP {response.status_code} - {response.text}")
                return None
                
            result = response.json()
            print(f"Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Raw response: {response.text}")
            return None

    def parse_show_response(self, response: Dict[str, Any], entity_type: str) -> List[Dict[str, Any]]:
        """Parse SHOW STREAMS/TABLES/QUERIES response"""
        entities = []
        
        # ksqlDB responses can be in different formats
        if isinstance(response, list):
            # Format 1: List response
            for item in response:
                if entity_type in item:
                    entities.extend(item[entity_type])
                elif 'statementText' in item:
                    # This might be an error message
                    print(f"Statement text: {item.get('statementText')}")
        elif isinstance(response, dict):
            # Format 2: Direct dict response
            if entity_type in response:
                entities.extend(response[entity_type])
        
        print(f"Found {len(entities)} {entity_type}")  # Debug
        return entities

    def build_lineage(self) -> Dict[str, Any]:
        """Build complete lineage graph"""
        print("Collecting ksqlDB metadata...")
        
        lineage = {
            "streams": {},
            "tables": {},
            "queries": {},
            "dependencies": [],
            "lineage_graph": {}
        }
        
        # Get streams
        streams_result = self.execute_ksql("SHOW STREAMS EXTENDED;")
        if streams_result:
            streams = self.parse_show_response(streams_result, 'streams')
            for stream in streams:
                stream_name = stream.get('name')
                if stream_name:
                    lineage['streams'][stream_name] = {
                        'type': 'STREAM',
                        'topic': stream.get('topic', ''),
                        'format': stream.get('format', ''),
                        'query': stream.get('query', '')
                    }
        
        # Get tables
        tables_result = self.execute_ksql("SHOW TABLES EXTENDED;")
        if tables_result:
            tables = self.parse_show_response(tables_result, 'tables')
            for table in tables:
                table_name = table.get('name')
                if table_name:
                    lineage['tables'][table_name] = {
                        'type': 'TABLE', 
                        'topic': table.get('topic', ''),
                        'format': table.get('format', ''),
                        'query': table.get('query', '')
                    }
        
        # Get queries
        queries_result = self.execute_ksql("SHOW QUERIES EXTENDED;")
        if queries_result:
            queries = self.parse_show_response(queries_result, 'queries')
            for query in queries:
                query_id = query.get('id')
                if query_id:
                    lineage['queries'][query_id] = {
                        'sql': query.get('sql', ''),
                        'status': query.get('status', ''),
                        'sources': query.get('sources', []),
                        'sinks': query.get('sinks', [])
                    }
                    
                    # Extract dependencies from SQL
                    sql_text = query.get('sql', '')
                    dependencies = self.parse_dependencies(sql_text, query_id)
                    lineage['dependencies'].extend(dependencies)
        
        # Build graph
        self._build_lineage_graph(lineage)
        return lineage

    # ... keep the rest of your existing methods (parse_dependencies, etc.) ...

def test_curl_command(ksql_url: str, auth_args: str = ""):
    """Test what curl command would work"""
    curl_cmd = f'curl -X "POST" "{ksql_url}/ksql" \\\n'
    curl_cmd += '     -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \\\n'
    curl_cmd += '     -H "Accept: application/vnd.ksql.v1+json" \\\n'
    
    if auth_args:
        curl_cmd += f'     {auth_args} \\\n'
    
    curl_cmd += '     -d $\'\n'
    curl_cmd += '     {\n'
    curl_cmd += '       "ksql": "SHOW STREAMS EXTENDED;",\n'
    curl_cmd += '       "streamsProperties": {}\n'
    curl_cmd += '     }\'\n'
    
    print("Try this curl command to test:")
    print(curl_cmd)

def main():
    parser = argparse.ArgumentParser(description='Extract ksqlDB lineage')
    parser.add_argument('--url', required=True, help='ksqlDB server URL')
    parser.add_argument('--username', help='Username for basic auth') 
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Show equivalent curl command for testing
    auth_args = ""
    if args.username and args.password:
        auth_args = f'-u "{args.username}:{args.password}"'
    elif args.api_key and args.api_secret:
        auth_args = f'-H "Authorization: Basic {args.api_key}:{args.api_secret}"'
    
    test_curl_command(args.url, auth_args)
    
    # Initialize and run
    ksql_client = KsqlDBLineage(
        ksql_url=args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key, 
        api_secret=args.api_secret
    )
    
    if args.debug:
        ksql_client.debug_response_format()
    else:
        lineage = ksql_client.build_lineage()
        ksql_client.print_lineage_report(lineage)

if __name__ == "__main__":
    main()
