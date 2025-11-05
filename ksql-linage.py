import requests
import json
from typing import Dict, List, Any, Optional
import argparse
import certifi

class KsqlDBLineage:
    def __init__(self, ksql_url: str, username: str = None, password: str = None, 
                 api_key: str = None, api_secret: str = None, 
                 verify_ssl: bool = True, ca_cert: str = None):
        self.ksql_url = f"{ksql_url}/ksql"
        self.headers = {
            "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
            "Accept": "application/vnd.ksql.v1+json"
        }
        self.auth = None
        self.verify_ssl = verify_ssl
        self.ca_cert = ca_cert
        
        # Handle authentication
        if username and password:
            self.auth = (username, password)
        elif api_key and api_secret:
            # For Confluent Cloud
            import base64
            credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
            self.headers["Authorization"] = f"Basic {credentials}"

    def execute_ksql(self, ksql: str) -> Optional[Dict[str, Any]]:
        """Execute ksqlDB query and return results"""
        try:
            print(f"Executing: {ksql[:100]}...")
            
            payload = {
                "ksql": ksql,
                "streamsProperties": {}
            }
            
            # Handle SSL verification
            verify = self.verify_ssl
            if self.ca_cert:
                verify = self.ca_cert
            elif verify is True:
                verify = certifi.where()  # Use certifi bundle
            
            response = requests.post(
                self.ksql_url,
                headers=self.headers,
                auth=self.auth,
                json=payload,
                timeout=30,
                verify=verify
            )
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Error: HTTP {response.status_code} - {response.text}")
                return None
                
            result = response.json()
            return result
            
        except requests.exceptions.SSLError as e:
            print(f"SSL Error: {e}")
            print("You may need to:")
            print("1. Use --no-ssl-verify to disable SSL verification")
            print("2. Use --ca-cert to specify a custom CA certificate")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Raw response: {response.text}")
            return None

    def debug_response_format(self):
        """Debug method to see the actual response structure"""
        test_queries = [
            "SHOW STREAMS;",
            "SHOW STREAMS EXTENDED;", 
            "SHOW QUERIES;",
            "SHOW QUERIES EXTENDED;"
        ]
        
        for query in test_queries:
            print(f"\n{'='*50}")
            print(f"Query: {query}")
            print(f"{'='*50}")
            result = self.execute_ksql(query)
            if result:
                print("Raw response:")
                print(json.dumps(result, indent=2))

    def parse_show_response(self, response: Dict[str, Any], entity_type: str) -> List[Dict[str, Any]]:
        """Parse SHOW STREAMS/TABLES/QUERIES response"""
        entities = []
        
        if isinstance(response, list):
            for item in response:
                if entity_type in item:
                    entities.extend(item[entity_type])
                elif 'statementText' in item:
                    print(f"Statement text: {item.get('statementText')}")
        elif isinstance(response, dict):
            if entity_type in response:
                entities.extend(response[entity_type])
        
        print(f"Found {len(entities)} {entity_type}")
        return entities

    def parse_dependencies(self, sql: str, query_id: str) -> List[Dict[str, str]]:
        """Parse SQL to extract source and target relationships"""
        dependencies = []
        sql_upper = sql.upper()
        
        # Extract CREATE statements
        if "CREATE STREAM" in sql_upper or "CREATE TABLE" in sql_upper:
            lines = sql.split('\n')
            target_obj = None
            source_obj = None
            
            for line in lines:
                line_upper = line.upper().strip()
                # Find target (AS clause)
                if " AS " in line_upper and not target_obj:
                    parts = line.split(' AS ')
                    if len(parts) > 1:
                        target_obj = parts[0].split()[-1].strip()
                
                # Find source (FROM clause)
                if " FROM " in line_upper and not source_obj:
                    parts = line_upper.split(' FROM ')
                    if len(parts) > 1:
                        source_obj = parts[1].split()[0].strip('`')
            
            if source_obj and target_obj:
                dependencies.append({
                    "source": source_obj,
                    "target": target_obj,
                    "query_id": query_id,
                    "type": "CREATE_AS"
                })
        
        # Extract INSERT INTO statements
        elif "INSERT INTO" in sql_upper:
            lines = sql.split('\n')
            target_obj = None
            source_obj = None
            
            for line in lines:
                line_upper = line.upper().strip()
                if "INSERT INTO" in line_upper:
                    parts = line_upper.split('INSERT INTO ')
                    if len(parts) > 1:
                        target_obj = parts[1].split()[0].strip('`')
                
                if " SELECT " in line_upper and " FROM " in line_upper:
                    from_part = line_upper.split(' FROM ')[1]
                    source_obj = from_part.split()[0].strip('`')
            
            if source_obj and target_obj:
                dependencies.append({
                    "source": source_obj,
                    "target": target_obj,
                    "query_id": query_id,
                    "type": "INSERT_INTO"
                })
        
        return dependencies

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

    def _build_lineage_graph(self, lineage: Dict[str, Any]):
        """Build a graph representation of lineage"""
        graph = {}
        
        # Add all nodes
        for stream in lineage['streams']:
            graph[stream] = {'type': 'stream', 'dependencies': [], 'dependents': []}
        
        for table in lineage['tables']:
            graph[table] = {'type': 'table', 'dependencies': [], 'dependents': []}
        
        # Add edges based on dependencies
        for dep in lineage['dependencies']:
            source = dep['source']
            target = dep['target']
            
            if source in graph and target in graph:
                graph[source]['dependents'].append(target)
                graph[target]['dependencies'].append(source)
        
        lineage['lineage_graph'] = graph

    def print_lineage_report(self, lineage: Dict[str, Any]):
        """Print a formatted lineage report"""
        print("\n" + "="*80)
        print("KSQLDB LINEAGE REPORT")
        print("="*80)
        
        print(f"\nOBJECT SUMMARY:")
        print(f"  Streams: {len(lineage['streams'])}")
        print(f"  Tables: {len(lineage['tables'])}")
        print(f"  Queries: {len(lineage['queries'])}")
        print(f"  Dependencies: {len(lineage['dependencies'])}")
        
        print(f"\nDEPENDENCIES:")
        print("-" * 80)
        for dep in lineage['dependencies']:
            print(f"  {dep['source']} -> {dep['target']} [{dep['type']}]")
        
        print(f"\nLINEAGE GRAPH:")
        print("-" * 80)
        for node, info in lineage['lineage_graph'].items():
            if info['dependencies'] or info['dependents']:
                deps = ", ".join(info['dependencies']) if info['dependencies'] else "None"
                dependents = ", ".join(info['dependents']) if info['dependents'] else "None"
                print(f"  {node} ({info['type']}):")
                print(f"    ← Depends on: {deps}")
                print(f"    → Feeds into: {dependents}")

    def export_lineage_json(self, lineage: Dict[str, Any], filename: str):
        """Export lineage as JSON file"""
        with open(filename, 'w') as f:
            json.dump(lineage, f, indent=2)
        print(f"Lineage exported to {filename}")

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
    
    print("Equivalent curl command for testing:")
    print(curl_cmd)

def main():
    parser = argparse.ArgumentParser(description='Extract ksqlDB lineage')
    parser.add_argument('--url', required=True, help='ksqlDB server URL')
    parser.add_argument('--username', help='Username for basic auth') 
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    parser.add_argument('--no-ssl-verify', action='store_true', help='Disable SSL verification')
    parser.add_argument('--ca-cert', help='Path to custom CA certificate file')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--export-json', help='Export lineage to JSON file')
    
    args = parser.parse_args()
    
    # Show equivalent curl command for testing
    auth_args = ""
    if args.username and args.password:
        auth_args = f'-u "{args.username}:{args.password}"'
    elif args.api_key and args.api_secret:
        import base64
        credentials = base64.b64encode(f"{args.api_key}:{args.api_secret}".encode()).decode()
        auth_args = f'-H "Authorization: Basic {credentials}"'
    
    test_curl_command(args.url, auth_args)
    print()  # Empty line
    
    # Initialize with SSL options
    ksql_client = KsqlDBLineage(
        ksql_url=args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key, 
        api_secret=args.api_secret,
        verify_ssl=not args.no_ssl_verify,
        ca_cert=args.ca_cert
    )
    
    if args.debug:
        ksql_client.debug_response_format()
    else:
        lineage = ksql_client.build_lineage()
        ksql_client.print_lineage_report(lineage)
        
        if args.export_json:
            ksql_client.export_lineage_json(lineage, args.export_json)

if __name__ == "__main__":
    main()
