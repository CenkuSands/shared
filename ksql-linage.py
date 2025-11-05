import requests
import json
from typing import Dict, List, Any, Optional
import argparse
import re

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
        
        if username and password:
            self.auth = (username, password)
        elif api_key and api_secret:
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
            
            verify = self.verify_ssl
            if self.ca_cert:
                verify = self.ca_cert
            elif verify is True:
                verify = certifi.where()
            
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
            
        except Exception as e:
            print(f"Error: {e}")
            return None

    def debug_response_format(self):
        """Debug method to see the actual response structure"""
        test_queries = [
            "SHOW STREAMS;",
            "SHOW STREAMS EXTENDED;", 
            "SHOW QUERIES;",
            "SHOW QUERIES EXTENDED;",
            "LIST STREAMS;",  # Alternative command
            "LIST QUERIES;",  # Alternative command
        ]
        
        for query in test_queries:
            print(f"\n{'='*60}")
            print(f"Query: {query}")
            print(f"{'='*60}")
            result = self.execute_ksql(query)
            if result:
                print("Full response structure:")
                print(json.dumps(result, indent=2, default=str))
                
                # Also print the type and keys for easier analysis
                if isinstance(result, list):
                    print(f"\nResponse is a LIST with {len(result)} items")
                    for i, item in enumerate(result):
                        print(f"Item {i} type: {type(item)}, keys: {list(item.keys()) if isinstance(item, dict) else 'Not a dict'}")
                elif isinstance(result, dict):
                    print(f"\nResponse is a DICT with keys: {list(result.keys())}")

    def parse_show_response(self, response: Any, entity_type: str) -> List[Dict[str, Any]]:
        """Parse SHOW STREAMS/TABLES/QUERIES response - handles multiple formats"""
        entities = []
        
        print(f"Parsing {entity_type} from response type: {type(response)}")
        
        # Format 1: Direct list of entities (most common)
        if isinstance(response, list):
            for item in response:
                if isinstance(item, dict):
                    # Check for different possible keys
                    if entity_type in item:
                        if isinstance(item[entity_type], list):
                            entities.extend(item[entity_type])
                        else:
                            entities.append(item[entity_type])
                    # Some versions put data in '@type' and other fields
                    elif 'name' in item and ('topic' in item or 'queryString' in item):
                        entities.append(item)
                    # Look for statementText with data
                    elif 'statementText' in item and entity_type[:-1].upper() in item.get('statementText', ''):
                        print(f"Found statementText: {item.get('statementText')}")
        
        # Format 2: Direct dict with entities
        elif isinstance(response, dict):
            if entity_type in response:
                if isinstance(response[entity_type], list):
                    entities.extend(response[entity_type])
                else:
                    entities.append(response[entity_type])
        
        # Format 3: Nested in statements field (some ksqlDB versions)
        elif hasattr(response, 'get') and response.get('statements'):
            for stmt in response['statements']:
                if entity_type in stmt:
                    entities.extend(stmt[entity_type])
        
        print(f"Found {len(entities)} {entity_type} after parsing")
        
        # Debug: Print first entity if available
        if entities:
            print(f"First {entity_type} example:")
            print(json.dumps(entities[0], indent=2, default=str))
        
        return entities

    def extract_entities_from_describe(self, entity_name: str, entity_type: str) -> Optional[Dict[str, Any]]:
        """Use DESCRIBE EXTENDED to get detailed information"""
        try:
            if entity_type.upper() == 'STREAM':
                result = self.execute_ksql(f"DESCRIBE {entity_name} EXTENDED;")
            elif entity_type.upper() == 'TABLE':
                result = self.execute_ksql(f"DESCRIBE {entity_name} EXTENDED;")
            else:
                result = self.execute_ksql(f"DESCRIBE {entity_name};")
            
            if result:
                print(f"DESCRIBE {entity_name} result:")
                print(json.dumps(result, indent=2, default=str))
            return result
        except Exception as e:
            print(f"Error describing {entity_name}: {e}")
            return None

    def parse_dependencies(self, sql: str, query_id: str) -> List[Dict[str, str]]:
        """Parse SQL to extract source and target relationships with better pattern matching"""
        dependencies = []
        if not sql:
            return dependencies
            
        sql_upper = sql.upper().replace('\n', ' ').replace('\r', ' ')
        
        print(f"Parsing dependencies from SQL: {sql_upper[:200]}...")
        
        # Pattern 1: CREATE STREAM/TABLE ... AS SELECT ... FROM ...
        create_pattern = r'CREATE\s+(TABLE|STREAM)\s+(\w+)\s+AS\s+SELECT.*?\s+FROM\s+(\w+)'
        matches = re.findall(create_pattern, sql_upper, re.IGNORECASE | re.DOTALL)
        for match in matches:
            object_type, target, source = match
            dependencies.append({
                "source": source,
                "target": target,
                "query_id": query_id,
                "type": f"CREATE_{object_type}"
            })
            print(f"Found CREATE dependency: {source} -> {target}")
        
        # Pattern 2: INSERT INTO ... SELECT ... FROM ...
        insert_pattern = r'INSERT\s+INTO\s+(\w+)\s+SELECT.*?\s+FROM\s+(\w+)'
        matches = re.findall(insert_pattern, sql_upper, re.IGNORECASE | re.DOTALL)
        for match in matches:
            target, source = match
            dependencies.append({
                "source": source,
                "target": target,
                "query_id": query_id,
                "type": "INSERT_INTO"
            })
            print(f"Found INSERT dependency: {source} -> {target}")
        
        # Pattern 3: CREATE STREAM/TABLE ... WITH (...)
        # This might be a source stream/table without explicit FROM
        if not dependencies and ("CREATE STREAM" in sql_upper or "CREATE TABLE" in sql_upper):
            create_name_pattern = r'CREATE\s+(TABLE|STREAM)\s+(\w+)'
            matches = re.findall(create_name_pattern, sql_upper)
            for match in matches:
                object_type, name = match
                dependencies.append({
                    "source": f"EXTERNAL_SOURCE_{name}",
                    "target": name,
                    "query_id": query_id,
                    "type": f"SOURCE_{object_type}"
                })
                print(f"Found SOURCE dependency: EXTERNAL -> {name}")
        
        print(f"Total dependencies found: {len(dependencies)}")
        return dependencies

    def build_lineage(self) -> Dict[str, Any]:
        """Build complete lineage graph with enhanced parsing"""
        print("Collecting ksqlDB metadata...")
        
        lineage = {
            "streams": {},
            "tables": {},
            "queries": {},
            "dependencies": [],
            "lineage_graph": {},
            "raw_responses": {}  # Store raw responses for debugging
        }
        
        # Get streams with multiple approaches
        streams_result = self.execute_ksql("SHOW STREAMS EXTENDED;")
        lineage['raw_responses']['streams'] = streams_result
        
        if streams_result:
            streams = self.parse_show_response(streams_result, 'streams')
            if not streams:
                # Try alternative command
                print("Trying LIST STREAMS as alternative...")
                streams_result_alt = self.execute_ksql("LIST STREAMS;")
                streams = self.parse_show_response(streams_result_alt, 'streams')
            
            for stream in streams:
                stream_name = stream.get('name')
                if not stream_name:
                    # Try other possible name fields
                    stream_name = stream.get('sourceDescription', {}).get('name') if isinstance(stream.get('sourceDescription'), dict) else None
                
                if stream_name:
                    lineage['streams'][stream_name] = {
                        'type': 'STREAM',
                        'topic': stream.get('topic', stream.get('kafkaTopic', '')),
                        'format': stream.get('format', ''),
                        'query': stream.get('query', ''),
                        'raw_data': stream  # Keep raw data for debugging
                    }
                    print(f"Added stream: {stream_name}")
        
        # Get tables
        tables_result = self.execute_ksql("SHOW TABLES EXTENDED;")
        lineage['raw_responses']['tables'] = tables_result
        
        if tables_result:
            tables = self.parse_show_response(tables_result, 'tables')
            if not tables:
                print("Trying LIST TABLES as alternative...")
                tables_result_alt = self.execute_ksql("LIST TABLES;")
                tables = self.parse_show_response(tables_result_alt, 'tables')
            
            for table in tables:
                table_name = table.get('name')
                if not table_name:
                    table_name = table.get('sourceDescription', {}).get('name') if isinstance(table.get('sourceDescription'), dict) else None
                
                if table_name:
                    lineage['tables'][table_name] = {
                        'type': 'TABLE', 
                        'topic': table.get('topic', table.get('kafkaTopic', '')),
                        'format': table.get('format', ''),
                        'query': table.get('query', ''),
                        'raw_data': table
                    }
                    print(f"Added table: {table_name}")
        
        # Get queries
        queries_result = self.execute_ksql("SHOW QUERIES EXTENDED;")
        lineage['raw_responses']['queries'] = queries_result
        
        if queries_result:
            queries = self.parse_show_response(queries_result, 'queries')
            if not queries:
                print("Trying LIST QUERIES as alternative...")
                queries_result_alt = self.execute_ksql("LIST QUERIES;")
                queries = self.parse_show_response(queries_result_alt, 'queries')
            
            for query in queries:
                query_id = query.get('id')
                if not query_id:
                    query_id = query.get('queryId', query.get('idString', ''))
                
                if query_id:
                    sql_text = query.get('sql', query.get('queryString', ''))
                    lineage['queries'][query_id] = {
                        'sql': sql_text,
                        'status': query.get('status', query.get('state', '')),
                        'sources': query.get('sources', []),
                        'sinks': query.get('sinks', []),
                        'raw_data': query
                    }
                    print(f"Added query: {query_id}")
                    
                    # Extract dependencies from SQL
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
            
            # Initialize nodes if they don't exist
            if source not in graph:
                graph[source] = {'type': 'unknown', 'dependencies': [], 'dependents': []}
            if target not in graph:
                graph[target] = {'type': 'unknown', 'dependencies': [], 'dependents': []}
            
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
        
        # Print actual stream/table names
        if lineage['streams']:
            print(f"  Stream names: {list(lineage['streams'].keys())}")
        if lineage['tables']:
            print(f"  Table names: {list(lineage['tables'].keys())}")
        
        print(f"\nDEPENDENCIES:")
        print("-" * 80)
        if lineage['dependencies']:
            for dep in lineage['dependencies']:
                print(f"  {dep['source']} -> {dep['target']} [{dep['type']}]")
        else:
            print("  No dependencies found")
        
        print(f"\nLINEAGE GRAPH:")
        print("-" * 80)
        if lineage['lineage_graph']:
            for node, info in lineage['lineage_graph'].items():
                deps = ", ".join(info['dependencies']) if info['dependencies'] else "None"
                dependents = ", ".join(info['dependents']) if info['dependents'] else "None"
                print(f"  {node} ({info['type']}):")
                print(f"    ← Depends on: {deps}")
                print(f"    → Feeds into: {dependents}")
        else:
            print("  No lineage graph built")

    def export_lineage_json(self, lineage: Dict[str, Any], filename: str):
        """Export lineage as JSON file"""
        with open(filename, 'w') as f:
            json.dump(lineage, f, indent=2, default=str)
        print(f"Lineage exported to {filename}")

# ... (keep the main() function and rest of the code the same as previous complete version)

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
