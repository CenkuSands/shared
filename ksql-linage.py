import json
import csv
import argparse
import re
import base64
from datetime import datetime
import os
import sys
try:
    from urllib.request import Request, urlopen
    from urllib.error import URLError, HTTPError
    import ssl
except ImportError:
    pass

class KsqlDBLineageOffline:
    def __init__(self, ksql_url: str, username: str = None, password: str = None, 
                 api_key: str = None, api_secret: str = None, 
                 verify_ssl: bool = True, ca_cert: str = None):
        self.ksql_url = f"{ksql_url}/ksql"
        self.username = username
        self.password = password
        self.api_key = api_key
        self.api_secret = api_secret
        self.verify_ssl = verify_ssl
        self.ca_cert = ca_cert

    def execute_ksql(self, ksql: str):
        """Execute ksqlDB query using urllib (standard library)"""
        try:
            print(f"Executing: {ksql[:100]}...")
            
            payload = json.dumps({
                "ksql": ksql,
                "streamsProperties": {}
            }).encode('utf-8')
            
            headers = {
                "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
                "Accept": "application/vnd.ksql.v1+json",
                "User-Agent": "ksqlDB-Lineage-Tool/1.0"
            }
            
            # Handle authentication
            if self.username and self.password:
                auth_string = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
                headers["Authorization"] = f"Basic {auth_string}"
            elif self.api_key and self.api_secret:
                auth_string = base64.b64encode(f"{self.api_key}:{self.api_secret}".encode()).decode()
                headers["Authorization"] = f"Basic {auth_string}"
            
            # Create SSL context
            ssl_context = None
            if not self.verify_ssl:
                ssl_context = ssl._create_unverified_context()
            elif self.ca_cert:
                ssl_context = ssl.create_default_context(cafile=self.ca_cert)
            
            request = Request(self.ksql_url, data=payload, headers=headers, method='POST')
            
            if ssl_context:
                response = urlopen(request, context=ssl_context, timeout=30)
            else:
                response = urlopen(request, timeout=30)
            
            print(f"Response status: {response.getcode()}")
            
            if response.getcode() == 200:
                result = response.read().decode('utf-8')
                parsed_result = json.loads(result)
                print(f"Response type: {type(parsed_result)}")
                if isinstance(parsed_result, list):
                    print(f"Response list length: {len(parsed_result)}")
                    for i, item in enumerate(parsed_result):
                        print(f"  Item {i} type: {type(item)}, keys: {list(item.keys()) if isinstance(item, dict) else 'N/A'}")
                return parsed_result
            else:
                print(f"Error: HTTP {response.getcode()}")
                return None
                
        except HTTPError as e:
            print(f"HTTP Error: {e.code} - {e.reason}")
            try:
                error_body = e.read().decode()
                print(f"Error response: {error_body}")
            except:
                pass
            return None
        except URLError as e:
            print(f"URL Error: {e.reason}")
            return None
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def debug_response_structure(self):
        """Debug the actual response structure"""
        test_queries = [
            "SHOW STREAMS;",
            "SHOW STREAMS EXTENDED;", 
            "SHOW QUERIES;",
            "SHOW QUERIES EXTENDED;",
            "LIST STREAMS;",
            "LIST QUERIES;"
        ]
        
        for query in test_queries:
            print(f"\n{'='*60}")
            print(f"DEBUG: Testing query: {query}")
            print(f"{'='*60}")
            result = self.execute_ksql(query)
            if result:
                print("RAW RESPONSE:")
                print(json.dumps(result, indent=2, default=str))

    def parse_show_response(self, response, entity_type: str):
        """Parse SHOW STREAMS/TABLES/QUERIES response - enhanced debugging"""
        print(f"DEBUG: Parsing {entity_type} from response type: {type(response)}")
        
        entities = []
        
        if isinstance(response, list):
            print(f"DEBUG: Response is a list with {len(response)} items")
            for i, item in enumerate(response):
                print(f"DEBUG: Item {i}: {type(item)}")
                if isinstance(item, dict):
                    print(f"DEBUG: Item {i} keys: {list(item.keys())}")
                    
                    # Check for different possible structures
                    if entity_type in item:
                        print(f"DEBUG: Found '{entity_type}' key in item {i}")
                        if isinstance(item[entity_type], list):
                            entities.extend(item[entity_type])
                            print(f"DEBUG: Added {len(item[entity_type])} entities from list")
                        else:
                            entities.append(item[entity_type])
                            print(f"DEBUG: Added 1 entity from direct object")
                    
                    # Alternative: look for statementText with tables/streams
                    elif 'statementText' in item:
                        stmt = item.get('statementText', '')
                        print(f"DEBUG: Found statementText: {stmt[:200]}...")
                        if 'stream' in stmt.lower() or 'table' in stmt.lower():
                            print(f"DEBUG: Statement contains stream/table reference")
                    
                    # Alternative: direct entity in root of item
                    elif 'name' in item and ('topic' in item or 'queryString' in item):
                        print(f"DEBUG: Found direct entity with name: {item.get('name')}")
                        entities.append(item)
        
        elif isinstance(response, dict):
            print(f"DEBUG: Response is a dict with keys: {list(response.keys())}")
            if entity_type in response:
                print(f"DEBUG: Found '{entity_type}' key in root")
                if isinstance(response[entity_type], list):
                    entities.extend(response[entity_type])
                else:
                    entities.append(response[entity_type])
        
        print(f"DEBUG: Total {len(entities)} {entity_type} found after parsing")
        
        # Show what we found
        if entities:
            print(f"DEBUG: First {entity_type} example:")
            print(json.dumps(entities[0], indent=2, default=str))
        
        return entities

    def parse_dependencies(self, sql: str, query_id: str):
        """Parse SQL to extract source and target relationships"""
        dependencies = []
        if not sql:
            return dependencies
            
        sql_upper = sql.upper().replace('\n', ' ').replace('\r', ' ')
        print(f"DEBUG: Parsing SQL: {sql_upper[:200]}...")
        
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
            print(f"DEBUG: Found CREATE dependency: {source} -> {target}")
        
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
            print(f"DEBUG: Found INSERT dependency: {source} -> {target}")
        
        # Pattern 3: Simple CREATE (source streams/tables)
        if not dependencies and ("CREATE STREAM" in sql_upper or "CREATE TABLE" in sql_upper):
            create_name_pattern = r'CREATE\s+(TABLE|STREAM)\s+(\w+)'
            matches = re.findall(create_name_pattern, sql_upper)
            for match in matches:
                object_type, name = match
                dependencies.append({
                    "source": f"EXTERNAL_SOURCE",
                    "target": name,
                    "query_id": query_id,
                    "type": f"SOURCE_{object_type}"
                })
                print(f"DEBUG: Found SOURCE dependency: EXTERNAL -> {name}")
        
        print(f"DEBUG: Total dependencies found in SQL: {len(dependencies)}")
        return dependencies

    def build_lineage(self):
        """Build complete lineage graph with enhanced debugging"""
        print("Collecting ksqlDB metadata...")
        
        lineage = {
            "streams": {},
            "tables": {},
            "queries": {},
            "dependencies": [],
            "lineage_graph": {},
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "ksql_url": self.ksql_url
            }
        }
        
        # Get streams with multiple approaches
        print("\n" + "="*50)
        print("FETCHING STREAMS")
        print("="*50)
        streams_result = self.execute_ksql("SHOW STREAMS EXTENDED;")
        if streams_result:
            streams = self.parse_show_response(streams_result, 'streams')
            if not streams:
                print("DEBUG: Trying LIST STREAMS as alternative...")
                streams_result_alt = self.execute_ksql("LIST STREAMS;")
                streams = self.parse_show_response(streams_result_alt, 'streams')
            
            for stream in streams:
                stream_name = stream.get('name')
                if not stream_name:
                    # Try alternative name fields
                    stream_name = stream.get('sourceDescription', {}).get('name') if isinstance(stream.get('sourceDescription'), dict) else None
                
                if stream_name:
                    lineage['streams'][stream_name] = {
                        'type': 'STREAM',
                        'topic': stream.get('topic', stream.get('kafkaTopic', '')),
                        'format': stream.get('format', ''),
                        'query': stream.get('query', ''),
                        'raw_data': stream  # Keep for debugging
                    }
                    print(f"DEBUG: Added stream: {stream_name}")
        
        # Get tables
        print("\n" + "="*50)
        print("FETCHING TABLES")
        print("="*50)
        tables_result = self.execute_ksql("SHOW TABLES EXTENDED;")
        if tables_result:
            tables = self.parse_show_response(tables_result, 'tables')
            if not tables:
                print("DEBUG: Trying LIST TABLES as alternative...")
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
                    print(f"DEBUG: Added table: {table_name}")
        
        # Get queries
        print("\n" + "="*50)
        print("FETCHING QUERIES")
        print("="*50)
        queries_result = self.execute_ksql("SHOW QUERIES EXTENDED;")
        if queries_result:
            queries = self.parse_show_response(queries_result, 'queries')
            if not queries:
                print("DEBUG: Trying LIST QUERIES as alternative...")
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
                    print(f"DEBUG: Added query: {query_id}")
                    
                    dependencies = self.parse_dependencies(sql_text, query_id)
                    lineage['dependencies'].extend(dependencies)
        
        # Build graph
        self._build_lineage_graph(lineage)
        return lineage

    def _build_lineage_graph(self, lineage):
        """Build a graph representation of lineage"""
        graph = {}
        
        for stream in lineage['streams']:
            graph[stream] = {'type': 'stream', 'dependencies': [], 'dependents': []}
        
        for table in lineage['tables']:
            graph[table] = {'type': 'table', 'dependencies': [], 'dependents': []}
        
        for dep in lineage['dependencies']:
            source = dep['source']
            target = dep['target']
            
            if source not in graph:
                graph[source] = {'type': 'external', 'dependencies': [], 'dependents': []}
            if target not in graph:
                graph[target] = {'type': 'unknown', 'dependencies': [], 'dependents': []}
            
            if target not in graph[source]['dependents']:
                graph[source]['dependents'].append(target)
            if source not in graph[target]['dependencies']:
                graph[target]['dependencies'].append(source)
        
        lineage['lineage_graph'] = graph

    def print_lineage_report(self, lineage):
        """Print a formatted lineage report"""
        print("\n" + "=" * 100)
        print("KSQLDB LINEAGE ANALYSIS REPORT")
        print("=" * 100)
        
        print(f"\nSUMMARY")
        print("-" * 50)
        print(f"  Streams: {len(lineage['streams'])}")
        print(f"  Tables:  {len(lineage['tables'])}")
        print(f"  Queries: {len(lineage['queries'])}")
        print(f"  Data Flows: {len(lineage['dependencies'])}")
        print(f"  Generated: {lineage['metadata']['generated_at']}")
        
        # Show actual names found
        if lineage['streams']:
            print(f"  Stream names: {list(lineage['streams'].keys())}")
        if lineage['tables']:
            print(f"  Table names: {list(lineage['tables'].keys())}")
        
        # Streams Section
        if lineage['streams']:
            print(f"\nSTREAMS ({len(lineage['streams'])})")
            print("-" * 50)
            for stream_name, info in sorted(lineage['streams'].items()):
                print(f"  {stream_name}")
                print(f"    Topic: {info['topic']} | Format: {info['format']}")
        
        # Tables Section
        if lineage['tables']:
            print(f"\nTABLES ({len(lineage['tables'])})")
            print("-" * 50)
            for table_name, info in sorted(lineage['tables'].items()):
                print(f"  {table_name}")
                print(f"    Topic: {info['topic']} | Format: {info['format']}")
        
        # Data Flow Section
        if lineage['dependencies']:
            print(f"\nDATA FLOWS ({len(lineage['dependencies'])})")
            print("-" * 80)
            print(f"  {'SOURCE':<20} -> {'TARGET':<20} {'TYPE':<15} {'QUERY':<15}")
            print("  " + "-" * 78)
            for dep in sorted(lineage['dependencies'], key=lambda x: (x['source'], x['target'])):
                source = dep['source'][:19]
                target = dep['target'][:19]
                flow_type = dep['type'].replace('_', ' ')[:14]
                query_id = dep['query_id'][:14]
                print(f"  {source:<20} -> {target:<20} {flow_type:<15} {query_id:<15}")
        else:
            print(f"\nDATA FLOWS")
            print("-" * 50)
            print("  No dependencies found - check if queries exist and have CREATE/INSERT statements")
        
        # Lineage Graph Section
        if lineage['lineage_graph']:
            print(f"\nLINEAGE GRAPH")
            print("-" * 80)
            for node, info in sorted(lineage['lineage_graph'].items()):
                if info['dependencies'] or info['dependents']:
                    print(f"  {node} ({info['type']})")
                    if info['dependencies']:
                        deps = ", ".join(sorted(info['dependencies']))
                        print(f"    <- Depends on: {deps}")
                    if info['dependents']:
                        dependents = ", ".join(sorted(info['dependents']))
                        print(f"    -> Feeds into: {dependents}")
                    print()

    def export_to_csv(self, lineage, base_filename: str):
        """Export lineage data to multiple CSV files"""
        
        # Export dependencies
        deps_filename = f"{base_filename}_dependencies.csv"
        with open(deps_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Source', 'Target', 'Relationship_Type', 'Query_ID'])
            for dep in lineage['dependencies']:
                writer.writerow([dep['source'], dep['target'], dep['type'], dep['query_id']])
        print(f"Dependencies exported to: {deps_filename}")
        
        # Export streams
        streams_filename = f"{base_filename}_streams.csv"
        with open(streams_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Stream_Name', 'Kafka_Topic', 'Format', 'Query'])
            for stream_name, info in lineage['streams'].items():
                writer.writerow([stream_name, info['topic'], info['format'], info['query']])
        print(f"Streams exported to: {streams_filename}")
        
        # Export tables
        tables_filename = f"{base_filename}_tables.csv"
        with open(tables_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Table_Name', 'Kafka_Topic', 'Format', 'Query'])
            for table_name, info in lineage['tables'].items():
                writer.writerow([table_name, info['topic'], info['format'], info['query']])
        print(f"Tables exported to: {tables_filename}")

    def export_lineage_json(self, lineage, filename: str):
        """Export lineage as JSON file"""
        with open(filename, 'w') as f:
            json.dump(lineage, f, indent=2, default=str)
        print(f"JSON exported to: {filename}")

def main():
    parser = argparse.ArgumentParser(description='Offline ksqlDB Lineage Tool - Enhanced Debugging')
    parser.add_argument('--url', required=True, help='ksqlDB server URL')
    parser.add_argument('--username', help='Username for basic auth') 
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    parser.add_argument('--no-ssl-verify', action='store_true', help='Disable SSL verification')
    parser.add_argument('--ca-cert', help='Path to custom CA certificate file')
    parser.add_argument('--export-csv', help='Export to CSV files (base filename)')
    parser.add_argument('--export-json', help='Export to JSON file')
    parser.add_argument('--debug-structure', action='store_true', help='Debug response structure only')
    
    args = parser.parse_args()
    
    ksql_client = KsqlDBLineageOffline(
        ksql_url=args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key, 
        api_secret=args.api_secret,
        verify_ssl=not args.no_ssl_verify,
        ca_cert=args.ca_cert
    )
    
    if args.debug_structure:
        ksql_client.debug_response_structure()
    else:
        lineage = ksql_client.build_lineage()
        ksql_client.print_lineage_report(lineage)
        
        if args.export_csv:
            ksql_client.export_to_csv(lineage, args.export_csv)
        
        if args.export_json:
            ksql_client.export_lineage_json(lineage, args.export_json)

if __name__ == "__main__":
    main()
