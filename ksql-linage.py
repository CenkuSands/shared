import json
import csv
import argparse
import re
import base64
from datetime import datetime
import os
import sys
try:
    # Try to use urllib from standard library instead of requests
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
                return json.loads(result)
            else:
                print(f"Error: HTTP {response.getcode()}")
                return None
                
        except HTTPError as e:
            print(f"HTTP Error: {e.code} - {e.reason}")
            print(f"Response: {e.read().decode()}")
            return None
        except URLError as e:
            print(f"URL Error: {e.reason}")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def parse_show_response(self, response, entity_type: str):
        """Parse SHOW STREAMS/TABLES/QUERIES response"""
        entities = []
        
        if isinstance(response, list):
            for item in response:
                if isinstance(item, dict):
                    if entity_type in item:
                        if isinstance(item[entity_type], list):
                            entities.extend(item[entity_type])
                        else:
                            entities.append(item[entity_type])
                    elif 'name' in item and ('topic' in item or 'queryString' in item):
                        entities.append(item)
        
        elif isinstance(response, dict):
            if entity_type in response:
                if isinstance(response[entity_type], list):
                    entities.extend(response[entity_type])
                else:
                    entities.append(response[entity_type])
        
        return entities

    def parse_dependencies(self, sql: str, query_id: str):
        """Parse SQL to extract source and target relationships"""
        dependencies = []
        if not sql:
            return dependencies
            
        sql_upper = sql.upper().replace('\n', ' ').replace('\r', ' ')
        
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
        
        return dependencies

    def build_lineage(self):
        """Build complete lineage graph"""
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
                    sql_text = query.get('sql', '')
                    lineage['queries'][query_id] = {
                        'sql': sql_text,
                        'status': query.get('status', ''),
                        'sources': query.get('sources', []),
                        'sinks': query.get('sinks', [])
                    }
                    
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
        
        # Summary Section
        print(f"\nSUMMARY")
        print("-" * 50)
        print(f"  Streams: {len(lineage['streams'])}")
        print(f"  Tables:  {len(lineage['tables'])}")
        print(f"  Queries: {len(lineage['queries'])}")
        print(f"  Data Flows: {len(lineage['dependencies'])}")
        print(f"  Generated: {lineage['metadata']['generated_at']}")
        
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
    parser = argparse.ArgumentParser(description='Offline ksqlDB Lineage Tool - No External Dependencies')
    parser.add_argument('--url', required=True, help='ksqlDB server URL')
    parser.add_argument('--username', help='Username for basic auth') 
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    parser.add_argument('--no-ssl-verify', action='store_true', help='Disable SSL verification')
    parser.add_argument('--ca-cert', help='Path to custom CA certificate file')
    parser.add_argument('--export-csv', help='Export to CSV files (base filename)')
    parser.add_argument('--export-json', help='Export to JSON file')
    
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
    
    lineage = ksql_client.build_lineage()
    ksql_client.print_lineage_report(lineage)
    
    if args.export_csv:
        ksql_client.export_to_csv(lineage, args.export_csv)
    
    if args.export_json:
        ksql_client.export_lineage_json(lineage, args.export_json)

if __name__ == "__main__":
    main()
