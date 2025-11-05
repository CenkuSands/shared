import requests
import json
from typing import Dict, List, Any
import argparse

class KsqlDBLineage:
    def __init__(self, ksql_url: str, username: str = None, password: str = None, api_key: str = None, api_secret: str = None):
        self.ksql_url = f"{ksql_url}/ksql"
        self.headers = {"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"}
        self.auth = None
        
        # Basic Authentication
        if username and password:
            self.auth = (username, password)
        
        # API Key/Secret (for Confluent Cloud)
        elif api_key and api_secret:
            self.headers.update({
                "Authorization": f"Basic {api_key}:{api_secret}"
            })
    
    def execute_ksql(self, ksql: str) -> Dict[str, Any]:
        """Execute ksqlDB query and return results"""
        try:
            response = requests.post(
                self.ksql_url,
                headers=self.headers,
                auth=self.auth,
                json={"ksql": ksql, "streamsProperties": {}},
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error executing ksql: {e}")
            return {}
    
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
        
        # Get all objects
        streams_result = self.execute_ksql("SHOW STREAMS EXTENDED;")
        tables_result = self.execute_ksql("SHOW TABLES EXTENDED;")
        queries_result = self.execute_ksql("SHOW QUERIES EXTENDED;")
        
        lineage = {
            "streams": {},
            "tables": {},
            "queries": {},
            "dependencies": [],
            "lineage_graph": {}
        }
        
        # Process streams
        if streams_result and 'streams' in streams_result[0]:
            for stream in streams_result[0]['streams']:
                stream_name = stream['name']
                lineage['streams'][stream_name] = {
                    'type': 'STREAM',
                    'topic': stream.get('topic', ''),
                    'format': stream.get('format', ''),
                    'query': stream.get('query', '')
                }
        
        # Process tables
        if tables_result and 'tables' in tables_result[0]:
            for table in tables_result[0]['tables']:
                table_name = table['name']
                lineage['tables'][table_name] = {
                    'type': 'TABLE',
                    'topic': table.get('topic', ''),
                    'format': table.get('format', ''),
                    'query': table.get('query', '')
                }
        
        # Process queries and build dependencies
        if queries_result and 'queries' in queries_result[0]:
            for query in queries_result[0]['queries']:
                query_id = query['id']
                sql_text = query['sql']
                
                lineage['queries'][query_id] = {
                    'sql': sql_text,
                    'status': query.get('status', ''),
                    'sources': query.get('sources', []),
                    'sinks': query.get('sinks', [])
                }
                
                # Parse dependencies from SQL
                dependencies = self.parse_dependencies(sql_text, query_id)
                lineage['dependencies'].extend(dependencies)
        
        # Build lineage graph
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

def main():
    parser = argparse.ArgumentParser(description='Extract ksqlDB lineage')
    parser.add_argument('--url', required=True, help='ksqlDB server URL (e.g., http://localhost:8088)')
    parser.add_argument('--username', help='Username for basic auth')
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    
    args = parser.parse_args()
    
    # Initialize ksqlDB client
    ksql_client = KsqlDBLineage(
        ksql_url=args.url,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
        api_secret=args.api_secret
    )
    
    # Build and print lineage
    lineage = ksql_client.build_lineage()
    ksql_client.print_lineage_report(lineage)

if __name__ == "__main__":
    main()
