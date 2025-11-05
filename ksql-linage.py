import json
import csv
import argparse
import re
import base64
from datetime import datetime
import os
import sys
from collections import defaultdict
try:
    from urllib.request import Request, urlopen
    from urllib.error import URLError, HTTPError
    import ssl
except ImportError:
    pass

class KsqlDBLineageEnhanced:
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
            
            if self.username and self.password:
                auth_string = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
                headers["Authorization"] = f"Basic {auth_string}"
            elif self.api_key and self.api_secret:
                auth_string = base64.b64encode(f"{self.api_key}:{self.api_secret}".encode()).decode()
                headers["Authorization"] = f"Basic {auth_string}"
            
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
                print(f"DEBUG: Response type: {type(parsed_result)}")
                if isinstance(parsed_result, list):
                    print(f"DEBUG: Response list length: {len(parsed_result)}")
                return parsed_result
            else:
                print(f"Error: HTTP {response.getcode()}")
                return None
                
        except Exception as e:
            print(f"Error: {e}")
            return None

    def debug_response_structure(self):
        """Debug the actual response structure"""
        test_queries = [
            "SHOW STREAMS;",
            "SHOW STREAMS EXTENDED;", 
            "SHOW QUERIES;",
            "SHOW QUERIES EXTENDED;"
        ]
        
        for query in test_queries:
            print(f"\n{'='*60}")
            print(f"DEBUG: Testing query: {query}")
            print(f"{'='*60}")
            result = self.execute_ksql(query)
            if result:
                print("DEBUG: Full response structure:")
                print(json.dumps(result, indent=2, default=str))

    def parse_show_response(self, response, entity_type: str):
        """Parse SHOW STREAMS/TABLES/QUERIES response with debugging"""
        print(f"DEBUG: Parsing {entity_type} from response type: {type(response)}")
        
        entities = []
        
        if isinstance(response, list):
            print(f"DEBUG: Response is a list with {len(response)} items")
            for i, item in enumerate(response):
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
        else:
            print(f"DEBUG: No {entity_type} found in response!")
        
        return entities

    def parse_dependencies(self, sql: str, query_id: str):
        """Parse SQL to extract source and target relationships with debugging"""
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

    def build_comprehensive_lineage(self):
        """Build comprehensive lineage with relationships and debugging"""
        print("Building comprehensive ksqlDB lineage...")
        
        lineage = {
            "streams": {},
            "tables": {}, 
            "queries": {},
            "dependencies": [],
            "relationships": {
                "stream_to_stream": [],
                "stream_to_table": [],
                "table_to_stream": [],
                "table_to_table": [],
                "query_relationships": []
            },
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "ksql_url": self.ksql_url
            }
        }
        
        # Get all objects with debugging
        print("\n" + "="*50)
        print("FETCHING STREAMS")
        print("="*50)
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
                    print(f"DEBUG: Added stream: {stream_name}")
        
        print("\n" + "="*50)
        print("FETCHING TABLES")
        print("="*50)
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
                    print(f"DEBUG: Added table: {table_name}")
        
        print("\n" + "="*50)
        print("FETCHING QUERIES")
        print("="*50)
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
                    print(f"DEBUG: Added query: {query_id}")
                    
                    dependencies = self.parse_dependencies(sql_text, query_id)
                    lineage['dependencies'].extend(dependencies)
        
        print(f"\nDEBUG: Summary before relationship building:")
        print(f"  Streams: {len(lineage['streams'])}")
        print(f"  Tables: {len(lineage['tables'])}")
        print(f"  Queries: {len(lineage['queries'])}")
        print(f"  Dependencies: {len(lineage['dependencies'])}")
        
        # Build comprehensive relationships
        self._build_relationships(lineage)
        return lineage

    def _build_relationships(self, lineage):
        """Build comprehensive relationship mapping"""
        relationships = lineage['relationships']
        
        # Build object type mapping
        object_types = {}
        for stream in lineage['streams']:
            object_types[stream] = 'STREAM'
        for table in lineage['tables']:
            object_types[table] = 'TABLE'
        
        print(f"DEBUG: Object types mapping: {object_types}")
        print(f"DEBUG: Processing {len(lineage['dependencies'])} dependencies...")
        
        # Analyze each dependency
        for dep in lineage['dependencies']:
            source = dep['source']
            target = dep['target']
            query_id = dep['query_id']
            
            source_type = object_types.get(source, 'EXTERNAL')
            target_type = object_types.get(target, 'UNKNOWN')
            
            print(f"DEBUG: Dependency: {source}({source_type}) -> {target}({target_type}) via {query_id}")
            
            # Categorize relationships
            if source_type == 'STREAM' and target_type == 'STREAM':
                relationships['stream_to_stream'].append({
                    'source_stream': source,
                    'target_stream': target,
                    'query_id': query_id,
                    'relationship_type': dep['type']
                })
                print(f"DEBUG: Added STREAM->STREAM relationship")
            elif source_type == 'STREAM' and target_type == 'TABLE':
                relationships['stream_to_table'].append({
                    'source_stream': source,
                    'target_table': target,
                    'query_id': query_id,
                    'relationship_type': dep['type']
                })
                print(f"DEBUG: Added STREAM->TABLE relationship")
            elif source_type == 'TABLE' and target_type == 'STREAM':
                relationships['table_to_stream'].append({
                    'source_table': source,
                    'target_stream': target,
                    'query_id': query_id,
                    'relationship_type': dep['type']
                })
                print(f"DEBUG: Added TABLE->STREAM relationship")
            elif source_type == 'TABLE' and target_type == 'TABLE':
                relationships['table_to_table'].append({
                    'source_table': source,
                    'target_table': target,
                    'query_id': query_id,
                    'relationship_type': dep['type']
                })
                print(f"DEBUG: Added TABLE->TABLE relationship")
            
            # Build query relationships
            relationships['query_relationships'].append({
                'query_id': query_id,
                'input_object': source,
                'input_type': source_type,
                'output_object': target,
                'output_type': target_type,
                'operation': dep['type']
            })
        
        print(f"DEBUG: Relationship building complete:")
        print(f"  Stream->Stream: {len(relationships['stream_to_stream'])}")
        print(f"  Stream->Table: {len(relationships['stream_to_table'])}")
        print(f"  Table->Stream: {len(relationships['table_to_stream'])}")
        print(f"  Table->Table: {len(relationships['table_to_table'])}")
        print(f"  Query Relationships: {len(relationships['query_relationships'])}")

    def print_relationship_report(self, lineage):
        """Print comprehensive relationship report"""
        print("\n" + "=" * 120)
        print("🔗 KSQLDB COMPREHENSIVE RELATIONSHIP REPORT")
        print("=" * 120)
        
        rel = lineage['relationships']
        
        # Summary
        print(f"\n📊 RELATIONSHIP SUMMARY")
        print("-" * 60)
        print(f"  Streams: {len(lineage['streams']):>3} | Tables: {len(lineage['tables']):>3} | Queries: {len(lineage['queries']):>3}")
        print(f"  Stream → Stream: {len(rel['stream_to_stream']):>3}")
        print(f"  Stream → Table:  {len(rel['stream_to_table']):>3}") 
        print(f"  Table → Stream:  {len(rel['table_to_stream']):>3}")
        print(f"  Table → Table:   {len(rel['table_to_table']):>3}")
        print(f"  Total Data Flows: {len(lineage['dependencies']):>3}")
        
        # Stream to Stream Relationships
        if rel['stream_to_stream']:
            print(f"\n🔄 STREAM TO STREAM RELATIONSHIPS ({len(rel['stream_to_stream'])})")
            print("-" * 100)
            print(f"  {'SOURCE STREAM':<30} {'→':^5} {'TARGET STREAM':<30} {'QUERY':<20} {'TYPE':<15}")
            print("  " + "-" * 98)
            for rel_item in sorted(rel['stream_to_stream'], key=lambda x: (x['source_stream'], x['target_stream'])):
                print(f"  {rel_item['source_stream']:<30} {'→':^5} {rel_item['target_stream']:<30} {rel_item['query_id']:<20} {rel_item['relationship_type']:<15}")
        else:
            print(f"\n🔄 STREAM TO STREAM RELATIONSHIPS (0)")
            print("  No stream-to-stream relationships found")
        
        # Stream to Table Relationships
        if rel['stream_to_table']:
            print(f"\n📥 STREAM TO TABLE RELATIONSHIPS ({len(rel['stream_to_table'])})")
            print("-" * 100)
            print(f"  {'SOURCE STREAM':<30} {'→':^5} {'TARGET TABLE':<30} {'QUERY':<20} {'TYPE':<15}")
            print("  " + "-" * 98)
            for rel_item in sorted(rel['stream_to_table'], key=lambda x: (x['source_stream'], x['target_table'])):
                print(f"  {rel_item['source_stream']:<30} {'→':^5} {rel_item['target_table']:<30} {rel_item['query_id']:<20} {rel_item['relationship_type']:<15}")
        else:
            print(f"\n📥 STREAM TO TABLE RELATIONSHIPS (0)")
            print("  No stream-to-table relationships found")
        
        # Table to Stream Relationships
        if rel['table_to_stream']:
            print(f"\n📤 TABLE TO STREAM RELATIONSHIPS ({len(rel['table_to_stream'])})")
            print("-" * 100)
            print(f"  {'SOURCE TABLE':<30} {'→':^5} {'TARGET STREAM':<30} {'QUERY':<20} {'TYPE':<15}")
            print("  " + "-" * 98)
            for rel_item in sorted(rel['table_to_stream'], key=lambda x: (x['source_table'], x['target_stream'])):
                print(f"  {rel_item['source_table']:<30} {'→':^5} {rel_item['target_stream']:<30} {rel_item['query_id']:<20} {rel_item['relationship_type']:<15}")
        else:
            print(f"\n📤 TABLE TO STREAM RELATIONSHIPS (0)")
            print("  No table-to-stream relationships found")
        
        # Table to Table Relationships
        if rel['table_to_table']:
            print(f"\n🔄 TABLE TO TABLE RELATIONSHIPS ({len(rel['table_to_table'])})")
            print("-" * 100)
            print(f"  {'SOURCE TABLE':<30} {'→':^5} {'TARGET TABLE':<30} {'QUERY':<20} {'TYPE':<15}")
            print("  " + "-" * 98)
            for rel_item in sorted(rel['table_to_table'], key=lambda x: (x['source_table'], x['target_table'])):
                print(f"  {rel_item['source_table']:<30} {'→':^5} {rel_item['target_table']:<30} {rel_item['query_id']:<20} {rel_item['relationship_type']:<15}")
        else:
            print(f"\n🔄 TABLE TO TABLE RELATIONSHIPS (0)")
            print("  No table-to-table relationships found")
        
        # Query Relationships (Complete View)
        if rel['query_relationships']:
            print(f"\n🔧 QUERY RELATIONSHIPS - COMPLETE VIEW ({len(rel['query_relationships'])})")
            print("-" * 120)
            print(f"  {'QUERY ID':<20} {'INPUT':<25} {'INPUT TYPE':<12} {'→':^5} {'OUTPUT':<25} {'OUTPUT TYPE':<12} {'OPERATION':<15}")
            print("  " + "-" * 118)
            for rel_item in sorted(rel['query_relationships'], key=lambda x: x['query_id']):
                print(f"  {rel_item['query_id']:<20} {rel_item['input_object']:<25} {rel_item['input_type']:<12} {'→':^5} {rel_item['output_object']:<25} {rel_item['output_type']:<12} {rel_item['operation']:<15}")
        else:
            print(f"\n🔧 QUERY RELATIONSHIPS - COMPLETE VIEW (0)")
            print("  No query relationships found")

    def export_relationship_csv(self, lineage, base_filename: str):
        """Export comprehensive relationship data to CSV files"""
        
        rel = lineage['relationships']
        
        # 1. Complete Relationship Master File
        master_filename = f"{base_filename}_complete_relationships.csv"
        with open(master_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Relationship_Type', 'Source_Name', 'Source_Type', 'Target_Name', 'Target_Type', 'Query_ID', 'Operation_Type'])
            
            # Stream to Stream
            for r in rel['stream_to_stream']:
                writer.writerow(['STREAM_TO_STREAM', r['source_stream'], 'STREAM', r['target_stream'], 'STREAM', r['query_id'], r['relationship_type']])
            
            # Stream to Table
            for r in rel['stream_to_table']:
                writer.writerow(['STREAM_TO_TABLE', r['source_stream'], 'STREAM', r['target_table'], 'TABLE', r['query_id'], r['relationship_type']])
            
            # Table to Stream
            for r in rel['table_to_stream']:
                writer.writerow(['TABLE_TO_STREAM', r['source_table'], 'TABLE', r['target_stream'], 'STREAM', r['query_id'], r['relationship_type']])
            
            # Table to Table
            for r in rel['table_to_table']:
                writer.writerow(['TABLE_TO_TABLE', r['source_table'], 'TABLE', r['target_table'], 'TABLE', r['query_id'], r['relationship_type']])
        
        print(f"✓ Complete relationships exported to: {master_filename}")
        
        # 2. Query Relationship Details
        query_filename = f"{base_filename}_query_relationships.csv"
        with open(query_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Query_ID', 'Input_Object', 'Input_Type', 'Output_Object', 'Output_Type', 'Operation'])
            
            for r in rel['query_relationships']:
                writer.writerow([r['query_id'], r['input_object'], r['input_type'], r['output_object'], r['output_type'], r['operation']])
        
        print(f"✓ Query relationships exported to: {query_filename}")
        
        # 3. Object Dependencies (What depends on what)
        deps_filename = f"{base_filename}_object_dependencies.csv"
        with open(deps_filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Object_Name', 'Object_Type', 'Depends_On', 'Required_By', 'Dependency_Count'])
            
            # Build dependency graph
            dependencies = defaultdict(list)
            dependents = defaultdict(list)
            
            for r in rel['query_relationships']:
                dependencies[r['output_object']].append(r['input_object'])
                dependents[r['input_object']].append(r['output_object'])
            
            all_objects = set(dependencies.keys()) | set(dependents.keys())
            for obj in sorted(all_objects):
                obj_type = 'STREAM' if obj in lineage['streams'] else 'TABLE' if obj in lineage['tables'] else 'EXTERNAL'
                deps_list = ', '.join(dependencies[obj])
                required_by_list = ', '.join(dependents[obj])
                count = len(dependencies[obj])
                
                writer.writerow([obj, obj_type, deps_list, required_by_list, count])
        
        print(f"✓ Object dependencies exported to: {deps_filename}")

    def export_lineage_json(self, lineage, filename: str):
        """Export complete lineage as JSON file"""
        with open(filename, 'w') as f:
            json.dump(lineage, f, indent=2, default=str)
        print(f"✓ Complete lineage exported to: {filename}")

def main():
    parser = argparse.ArgumentParser(description='Comprehensive ksqlDB Relationship Analysis')
    parser.add_argument('--url', required=True, help='ksqlDB server URL')
    parser.add_argument('--username', help='Username for basic auth') 
    parser.add_argument('--password', help='Password for basic auth')
    parser.add_argument('--api-key', help='API key for Confluent Cloud')
    parser.add_argument('--api-secret', help='API secret for Confluent Cloud')
    parser.add_argument('--no-ssl-verify', action='store_true', help='Disable SSL verification')
    parser.add_argument('--ca-cert', help='Path to custom CA certificate file')
    parser.add_argument('--export-csv', help='Export relationship CSVs (base filename)')
    parser.add_argument('--export-json', help='Export complete lineage to JSON file')
    parser.add_argument('--debug-structure', action='store_true', help='Debug response structure only')
    
    args = parser.parse_args()
    
    ksql_client = KsqlDBLineageEnhanced(
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
        lineage = ksql_client.build_comprehensive_lineage()
        ksql_client.print_relationship_report(lineage)
        
        if args.export_csv:
            ksql_client.export_relationship_csv(lineage, args.export_csv)
        
        if args.export_json:
            ksql_client.export_lineage_json(lineage, args.export_json)

if __name__ == "__main__":
    main()
