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
            print(f"Executing: {ksql}")
            
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
                return json.loads(result)
            else:
                print(f"Error: HTTP {response.getcode()}")
                return None
                
        except Exception as e:
            print(f"Error: {e}")
            return None

    def parse_show_response(self, response, entity_type: str):
        """Parse SHOW STREAMS/TABLES/QUERIES response - fixed for your ksqlDB format"""
        print(f"DEBUG: Parsing {entity_type} from response type: {type(response)}")
        
        entities = []
        
        if isinstance(response, list):
            print(f"DEBUG: Response is a list with {len(response)} items")
            for i, item in enumerate(response):
                if isinstance(item, dict):
                    print(f"DEBUG: Item {i} keys: {list(item.keys())}")
                    
                    # Your ksqlDB format: streams are directly in the list items
                    if 'type' in item and item['type'] in ['STREAM', 'TABLE']:
                        print(f"DEBUG: Found direct {item['type']} entity: {item.get('name')}")
                        entities.append(item)
                    
                    # Also check for nested entities (some ksqlDB versions)
                    elif entity_type in item and isinstance(item[entity_type], list):
                        print(f"DEBUG: Found {entity_type} list with {len(item[entity_type])} items")
                        entities.extend(item[entity_type])
        
        elif isinstance(response, dict):
            print(f"DEBUG: Response is a dict with keys: {list(response.keys())}")
            if entity_type in response and isinstance(response[entity_type], list):
                entities.extend(response[entity_type])
        
        print(f"DEBUG: Found {len(entities)} {entity_type} entities")
        return entities

    def extract_entity_info(self, entity, entity_type: str):
        """Extract standardized information from entity objects"""
        if not isinstance(entity, dict):
            return None
            
        name = entity.get('name')
        if not name:
            return None
        
        # Your ksqlDB uses different field names
        topic = entity.get('topic', '')
        format_val = entity.get('valueFormat', entity.get('format', ''))
        key_format = entity.get('keyFormat', '')
        
        return {
            'name': name,
            'type': entity_type,
            'topic': topic,
            'format': format_val,
            'key_format': key_format,
            'is_windowed': entity.get('isWindowed', False)
        }

    def parse_dependencies_from_sql(self, sql: str, query_id: str):
        """Parse SQL to extract source and target relationships"""
        dependencies = []
        if not sql:
            return dependencies
            
        # Clean and normalize SQL
        sql_clean = ' '.join(sql.split()).upper()
        print(f"DEBUG: Parsing SQL for {query_id}: {sql_clean[:200]}...")
        
        # Pattern 1: CREATE STREAM/TABLE ... AS SELECT ... FROM ...
        create_pattern = r'CREATE\s+(TABLE|STREAM)\s+(\w+)\s+AS\s+SELECT\s+.*?\s+FROM\s+(\w+)'
        matches = re.findall(create_pattern, sql_clean, re.IGNORECASE | re.DOTALL)
        for match in matches:
            if len(match) == 3:
                object_type, target, source = match
                dependencies.append({
                    "source": source,
                    "target": target,
                    "query_id": query_id,
                    "type": f"CREATE_{object_type}"
                })
                print(f"DEBUG: Found CREATE dependency: {source} -> {target}")
        
        # Pattern 2: INSERT INTO ... SELECT ... FROM ...
        insert_pattern = r'INSERT\s+INTO\s+(\w+)\s+SELECT\s+.*?\s+FROM\s+(\w+)'
        matches = re.findall(insert_pattern, sql_clean, re.IGNORECASE | re.DOTALL)
        for match in matches:
            if len(match) == 2:
                target, source = match
                dependencies.append({
                    "source": source,
                    "target": target,
                    "query_id": query_id,
                    "type": "INSERT_INTO"
                })
                print(f"DEBUG: Found INSERT dependency: {source} -> {target}")
        
        # Pattern 3: CREATE STREAM/TABLE with WITH properties (source)
        if not dependencies:
            create_source_pattern = r'CREATE\s+(TABLE|STREAM)\s+(\w+)\s+WITH\s*\('
            matches = re.findall(create_source_pattern, sql_clean)
            for match in matches:
                if len(match) == 2:
                    object_type, name = match
                    dependencies.append({
                        "source": "EXTERNAL_SOURCE",
                        "target": name,
                        "query_id": query_id,
                        "type": f"SOURCE_{object_type}"
                    })
                    print(f"DEBUG: Found SOURCE creation: EXTERNAL -> {name}")
        
        print(f"DEBUG: Found {len(dependencies)} dependencies in SQL")
        return dependencies

    def build_comprehensive_lineage(self):
        """Build comprehensive lineage with fixed parsing"""
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
        
        # Get streams
        print("\n" + "="*60)
        print("STEP 1: FETCHING STREAMS")
        print("="*60)
        streams_result = self.execute_ksql("SHOW STREAMS EXTENDED;")
        if streams_result:
            streams = self.parse_show_response(streams_result, 'streams')
            for stream in streams:
                info = self.extract_entity_info(stream, 'STREAM')
                if info and info['name']:
                    lineage['streams'][info['name']] = info
                    print(f"✓ Added stream: {info['name']}")
        
        # If no streams found with EXTENDED, try without
        if not lineage['streams']:
            print("Trying SHOW STREAMS without EXTENDED...")
            streams_result = self.execute_ksql("SHOW STREAMS;")
            if streams_result:
                streams = self.parse_show_response(streams_result, 'streams')
                for stream in streams:
                    info = self.extract_entity_info(stream, 'STREAM')
                    if info and info['name']:
                        lineage['streams'][info['name']] = info
                        print(f"✓ Added stream: {info['name']}")
        
        # Get tables  
        print("\n" + "="*60)
        print("STEP 2: FETCHING TABLES")
        print("="*60)
        tables_result = self.execute_ksql("SHOW TABLES EXTENDED;")
        if tables_result:
            tables = self.parse_show_response(tables_result, 'tables')
            for table in tables:
                info = self.extract_entity_info(table, 'TABLE')
                if info and info['name']:
                    lineage['tables'][info['name']] = info
                    print(f"✓ Added table: {info['name']}")
        
        # If no tables found with EXTENDED, try without
        if not lineage['tables']:
            print("Trying SHOW TABLES without EXTENDED...")
            tables_result = self.execute_ksql("SHOW TABLES;")
            if tables_result:
                tables = self.parse_show_response(tables_result, 'tables')
                for table in tables:
                    info = self.extract_entity_info(table, 'TABLE')
                    if info and info['name']:
                        lineage['tables'][info['name']] = info
                        print(f"✓ Added table: {info['name']}")
        
        # Get queries
        print("\n" + "="*60)
        print("STEP 3: FETCHING QUERIES")
        print("="*60)
        queries_result = self.execute_ksql("SHOW QUERIES EXTENDED;")
        if queries_result:
            queries = self.parse_show_response(queries_result, 'queries')
            for query in queries:
                query_id = query.get('id', query.get('queryId', ''))
                if query_id:
                    sql_text = query.get('sql', query.get('queryString', ''))
                    
                    lineage['queries'][query_id] = {
                        'sql': sql_text,
                        'status': query.get('status', query.get('state', '')),
                        'sources': query.get('sources', []),
                        'sinks': query.get('sinks', [])
                    }
                    print(f"✓ Added query: {query_id}")
                    
                    # Parse dependencies from SQL
                    dependencies = self.parse_dependencies_from_sql(sql_text, query_id)
                    lineage['dependencies'].extend(dependencies)
        
        # If no queries found with EXTENDED, try without
        if not lineage['queries']:
            print("Trying SHOW QUERIES without EXTENDED...")
            queries_result = self.execute_ksql("SHOW QUERIES;")
            if queries_result:
                queries = self.parse_show_response(queries_result, 'queries')
                for query in queries:
                    query_id = query.get('id', query.get('queryId', ''))
                    if query_id:
                        sql_text = query.get('sql', query.get('queryString', ''))
                        
                        lineage['queries'][query_id] = {
                            'sql': sql_text,
                            'status': query.get('status', query.get('state', '')),
                            'sources': query.get('sources', []),
                            'sinks': query.get('sinks', [])
                        }
                        print(f"✓ Added query: {query_id}")
                        
                        # Parse dependencies from SQL
                        dependencies = self.parse_dependencies_from_sql(sql_text, query_id)
                        lineage['dependencies'].extend(dependencies)
        
        print(f"\n" + "="*60)
        print("COLLECTION SUMMARY")
        print("="*60)
        print(f"Streams: {len(lineage['streams'])}")
        print(f"Tables: {len(lineage['tables'])}")
        print(f"Queries: {len(lineage['queries'])}")
        print(f"Dependencies: {len(lineage['dependencies'])}")
        
        # Build comprehensive relationships
        if lineage['dependencies']:
            print(f"\nBuilding relationships from {len(lineage['dependencies'])} dependencies...")
            self._build_relationships(lineage)
        else:
            print(f"\nNo dependencies found to build relationships")
            
        return lineage

    def _build_relationships(self, lineage):
        """Build comprehensive relationship mapping"""
        relationships = lineage['relationships']
        
        # Build object type mapping
        object_types = {}
        for stream_name in lineage['streams']:
            object_types[stream_name] = 'STREAM'
        for table_name in lineage['tables']:
            object_types[table_name] = 'TABLE'
        
        print(f"Object type mapping: {len(object_types)} objects")
        
        # Analyze each dependency
        for dep in lineage['dependencies']:
            source = dep['source']
            target = dep['target']
            query_id = dep['query_id']
            
            source_type = object_types.get(source, 'EXTERNAL')
            target_type = object_types.get(target, 'UNKNOWN')
            
            print(f"Relationship: {source}({source_type}) → {target}({target_type}) via {query_id}")
            
            # Categorize relationships
            relationship_data = {
                'source_object': source,
                'target_object': target, 
                'query_id': query_id,
                'relationship_type': dep['type']
            }
            
            if source_type == 'STREAM' and target_type == 'STREAM':
                relationships['stream_to_stream'].append(relationship_data)
            elif source_type == 'STREAM' and target_type == 'TABLE':
                relationships['stream_to_table'].append(relationship_data)
            elif source_type == 'TABLE' and target_type == 'STREAM':
                relationships['table_to_stream'].append(relationship_data)
            elif source_type == 'TABLE' and target_type == 'TABLE':
                relationships['table_to_table'].append(relationship_data)
            
            # Build query relationships
            relationships['query_relationships'].append({
                'query_id': query_id,
                'input_object': source,
                'input_type': source_type,
                'output_object': target,
                'output_type': target_type,
                'operation': dep['type']
            })
        
        print(f"Relationship building complete:")
        print(f"  Stream→Stream: {len(relationships['stream_to_stream'])}")
        print(f"  Stream→Table:  {len(relationships['stream_to_table'])}")
        print(f"  Table→Stream:  {len(relationships['table_to_stream'])}")
        print(f"  Table→Table:   {len(relationships['table_to_table'])}")

    def print_relationship_report(self, lineage):
        """Print comprehensive relationship report"""
        print("\n" + "=" * 100)
        print("FINAL RELATIONSHIP REPORT")
        print("=" * 100)
        
        rel = lineage['relationships']
        
        # Summary
        print(f"\nSUMMARY")
        print("-" * 50)
        print(f"Streams: {len(lineage['streams'])}")
        print(f"Tables:  {len(lineage['tables'])}")
        print(f"Queries: {len(lineage['queries'])}")
        print(f"Dependencies found: {len(lineage['dependencies'])}")
        
        # Show what objects we found
        if lineage['streams']:
            print(f"\nSTREAMS FOUND:")
            for stream in sorted(lineage['streams'].keys()):
                print(f"  - {stream}")
        
        if lineage['tables']:
            print(f"\nTABLES FOUND:")
            for table in sorted(lineage['tables'].keys()):
                print(f"  - {table}")
        
        if lineage['queries']:
            print(f"\nQUERIES FOUND:")
            for query in sorted(lineage['queries'].keys()):
                print(f"  - {query}")
        
        # Show relationships if any
        all_relationships = (rel['stream_to_stream'] + rel['stream_to_table'] + 
                           rel['table_to_stream'] + rel['table_to_table'])
        
        if all_relationships:
            print(f"\nALL RELATIONSHIPS ({len(all_relationships)}):")
            print("-" * 80)
            for rel_item in sorted(all_relationships, key=lambda x: (x['source_object'], x['target_object'])):
                source_type = 'STREAM' if rel_item['source_object'] in lineage['streams'] else 'TABLE' if rel_item['source_object'] in lineage['tables'] else 'EXTERNAL'
                target_type = 'STREAM' if rel_item['target_object'] in lineage['streams'] else 'TABLE' if rel_item['target_object'] in lineage['tables'] else 'UNKNOWN'
                
                print(f"  {rel_item['source_object']} ({source_type}) → {rel_item['target_object']} ({target_type}) via {rel_item['query_id']}")
        else:
            print(f"\nNO RELATIONSHIPS FOUND")
            if lineage['dependencies']:
                print("Dependencies were found but couldn't be mapped to relationships.")
                print("This might be because the source/target objects don't exist as streams/tables.")
            else:
                print("No CREATE...AS SELECT or INSERT INTO statements found in queries.")

    def export_relationship_csv(self, lineage, base_filename: str):
        """Export relationship data to CSV files"""
        
        rel = lineage['relationships']
        all_relationships = (rel['stream_to_stream'] + rel['stream_to_table'] + 
                           rel['table_to_stream'] + rel['table_to_table'])
        
        if all_relationships:
            # Master relationships file
            master_filename = f"{base_filename}_relationships.csv"
            with open(master_filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Source', 'Source_Type', 'Target', 'Target_Type', 'Query_ID', 'Operation'])
                
                for r in all_relationships:
                    source_type = 'STREAM' if r['source_object'] in lineage['streams'] else 'TABLE' if r['source_object'] in lineage['tables'] else 'EXTERNAL'
                    target_type = 'STREAM' if r['target_object'] in lineage['streams'] else 'TABLE' if r['target_object'] in lineage['tables'] else 'UNKNOWN'
                    
                    writer.writerow([
                        r['source_object'], 
                        source_type, 
                        r['target_object'], 
                        target_type, 
                        r['query_id'], 
                        r['relationship_type']
                    ])
            
            print(f"✓ Relationships exported to: {master_filename}")
            
            # Object inventory
            inventory_filename = f"{base_filename}_inventory.csv"
            with open(inventory_filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Name', 'Type', 'Topic', 'Format', 'Key_Format'])
                
                for stream_name, info in lineage['streams'].items():
                    writer.writerow([stream_name, 'STREAM', info['topic'], info['format'], info.get('key_format', '')])
                
                for table_name, info in lineage['tables'].items():
                    writer.writerow([table_name, 'TABLE', info['topic'], info['format'], info.get('key_format', '')])
            
            print(f"✓ Object inventory exported to: {inventory_filename}")
        else:
            print("No relationships to export")

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
    
    lineage = ksql_client.build_comprehensive_lineage()
    ksql_client.print_relationship_report(lineage)
    
    if args.export_csv:
        ksql_client.export_relationship_csv(lineage, args.export_csv)

if __name__ == "__main__":
    main()
