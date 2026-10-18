import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from flask_cors import CORS

# --- Configuration ---
# Get DB URL from environment variable (standard for cloud hosting)
# Locally, you can set this in your terminal or hardcode for testing
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgres://user:password@host:port/database')
RESULTS_PER_PAGE = 20

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)

# --- Database Connection ---
def get_db_conn():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

# --- API Endpoint: /api/search ---
@app.route('/api/search')
def search_providers():
    try:
        # 1. Get Query Parameters
        user_lat = float(request.args.get('lat'))
        user_lon = float(request.args.get('lon'))
        radius = float(request.args.get('radius', 25))
        taxonomy = request.args.get('taxonomy', "")
        offset = int(request.args.get('offset', 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid query parameters."}), 400

    conn = get_db_conn()
    try:
        # Use RealDictCursor to get results as dictionaries (like SQLite Row)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 2. Build Query
        # PostgreSQL supports native math functions (radians, acos, cos, sin)
        # 3958.8 is Earth's radius in miles
        
        haversine = """
        (3958.8 * acos(
            cos(radians(%(user_lat)s)) * cos(radians(latitude)) *
            cos(radians(longitude) - radians(%(user_lon)s)) +
            sin(radians(%(user_lat)s)) * sin(radians(latitude))
        ))
        """

        params = {
            'user_lat': user_lat,
            'user_lon': user_lon,
            'radius': radius,
            'limit': RESULTS_PER_PAGE,
            'offset': offset
        }

        where_clauses = ["latitude IS NOT NULL"]
        
        # Taxonomy Filter using Postgres JSONB operator
        # The @> operator checks if the JSON array contains the specific text string
        if taxonomy:
            where_clauses.append("taxonomy @> %(taxonomy_json)s")
            # Format as a JSON list string for the query param
            params['taxonomy_json'] = f'["{taxonomy}"]'

        where_sql = " AND ".join(where_clauses)

        # Query 1: Total Count
        # We calculate distance in the WHERE clause here to filter by radius
        count_sql = f"""
        SELECT COUNT(*) as total
        FROM providers
        WHERE {where_sql}
        AND {haversine} < %(radius)s
        """
        
        cursor.execute(count_sql, params)
        total_results = cursor.fetchone()['total']

        # Query 2: Fetch Results
        # We select distance to sort by it
        results_sql = f"""
        SELECT 
            npi, name, address, city, state, postal_code, 
            latitude, longitude, taxonomy::text, -- Cast JSONB back to text for JSON response
            {haversine} as distance
        FROM providers
        WHERE {where_sql}
        AND {haversine} < %(radius)s
        ORDER BY distance ASC
        LIMIT %(limit)s OFFSET %(offset)s
        """

        cursor.execute(results_sql, params)
        results = cursor.fetchall()

        # Fix casing to match front-end expectations if necessary
        # The JS expects: Name, Address, City, State, PostalCode, NPI
        # Postgres returns lowercase column names by default.
        formatted_results = []
        for row in results:
            formatted_results.append({
                "NPI": row['npi'],
                "Name": row['name'],
                "Address": row['address'],
                "City": row['city'],
                "State": row['state'],
                "PostalCode": row['postal_code'],
                "latitude": row['latitude'],
                "longitude": row['longitude'],
                "taxonomy": row['taxonomy'],
                "distance": row['distance']
            })

        return jsonify({
            "results": formatted_results,
            "total": total_results,
            "offset": offset,
            "limit": RESULTS_PER_PAGE
        })

    except Exception as e:
        print(f"Server Error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
