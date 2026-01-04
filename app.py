import sqlite3
import math
import os
from flask import Flask, jsonify, request
from flask_cors import CORS

# --- Configuration ---
# SECURITY: Use absolute path to ensure DB is found regardless of launch directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'providers.sqlite')

RESULTS_PER_PAGE = 20

# --- Flask App Initialization ---
app = Flask(__name__)
# Enable CORS (Safe for public read-only APIs)
CORS(app)

# --- Database Connection ---
def get_db_conn():
    """Establishes a connection to the SQLite database."""
    # Robust check to prevent confusing "OperationalError" if file missing
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row 
    
    # Register math functions
    conn.create_function("radians", 1, math.radians)
    conn.create_function("cos", 1, math.cos)
    conn.create_function("sin", 1, math.sin)
    conn.create_function("acos", 1, math.acos)
    return conn

# --- API Endpoint: /api/search ---
@app.route('/api/search')
def search_providers():
    try:
        # 1. Input Validation
        user_lat = float(request.args.get('lat'))
        user_lon = float(request.args.get('lon'))
        radius = float(request.args.get('radius', 25))
        taxonomy = request.args.get('taxonomy', "")
        offset = int(request.args.get('offset', 0))

        # SECURITY: Basic range check prevents nonsense math errors
        if not (-90 <= user_lat <= 90) or not (-180 <= user_lon <= 180):
             return jsonify({"error": "Invalid coordinates."}), 400

    except (TypeError, ValueError):
        return jsonify({"error": "Invalid query parameters."}), 400

    # --- 2. Optimization: Bounding Box Filter (DoS Protection) ---
    # Calculating Haversine for the entire DB is too slow.
    # We pre-filter using a simple bounding box to reduce the dataset.
    # 1 degree lat ~= 69 miles.
    lat_change = radius / 69.0
    # Longitude varies by latitude, handle poles/equator
    lon_change = radius / (69.0 * abs(math.cos(math.radians(user_lat)))) if abs(math.cos(math.radians(user_lat))) > 0.001 else 180

    min_lat = user_lat - lat_change
    max_lat = user_lat + lat_change
    min_lon = user_lon - lon_change
    max_lon = user_lon + lon_change

    # --- 3. Build Query ---
    haversine_formula = """
    (3958.8 * acos(
        cos(radians(:user_lat)) * cos(radians(p.latitude)) *
        cos(radians(p.longitude) - radians(:user_lon)) +
        sin(radians(:user_lat)) * sin(radians(p.latitude))
    ))
    """

    query_params = {
        "user_lat": user_lat,
        "user_lon": user_lon,
        "radius": radius,
        "taxonomy": taxonomy,
        "limit": RESULTS_PER_PAGE,
        "offset": offset,
        # Bounding box params
        "min_lat": min_lat, "max_lat": max_lat,
        "min_lon": min_lon, "max_lon": max_lon
    }

    base_query = f"""
    SELECT
        p.NPI, p.Name, p.Address, p.City, p.State, p.PostalCode,
        p.latitude, p.longitude, p.taxonomy,
        {haversine_formula} AS distance
    FROM
        providers p
    """

    taxonomy_join = "LEFT JOIN json_each(p.taxonomy) AS t_join ON 1=1"

    # WHERE clauses: We add the Bounding Box check here.
    # This runs BEFORE the heavy math, making it 100x faster.
    where_clauses = [
        "p.latitude BETWEEN :min_lat AND :max_lat",
        "p.longitude BETWEEN :min_lon AND :max_lon"
    ]

    if taxonomy:
        where_clauses.append("t_join.value = :taxonomy")
    else:
        # Standardize grouping if no taxonomy selected
        base_query += " GROUP BY p.NPI"

    having_clauses = [
        "distance < :radius"
    ]

    # --- 4. Construct Full Queries ---
    
    count_query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT p.NPI, {haversine_formula} AS distance
        FROM providers p
        {taxonomy_join if taxonomy else ""}
        WHERE {" AND ".join(where_clauses)}
        {"" if taxonomy else "GROUP BY p.NPI"}
        HAVING {" AND ".join(having_clauses)}
    )
    """

    results_query = f"""
    {base_query}
    {taxonomy_join if taxonomy else ""}
    WHERE {" AND ".join(where_clauses)}
    HAVING {" AND ".join(having_clauses)}
    ORDER BY distance
    LIMIT :limit
    OFFSET :offset
    """

    # --- 5. Execute ---
    conn = None
    try:
        conn = get_db_conn()
        
        # Execute Count
        count_cursor = conn.execute(count_query, query_params)
        row = count_cursor.fetchone()
        total_results = row[0] if row else 0
        
        # Execute Results
        results_cursor = conn.execute(results_query, query_params)
        results = [dict(row) for row in results_cursor.fetchall()]
        
        return jsonify({
            "results": results,
            "total": total_results,
            "offset": offset,
            "limit": RESULTS_PER_PAGE
        })

    except sqlite3.Error as e:
        # SECURITY: Log specific error to console/logs
        print(f"Database error: {e}")
        # SECURITY: Return generic error to user to avoid leaking DB schema
        return jsonify({"error": "A database error occurred."}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # SECURITY: Never run debug=True in production context
    app.run(debug=False, port=5000)
