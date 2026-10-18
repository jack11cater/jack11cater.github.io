import sqlite3
import math
import os
import re
from flask import Flask, jsonify, request
from flask_cors import CORS

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'providers.sqlite')

RESULTS_PER_PAGE = 20

# SECURITY: Whitelist of allowed taxonomy codes. Prevents arbitrary string injection
# into queries even though parameterized queries already protect against SQL injection.
ALLOWED_TAXONOMIES = {
    "", "2084P0800X", "101Y00000X", "101YA0400X", "101YM0800X",
    "103T00000X", "103TA0700X", "207Q00000X", "207QA0000X"
}

# SECURITY: Max radius cap prevents DoS via absurdly large bounding box queries
MAX_RADIUS = 100.0

# --- Flask App Initialization ---
app = Flask(__name__)

# SECURITY: Restrict CORS to your specific GitHub Pages domain instead of wildcard
CORS(app, origins=["https://jack11cater.github.io"])

# --- Database Connection ---
def get_db_conn():
    """Establishes a connection to the SQLite database."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Register math functions needed for Haversine
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
        taxonomy = request.args.get('taxonomy', "").strip()
        offset = int(request.args.get('offset', 0))

        # SECURITY: Coordinate range check
        if not (-90 <= user_lat <= 90) or not (-180 <= user_lon <= 180):
            return jsonify({"error": "Invalid coordinates."}), 400

        # SECURITY: Cap radius to prevent DoS via huge bounding box
        radius = min(radius, MAX_RADIUS)

        # SECURITY: Reject offset values that are negative or absurdly large
        if offset < 0 or offset > 100000:
            return jsonify({"error": "Invalid offset."}), 400

        # SECURITY: Validate taxonomy against whitelist
        if taxonomy not in ALLOWED_TAXONOMIES:
            return jsonify({"error": "Invalid taxonomy value."}), 400

    except (TypeError, ValueError):
        return jsonify({"error": "Invalid query parameters."}), 400

    # --- 2. Bounding Box Pre-filter ---
    lat_change = radius / 69.0
    cos_lat = abs(math.cos(math.radians(user_lat)))
    lon_change = radius / (69.0 * cos_lat) if cos_lat > 0.001 else 180

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
        "min_lat": min_lat, "max_lat": max_lat,
        "min_lon": min_lon, "max_lon": max_lon
    }

    # BUG FIX: Column aliases guarantee consistent JSON key casing for the frontend.
    # Replace p.ColumnName values with your actual DB column names if they differ.
    base_query = f"""
    SELECT
        p.NPI        AS "NPI",
        p.Name       AS "Name",
        p.Address    AS "Address",
        p.City       AS "City",
        p.State      AS "State",
        p.PostalCode AS "PostalCode",
        p.latitude, p.longitude, p.taxonomy,
        {haversine_formula} AS distance
    FROM
        providers p
    """

    taxonomy_join = "LEFT JOIN json_each(p.taxonomy) AS t_join ON 1=1"

    where_clauses = [
        "p.latitude BETWEEN :min_lat AND :max_lat",
        "p.longitude BETWEEN :min_lon AND :max_lon"
    ]

    if taxonomy:
        where_clauses.append("t_join.value = :taxonomy")

    having_clauses = ["distance < :radius"]

    # --- 4. Construct Full Queries ---
    # BUG FIX: GROUP BY is now placed correctly after WHERE, not appended to base_query.
    group_by_clause = "" if taxonomy else "GROUP BY p.NPI"

    count_query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT p.NPI, {haversine_formula} AS distance
        FROM providers p
        {taxonomy_join if taxonomy else ""}
        WHERE {" AND ".join(where_clauses)}
        {group_by_clause}
        HAVING {" AND ".join(having_clauses)}
    )
    """

    results_query = f"""
    {base_query}
    {taxonomy_join if taxonomy else ""}
    WHERE {" AND ".join(where_clauses)}
    {group_by_clause}
    HAVING {" AND ".join(having_clauses)}
    ORDER BY distance
    LIMIT :limit
    OFFSET :offset
    """

    # --- 5. Execute ---
    conn = None
    try:
        conn = get_db_conn()

        count_cursor = conn.execute(count_query, query_params)
        row = count_cursor.fetchone()
        total_results = row[0] if row else 0

        results_cursor = conn.execute(results_query, query_params)
        results = [dict(row) for row in results_cursor.fetchall()]

        return jsonify({
            "results": results,
            "total": total_results,
            "offset": offset,
            "limit": RESULTS_PER_PAGE
        })

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "A database error occurred."}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=False, port=5000)
