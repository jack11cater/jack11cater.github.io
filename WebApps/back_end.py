import sqlite3
import math
from flask import Flask, jsonify, request
from flask_cors import CORS

# --- Configuration ---
# Make sure your final database is named this and is in the same directory
DB_PATH = 'providers.sqlite' 
# How many results to send back per "page"
RESULTS_PER_PAGE = 20

# --- Flask App Initialization ---
app = Flask(__name__)
# Enable CORS (Cross-Origin Resource Sharing) to allow your index.html 
# (even when opened as a file) to make requests to this server.
CORS(app)

# --- Database Connection ---
def get_db_conn():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # Allows accessing columns by name
    
    # --- Register custom SQL functions ---
    # We need to teach SQLite how to do advanced math (sin, cos, acos, radians)
    # This is necessary for the Haversine (distance) formula.
    conn.create_function("radians", 1, math.radians)
    conn.create_function("cos", 1, math.cos)
    conn.create_function("sin", 1, math.sin)
    conn.create_function("acos", 1, math.acos)
    return conn

# --- API Endpoint: /api/search ---
@app.route('/api/search')
def search_providers():
    """
    The main search endpoint. Takes user coordinates and filters
    and returns a paginated list of nearby providers.
    """
    
    # --- 1. Get Query Parameters ---
    try:
        # User's location
        user_lat = float(request.args.get('lat'))
        user_lon = float(request.args.get('lon'))
        # Search radius
        radius = float(request.args.get('radius', 25))
        # Taxonomy filter (e.g., '101Y00000X' or "" for all)
        taxonomy = request.args.get('taxonomy', "")
        # Pagination (for "Load More" button)
        offset = int(request.args.get('offset', 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid query parameters. 'lat', 'lon', and 'radius' must be numbers."}), 400
    except Exception as e:
         return jsonify({"error": f"An unexpected error occurred parsing parameters: {e}"}), 400

    
    # --- 2. Build the SQL Query ---
    
    # This is the Haversine formula in SQL. 3958.8 is the radius of Earth in miles.
    # It calculates the distance for every row and returns it as a new 'distance' column.
    haversine_formula = """
    (3958.8 * acos(
        cos(radians(:user_lat)) * cos(radians(p.latitude)) *
        cos(radians(p.longitude) - radians(:user_lon)) +
        sin(radians(:user_lat)) * sin(radians(p.latitude))
    ))
    """
    
    # We use named parameters (:user_lat, etc.) to safely insert values
    query_params = {
        "user_lat": user_lat,
        "user_lon": user_lon,
        "radius": radius,
        "taxonomy": taxonomy,
        "limit": RESULTS_PER_PAGE,
        "offset": offset
    }

    # Base query selects all columns plus the calculated distance
    base_query = f"""
    SELECT
        p.NPI, p.Name, p.Address, p.City, p.State, p.PostalCode,
        p.latitude, p.longitude, p.taxonomy,
        {haversine_formula} AS distance
    FROM
        providers p
    """
    
    # We need to join against the JSON data in the taxonomy column
    # `json_each` explodes the JSON array (e.g., ["code1", "code2"]) into separate rows
    taxonomy_join = "LEFT JOIN json_each(p.taxonomy) AS t_join ON 1=1"
    
    # WHERE clauses filter *before* calculating distance (faster)
    where_clauses = [
        "p.latitude IS NOT NULL AND p.latitude != ''" # Must have coordinates
    ]
    
    # This clever clause adds the taxonomy filter ONLY if 'taxonomy' is not empty
    if taxonomy:
        where_clauses.append("t_join.value = :taxonomy")
    else:
        # If no taxonomy is selected, we must use GROUP BY to avoid duplicates
        # from the taxonomy_join.
        # This is a bit advanced but ensures one row per provider.
        base_query += " GROUP BY p.NPI"

    # HAVING clauses filter *after* calculating distance
    having_clauses = [
        "distance < :radius"
    ]

    # --- 3. Build Two Queries (Count and Results) ---
    
    # Query 1: Get the TOTAL count of matching providers (for "Showing X of Y")
    # We wrap the main query in a COUNT to get the total
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

    # Query 2: Get the paginated RESULTS
    results_query = f"""
    {base_query}
    {taxonomy_join if taxonomy else ""}
    WHERE {" AND ".join(where_clauses)}
    HAVING {" AND ".join(having_clauses)}
    ORDER BY distance
    LIMIT :limit
    OFFSET :offset
    """

    # --- 4. Execute Queries and Return JSON ---
    conn = get_db_conn()
    try:
        # Execute Count Query
        count_cursor = conn.execute(count_query, query_params)
        total_results = count_cursor.fetchone()[0]
        
        # Execute Results Query
        results_cursor = conn.execute(results_query, query_params)
        results = [dict(row) for row in results_cursor.fetchall()]
        
        # Return everything in a neat JSON object
        return jsonify({
            "results": results,
            "total": total_results,
            "offset": offset,
            "limit": RESULTS_PER_PAGE
        })

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "A database error occurred.", "details": str(e)}), 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({"error": "An unexpected server error occurred.", "details": str(e)}), 500
    finally:
        if conn:
            conn.close()

# --- Run the App ---
if __name__ == '__main__':
    # Runs the server on http://127.0.0.1:5000
    # Use host='0.0.0.0' to make it accessible on your network
    app.run(debug=True, port=5000)
