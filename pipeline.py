import mysql.connector


def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="kia_dealers"
    )


# ================= STATE CITY =================

def create_state_city_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kia_state_city (
        id INT AUTO_INCREMENT PRIMARY KEY,
        state_name VARCHAR(100),
        state_key VARCHAR(50),
        city_name VARCHAR(100),
        city_key VARCHAR(50),
        status VARCHAR(50)
    )
    """)

    conn.commit()
    conn.close()


def insert_state_city_batch(rows):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
    INSERT IGNORE INTO kia_state_city 
    (state_name, state_key, city_name, city_key, status)
    VALUES (%s, %s, %s, %s, %s)
    """

    values = [
        (
            r["state_name"],
            r["state_key"],
            r["city_name"],
            r["city_key"],
            "pending"
        )
        for r in rows
    ]

    cursor.executemany(query, values)
    conn.commit()
    conn.close()


def fetch_pending_state_city_keys():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT state_key, city_key 
    FROM kia_state_city 
    WHERE status = 'pending' OR status IS NULL
    """)

    data = cursor.fetchall()
    conn.close()
    return data


def update_state_city_status(state_key, city_key, status):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE kia_state_city
    SET status = %s
    WHERE state_key = %s AND city_key = %s
    """, (status, state_key, city_key))

    conn.commit()
    conn.close()


# ================= DEALERS =================

def create_dealers_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kia_dealers_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        dealer_name VARCHAR(255),
        dealer_id VARCHAR(100),
        dealer_type VARCHAR(100),
        address TEXT,
        phone VARCHAR(50),
        email VARCHAR(255),
        web_url TEXT,
        state_name VARCHAR(100),
        city_name VARCHAR(100),
        state_key VARCHAR(50),
        city_key VARCHAR(50),
        source_url TEXT
    )
    """)

    conn.commit()
    conn.close()


def insert_dealer(data):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO kia_dealers_data (
        dealer_name, dealer_id, dealer_type, address,
        phone, email, web_url,
        state_name, city_name, state_key, city_key, source_url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data["dealer_name"],
        data["dealer_id"],
        data["dealer_type"],
        data["address"],
        data["phone"],
        data["email"],
        data["web_url"],
        data["state_name"],
        data["city_name"],
        data["state_key"],
        data["city_key"],
        data["source_url"]
    ))

    conn.commit()
    conn.close()