import sqlite3

conn = sqlite3.connect('example.db')
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        name TEXT,
        value INTEGER
    )
''')

# Define the item you want to insert/update
item = (2, 'item_1', 5)

# Use an UPSERT statement to insert or update the item
cursor.execute('''
    INSERT INTO items (id, name, value)
    VALUES (?,?,?)
    ON CONFLICT(id) 
    DO UPDATE SET value=excluded.value
''', item)

item = (2, 'item_4', 7)

cursor.execute('''
    INSERT INTO items (id, name, value)
    VALUES (?,?,?)
    ON CONFLICT(id) 
    DO UPDATE SET value=excluded.value
''', item)

res = cursor.execute("SELECT name, value FROM items WHERE id=2")
print(res.fetchone())

conn.commit()
conn.close()
