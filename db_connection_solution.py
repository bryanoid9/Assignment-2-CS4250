#-------------------------------------------------------------------------
# AUTHOR: Bryan Martinez Ramirez
# FILENAME: db_connection.py
# SPECIFICATION: A python program that connects to a precreated database and automatically creates the necessary tables per the instructions. This program handles all user input.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 6 hours 
#-----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with standard arrays

# importing some Python libraries
import psycopg2
import string

def connectDataBase():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(database="Assignment2", user="postgres", password="123", host="localhost", port="5432")
    return conn

def createTables(conn):
    # Create tables in the PostgreSQL database
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Categories (
            id_cat INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Documents (
            doc_number INTEGER PRIMARY KEY,
            text TEXT NOT NULL,
            title TEXT NOT NULL,
            num_chars INTEGER NOT NULL,
            date TEXT NOT NULL,
            category_id INTEGER NOT NULL REFERENCES Categories(id_cat)
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Terms (
            term TEXT PRIMARY KEY,
            num_chars INTEGER NOT NULL
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Document_Term_Relationship (
            doc_number INTEGER NOT NULL REFERENCES Documents(doc_number),
            term TEXT NOT NULL REFERENCES Terms(term),
            term_count INTEGER NOT NULL,
            PRIMARY KEY (doc_number, term)
        );
    ''')
    conn.commit()
    cur.close()

def createCategory(cur, catId, catName):
    # Insert a new category into the Categories table
    cur.execute("INSERT INTO Categories (id_cat, name) VALUES (%s, %s) ON CONFLICT (id_cat) DO NOTHING;", (catId, catName))

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    # Get the category id based on the informed category name
    cur.execute("SELECT id_cat FROM Categories WHERE name = %s", (docCat,))
    category_id = cur.fetchone()[0]
    # Calculate num_chars by removing spaces and punctuation
    num_chars = len(''.join(e for e in docText if e.isalnum()))
    # Insert a new document into the Documents table
    cur.execute("INSERT INTO Documents (doc_number, text, title, num_chars, date, category_id) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (doc_number) DO NOTHING;", (docId, docText, docTitle, num_chars, docDate, category_id))
    # Tokenize the text to update Terms and Document_Term_Relationship
    terms = docText.lower().translate(str.maketrans('', '', string.punctuation)).split()
    for term in set(terms):
        term_count = terms.count(term)
        # Insert new terms into the Terms table or ignore if it already exists
        cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s) ON CONFLICT (term) DO NOTHING;", (term, len(term)))
        # Insert into Document_Term_Relationship table
        cur.execute("INSERT INTO Document_Term_Relationship (doc_number, term, term_count) VALUES (%s, %s, %s) ON CONFLICT (doc_number, term) DO NOTHING;", (docId, term, term_count))

def deleteDocument(cur, docId):
    # Delete document-related term relationships and the document itself
    cur.execute("DELETE FROM Document_Term_Relationship WHERE doc_number = %s;", (docId,))
    cur.execute("DELETE FROM Documents WHERE doc_number = %s;", (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # Update a document by first deleting and then re-creating it
    deleteDocument(cur, docId)
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
    # Retrieve the inverted index of terms with their associated documents and counts
    cur.execute("SELECT term, STRING_AGG(title || ':' || term_count::text, ', ') FROM Document_Term_Relationship JOIN Documents ON Document_Term_Relationship.doc_number = Documents.doc_number GROUP BY term ORDER BY term;")
    index_data = cur.fetchall()
    index = {term: titles for term, titles in index_data}
    return index

# Main
if __name__ == "__main__":
    conn = connectDataBase()
    if conn is not None:
        createTables(conn)
        with conn.cursor() as cur:
            # You can test your functions here
            pass
    else:
        print("Failed to connect to the database")
