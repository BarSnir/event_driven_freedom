import mysql.connector

def process():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="db"
    )
    mycursor = mydb.cursor()
    sql = """
        INSERT INTO customers_test (CustomerID, LastName, FirstName, Address, City) 
        VALUES (%s, %s, %s, %s, %s)
    """
    val = [
        (1, 'Brandt', 'Nola', 'Apt. 218 21604 Lanny Mission, East Dannbury, NJ 43383', 'NJ'),
        (2, 'Singh', 'Campbell', 'Apt. 967 747 Lashunda Via, South Shakia, HI 35872-9933', 'HI'),
        (3, 'Savage', 'Tatiana', 'Apt. 168 8141 Jakubowski Turnpike, Lake Nickytown, KY 49576-0392', 'KY'),
        (4, 'Bridges', 'Romeo', 'Suite 569 845 Deckow Square, McGlynnstad, AR 65713', 'AR'),
        (5, 'Mayo', 'Devin', 'Suite 391 4103 Zulauf Orchard, Lake Lemuel, ND 53175-1256', 'ND'),
        (5, 'Myers', 'Nathaly', 'Suite 644 596 Angele Cape, East Jaymie, WI 86126', 'WI'),
        (6, 'Potts', 'Brayden', 'Apt. 124 37413 Champlin Square, New Patriciaside, NH 44628-8406', 'NH'),
        (7, 'Carroll', 'Ariel', '585 Schinner Inlet, Port Tanya, IN 26819', 'IN'),
        (8, 'Stone', 'Brooklyn', 'Apt. 234 9865 Pollich Stream, New Emelda, NV 44996', 'NV'),
        (9, 'Burnett', 'Luna', '560 Ranae Motorway, Boehmbury, AR 72475', ''),
        (10, 'Hernandez', 'Anika', '5931 Mertz Underpass, Jayechester, AR 88731-9015', 'AR')
    ]

    mycursor.executemany(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "was inserted.")

if __name__ == '__main__':
    process()