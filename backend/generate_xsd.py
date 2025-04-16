from pymongo import MongoClient
from neo4j import GraphDatabase
import xml.etree.ElementTree as ET

MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "admin@123"

label_path_map = {
    "Payment": "(a:Account)-[:MADE_PAYMENT]->(p:Payment)",
}

def get_mongodb_attributes():
    client = MongoClient(MONGO_URI)
    db = client['fraudDB']
    collection = db['users']
    keys = set()
    for doc in collection.find():
        keys.update(doc.keys())
    client.close()
    return keys

def get_neo4j_attributes():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    node_labels = ['Account', 'Payment']
    node_props = {}

    with driver.session() as session:
        for label in node_labels:
            node_props[label] = set()
            query = f"MATCH (n:{label}) RETURN DISTINCT keys(n) AS props LIMIT 50"
            result = session.run(query)
            for record in result:
                prop_list = record["props"]
                if prop_list:
                    node_props[label].update(prop_list)

    driver.close()
    return node_props

def add_element_sequence(parent):
    return ET.SubElement(parent, "xs:sequence")

def add_element(parent, name):
    ET.SubElement(parent, "xs:element", name=name, type="xs:string")

def add_path_annotation(parent, path_value):
    annotation = ET.SubElement(parent, "xs:annotation")
    appinfo = ET.SubElement(annotation, "xs:appinfo")
    appinfo.text = f"path: {path_value}"

def generate_xsd():
    xsd = ET.Element("xs:schema", attrib={"xmlns:xs": "http://www.w3.org/2001/XMLSchema"})

    # MongoDB schema
    mongo_el = ET.SubElement(xsd, "xs:element", name="MongoDBUser")
    mongo_type = ET.SubElement(mongo_el, "xs:complexType")
    mongo_seq = add_element_sequence(mongo_type)
    for attr in sorted(get_mongodb_attributes()):
        add_element(mongo_seq, attr)

    # Neo4j schema
    neo4j_nodes_el = ET.SubElement(xsd, "xs:element", name="Neo4jNodes")
    neo4j_nodes_type = ET.SubElement(neo4j_nodes_el, "xs:complexType")
    neo4j_seq = add_element_sequence(neo4j_nodes_type)

    node_attrs = get_neo4j_attributes()
    for label, props in node_attrs.items():
        label_el = ET.SubElement(neo4j_seq, "xs:element", name=label)
        
        if label in label_path_map:
            add_path_annotation(label_el, label_path_map[label])

        label_type = ET.SubElement(label_el, "xs:complexType")
        label_seq = add_element_sequence(label_type)
        for prop in sorted(props):
            name = f"{label.lower()}_{prop}" if prop == "id" else prop
            add_element(label_seq, name)

    tree = ET.ElementTree(xsd)
    tree.write("schema_mapping.xsd", encoding="utf-8", xml_declaration=True)

if __name__ == "__main__":
    generate_xsd()
