from flask import Flask, request, jsonify
from pymongo import MongoClient
from neo4j import GraphDatabase
import xml.etree.ElementTree as ET
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["fraudDB"]
users_collection = mongo_db["users"]

neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin@123"))


def parse_xsd_mapping(xsd_path="schema_mapping.xsd"):
    tree = ET.parse(xsd_path)
    root = tree.getroot()
    ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}

    field_db_map = {}
    field_label_map = {}
    label_path_map = {}

    for element in root.findall("xs:element", ns):
        top_name = element.attrib["name"]
        complex_type = element.find("xs:complexType", ns)
        if complex_type is not None:
            if top_name == "MongoDBUser":
                for child_elem in complex_type.find("xs:sequence", ns).findall("xs:element", ns):
                    field = child_elem.attrib["name"]
                    field_db_map[field] = "MongoDB"
            elif top_name == "Neo4jNodes":
                for sub_elem in complex_type.find("xs:sequence", ns).findall("xs:element", ns):
                    label = sub_elem.attrib["name"]
                    path = None
                    annotation = sub_elem.find("xs:annotation/xs:appinfo", ns)
                    if annotation is not None and annotation.text and annotation.text.strip().startswith("path:"):
                        path = annotation.text.strip().replace("path:", "").strip()
                        label_path_map[label] = path

                    sub_type = sub_elem.find("xs:complexType", ns)
                    if sub_type is not None:
                        for prop in sub_type.find("xs:sequence", ns).findall("xs:element", ns):
                            field = prop.attrib["name"]
                            field_db_map[field] = "Neo4j"
                            field_label_map[field] = label

    return field_db_map, field_label_map, label_path_map


@app.route("/query_data", methods=["POST"])
def query_data():
    request_data = request.json
    requested_fields = request_data.get("fields", [])

    if not requested_fields:
        return jsonify({"error": "No fields provided"}), 400

    field_db_map, field_label_map, label_path_map = parse_xsd_mapping()

    mongo_fields = [f for f in requested_fields if field_db_map.get(f) == "MongoDB"]
    neo4j_fields = [f for f in requested_fields if field_db_map.get(f) == "Neo4j"]

    mongo_data = {}
    account_ids = set()

    if mongo_fields:
        projection = {field: 1 for field in mongo_fields}
        projection["account_id"] = 1
        for doc in users_collection.find({}, projection):
            acc_id = doc.get("account_id")
            if acc_id:
                mongo_data[acc_id] = {field: doc.get(field) for field in mongo_fields}
                account_ids.add(acc_id)

    if not account_ids and neo4j_fields:
        with neo4j_driver.session() as session:
            result = session.run("MATCH (a:Account) RETURN a.account_id AS account_id")
            for record in result:
                acc_id = record["account_id"]
                if acc_id:
                    account_ids.add(acc_id)

    neo4j_data = []

    with neo4j_driver.session() as session:
        for acc_id in account_ids:
            acc_data = {"account_id": acc_id}

            neo4j_fields_by_label = {}
            for field in neo4j_fields:
                label = field_label_map.get(field)
                if label:
                    neo4j_fields_by_label.setdefault(label, []).append(field)

            for label, fields in neo4j_fields_by_label.items():
                unique_fields = list(set(fields))
                projection = ", ".join([f"{label[0].lower()}.{f} AS {f}" for f in unique_fields])
                if label == "Account":
                    query = f"""
                    MATCH (a:Account {{account_id: $acc_id}})
                    RETURN {projection}
                    """
                    result = session.run(query, acc_id=acc_id).single()
                    if result:
                        acc_data.update({field: result.get(field) for field in fields})
                else:
                    path = label_path_map.get(label)
                    if path:
                        query = f"""
                        MATCH {path}
                        WHERE a.account_id = $acc_id
                        RETURN {projection}
                        """
                        result = session.run(query, acc_id=acc_id)
                        for record in result:
                            row = acc_data.copy()
                            row.update({field: record.get(field) for field in fields})
                            neo4j_data.append(row)
                    else:
                        neo4j_data.append(acc_data)

            if not any(field_label_map.get(f) != "Account" for f in neo4j_fields):
                neo4j_data.append(acc_data)

    final_results = []
    for row in neo4j_data:
        acc_id = row["account_id"]
        mongo_part = mongo_data.get(acc_id, {})
        combined = {**row, **mongo_part}
        final_results.append(combined)

    return jsonify({"results": final_results})


if __name__ == "__main__":
    app.run(port=7001, debug=True)
