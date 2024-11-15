import pymongo
from flask import Flask, request, json, Response, jsonify, send_file
from flask_cors import CORS, cross_origin
from bson import ObjectId
import pandas as pd
import io

app = Flask(__name__)
CORS(app, support_credentials=True)

client = pymongo.MongoClient("mongodb+srv://sajith:snipl%40123@cluster0.s88zo.mongodb.net/")

inventory_items = ["fertilizers", "insecticides", "fungicides", "miticides", "herbicides", "nematicides", "infrastructure_tools", "planting_tools", "irrigation_tools", "climate_tools", "harvesting_tools", "pests_tools", "transport_tools", "storage_tools", "software_tools", "electrical_tools", "growth_promoters", "growing_media", "irrigation_systems", "polyhouse_materials", "environment_controls", "packaging_materials", "consumables", "plumbing_materials", "seeds&plants"]

pests_items = ["pests"]

crops_items = ["vegetables", "fruits", "flowers", "herbs", "exotic_crops", "medicinal_plants"]

uom_items = ["length", "area", "volume_liquid", "volume_solid", "mass", "time", "temperature", "si", "indian_system", "british_system", "metric_system"]

supplier_items = ["suppliers_datapoints"]

buyer_items = ["buyers_datapoints"]

disease_items = ["fungal", "bacterial", "viral", "parasitic", "oomycete", "other_diseases"]

labour_master_items = ["labours"]

labours_data_items = ["labours_data"]

user_data_items = ["users", "jobs", "jobs_allotment", "components", "stocks", "suppliers"]

items = ["inventory", "crops", "diseases"]

# Helper function to format MongoDB documents
def format_doc(doc):
    
    doc['_id'] = str(doc['_id'])
    return doc

#Dynamic routing all the items for GET, POST, PUT, DELETE APIs
def create_dynamic_routes(item, db):
    # Dynamic GET route
    def get_items():
        col = db[item]
        data = [format_doc(doc) for doc in col.find()]
        return jsonify({"data": data}), 200

    # Assign a unique name to the function
    get_items.__name__ = f'get_{item}'

    # Dynamic POST route
    def add_items():
        data = request.json
        col = db[item]
        result = col.insert_one(data)
        return jsonify({"_id": str(result.inserted_id), "message": "Item successfully added", "status": 201}), 201
    
        # Assign a unique name to the function
    add_items.__name__ = f'add_{item}'
    
    def edit_items(id):
        data = request.json
        col = db[item]
        result = col.update_one({'_id': ObjectId(id)}, {'$set': data})
        if result.matched_count:
            return jsonify({"message": "Item successfully updated", "status": 200}), 200
        else:
            return jsonify({"errors": "Item not found"}), 404

    # Assign a unique name to the function
    edit_items.__name__ = f'edit_{item}'
    
    def delete_items(id):
        col = db[item]
        result = col.delete_one({'_id': ObjectId(id)})
        if result.deleted_count:
            return jsonify({"message":"Deleted successfully", "status": 200}), 200
        else:
            return jsonify({"errors": "Not found"}), 404
    
    # Assign a unique name to the function
    delete_items.__name__ = f'delete_{item}'

    # Register the functions with Flask
    app.add_url_rule(f'/{item}', view_func=get_items, methods=['GET'])
    app.add_url_rule(f'/add/{item}', view_func=add_items, methods=['POST'])
    app.add_url_rule(f'/edit/{item}/<id>', view_func=edit_items, methods=['PUT'])
    app.add_url_rule(f'/delete/{item}/<id>', view_func=delete_items, methods=['DELETE'])

# Register the routes for each item in the list
for item in inventory_items:
    db = client["inventory_master"]
    create_dynamic_routes(item, db)

for item in pests_items:
    db = client["pests_master"]
    create_dynamic_routes(item, db)
    
for item in crops_items:
    db = client["crops_master"]
    create_dynamic_routes(item, db)
    
for item in uom_items:
    db = client["uom_master"]
    create_dynamic_routes(item, db)

for item in supplier_items:
    db = client["suppliers_master"]
    create_dynamic_routes(item, db)

for item in buyer_items:
    db = client["buyers_master"]
    create_dynamic_routes(item, db)

for item in disease_items:
    db = client["diseases_master"]
    create_dynamic_routes(item, db)
    
for item in labour_master_items:
    db = client["labour_master"]
    create_dynamic_routes(item, db)
    
for item in labours_data_items:
    db = client["labours_data"]
    create_dynamic_routes(item, db) 
    
for item in user_data_items:
    db = client["user_db"]
    create_dynamic_routes(item, db)
    
for item in items:
    db = client["items_db"]
    create_dynamic_routes(item, db)

#Post function for adding data points to labour sub-master as per the user selection.
@app.route('/user_master', methods=["POST"]) 
def create_user_master():
    db = client["user_db"]
    col = db["labours_subMaster"]
    data = request.json
    id = data.get('id')
    if not id:
        return jsonify({"errors": "ID is required", "status": 400}), 400
    result = col.replace_one({"id": id}, data, upsert=True)
    if result.matched_count>0:
        return jsonify({"message": "Updated", "status": 200}), 200
    elif result.upserted_id:
        return jsonify({"_id":str(result.upserted_id), "message":"Created", "status": 201}), 201
    else:
        return jsonify({"errors":"error", "status": 500}), 500

#Get the data points selected in the labour sub master    
@app.route('/user_form', methods=["GET"])
def get_form():
    db = client["user_db"]
    col = db["labours_subMaster"]
    result = list(col.find({},{'_id':0, 'id':0}))
    # [format_doc(doc) for doc in col.find({}, {'id': 0})]
    return jsonify({"data": result, "status": 200}), 200

#GET api to get a single job alloted to a labour
@app.route('/single_job/<id>', methods=['GET'])
def get_single_job(id):
    db = client["user_db"]
    col = db["jobs_allotment"]
    data = list(col.find({'_id': ObjectId(id)}, {'_id': 0}))
    return jsonify({"data": data}), 200

#GET api to download all the jobs alloted within a date range
@app.route('/jobs_download', methods=['GET'])
def download_jobs():
    db = client["user_db"]
    col = db["jobs_allotment"]
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    data = list(col.find({'start_date':{'$gte':start_date_str, '$lte':end_date_str}}, {'_id': 0, 'labour.Labour ID': 0}))
    if not data:
        return jsonify({'message': 'No data found'}), 404
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name = 'Sheet1')
    output.seek(0)
    filename = f"jobs-{start_date_str}_to_{end_date_str}.xlsx"
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    
if __name__ == '__main__':
    app.run(host = "0.0.0.0", port=5000, debug=True)