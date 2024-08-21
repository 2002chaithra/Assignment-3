import os.path
import requests
from flask import Flask, request, jsonify, render_template, url_for, redirect
import csv
import threading
import queue
import logging
import json

app = Flask(__name__)

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

log_path = config['logging']['path']
thread_size = config['threading']['size']
queue_size = config['queue']['size']
csv_file = config['csv']['file_path']

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def load_data():
    """
    Load data from the csv file.

    :return: List of rows in the csv file as dictionaries.
    """
    try:
        if not os.path.exists(csv_file):
            with open(csv_file, "w+", newline='') as file:
                writer = csv.DictWriter(file, fieldnames=['Rollno', 'name', 'english', 'maths', 'science'])
                writer.writeheader()
                logging.info("CSV file created as it did not exist.")
                return []
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            logging.info(f"Loaded records from CSV.")
            return list(reader)
    except Exception as e:
        logging.error(f"Unexpected error occurred while loading data: {e}")
        return jsonify({"Unexpected error occurred": e})


def save(data):
    """
    Save data to the CSV file.
    :param data: List of dictionaries representing the rows to be saved.
    """
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Rollno', 'name', 'english', 'maths', 'science'])
        writer.writeheader()
        writer.writerows(data)
        logging.info(f"Saved records to CSV.")

def queue_data():
    """
    Queue data from the CSV file for processing.
    :return: Queue containing rows from the CSV file.
    """
    q = queue.Queue(maxsize=queue_size)
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            q.put(row)
    logging.info("Data queued for processing.")
    return q


def cal_avg(q, result_dict, result_lock):
    """
    Calculate the average of the subjects of each student from the records in the queue.
    :param q: Queue containing student records.
    :param result_dict: Dictionary to store the average results.
    :param result_lock: Lock to ensure thread safety when updating result_dict.
    """
    while not q.empty():
        record = q.get()
        rollno = record.get('Rollno')
        try:
            english = float(record.get('english'))
            maths = float(record.get('maths'))
            science = float(record.get('science'))
            avg = round(((english + maths + science) / 3), 2)
            with result_lock:
                result_dict[rollno] = {
                    'average': avg
                }
            logging.debug(f"Calculated average for Rollno {rollno}: {avg}")
        except ValueError:
            logging.warning(f"Skipping record for Rollno {rollno} due to invalid data.")
            print(f"Skipping record for Rollno {rollno} due to invalid data.")
        q.task_done()


@app.route("/")
def home():
    """
    Render the home page.
    :return: Rendered home page template.
    """
    return render_template('index.html')


@app.route("/insert", methods=['GET', 'POST'])
def insert():
    """
    Insert a new record or update an existing record.
    :return: JSON response indicating the result of the operation.
    """
    if request.method == 'POST':
        try:
            data = request.form.to_dict()
            rows = load_data()
            rows = [row for row in rows if row['Rollno'] != data['Rollno']]
            rows.append(data)
            save(rows)
            logging.info(f"Inserted/Updated record for Rollno {data['Rollno']}.")
            return jsonify({'status': 'success'}, {"status code": "200"})
        except Exception as e:
            logging.error(f"Unexpected error occurred while inserting/updating record: {e}")
            return jsonify({'Unexpected error ocurred': str(e)}), 500
    return render_template('insert.html')


@app.route("/remove", methods=['GET', 'POST'])
def remove():
    """
    Obtain the Rollno whose record is to be deleted. Redirect to delete endpoint.
    :return: Render to remove page.
    """
    if request.method == 'POST':
        try:
            rollno = request.form["Rollno"]
            requests.delete(url_for('delete', id=rollno, _external=True))
            logging.info(f"Requested removal of record for Rollno {rollno}.")
        except Exception as e:
            logging.error(f"Unexpected error occurred while requesting removal: {e}")
            return jsonify({'Unexpected error occurred': str(e)}), 500
        return redirect(url_for('delete', id=rollno))
    return render_template('remove.html')


@app.route("/remove/<id>", methods=['DELETE', 'GET'])
def delete(id):
    """
    Delete a record by Rollno.
    :param id: Rollno of the record to be deleted.
    :return: JSON response indicating the result of the deletion.
    """
    rows = load_data()
    flag =0
    new_rows = [row for row in rows if row['Rollno'] != id]
    save(new_rows)
    flag = [1 for row in rows if row['Rollno'] == id]
    if flag == 0:
        logging.warning(f"Record with Rollno {id} not found for deletion.")
        return jsonify({"status": "Record not found"}, {"status code": "404"})
    logging.info(f"Deleted record for Rollno {id}.")
    return jsonify({'status': 'success'}, {"status code": "200"})


@app.route("/update", methods=['POST', 'GET'])
def update_data():
    """
    Obtain data that need to be updated. Send a put request.
    :return: JSON response from the update operation.
    """
    if request.method == 'POST':
        try:
            data = request.form.to_dict()
            response = requests.put(url_for('update', _external=True), json=data)
            logging.info(f"Requested update for Rollno {data['Rollno']}.")
            return response.json()
        except Exception as e:
            logging.error(f"Unexpected error occurred while requesting update: {e}")
            return jsonify({'Unexpected error occurred': str(e)}, 500)
    return render_template('update.html')


@app.route("/update", methods=['PUT'])
def update():
    """
    Update a record with new data.
    :return: JSON response indicating the result of the update operation.
    """
    data = request.get_json()
    rows = load_data()
    for row in rows:
        if row['Rollno'] == data['Rollno']:
            row.update(data)
            save(rows)
            logging.info(f"Updated record for Rollno {data['Rollno']}.")
            return jsonify({'status': 'success'}, {"status code": "200"})
    logging.warning(f"Record with Rollno {data['Rollno']} not found for update.")
    return jsonify({'status': 'Rollno not found'}, {"status code": "404"})


@app.route("/read", methods=['GET', 'POST'])
def read_rollno():
    """
    Obtain the Rollno whose record is to be read. Send a get request.
    :return: Redirect to read endpoint or render remove page.
    """
    if request.method == 'POST':
        rollno = request.form["Rollno"]
        requests.get(url_for('read', id=rollno, _external=True))
        logging.info(f"Requested read for Rollno {rollno}.")
        return redirect(url_for('read', id=rollno))
    return render_template('read.html')


@app.route("/read/<id>", methods=['GET'])
def read(id):
    """
    Read a record by Rollno.
    :param id:  Rollno of the record to be read.
    :return: JSON representation of the record or error message.
    """
    rows = load_data()
    for row in rows:
        if row['Rollno'] == id:
            logging.info(f"Record for Rollno {id} retrieved.")
            return row
    logging.warning(f"Record with Rollno {id} not found.")
    return jsonify({'error': 'Missing Rollno parameter'}, {'status code': '404'})


@app.route("/average", methods=['GET'])
def average():
    """
    Calculate the average marks for each student.
    :return: JSON response containing average marks for each student.
    """
    q = queue_data()
    result_lock = threading.Lock()
    result_dict = {}
    threads = []
    for _ in range(thread_size):
        thread = threading.Thread(target=cal_avg, args=(q, result_dict, result_lock))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    logging.info("Calculated averages for all students.")
    return jsonify(result_dict)


if __name__ == "__main__":
    app.run(debug=True)
