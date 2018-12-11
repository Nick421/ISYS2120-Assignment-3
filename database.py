#!/usr/bin/env python3
"""
DeviceManagement Database module.
Contains all interactions between the webapp and the queries to the database.
"""

import configparser
import datetime
from typing import List, Optional

import setup_vendor_path  # noqa
import pg8000

################################################################################
#   Welcome to the database file, where all the query magic happens.
#   My biggest tip is look at the *week 9 lab*.
#   Important information:
#       - If you're getting issues and getting locked out of your database.
#           You may have reached the maximum number of connections.
#           Why? (You're not closing things!) Be careful!
#       - Check things *carefully*.
#       - There may be better ways to do things, this is just for example
#           purposes
#       - ORDERING MATTERS
#           - Unfortunately to make it easier for everyone, we have to ask that
#               your columns are in order. WATCH YOUR SELECTS!! :)
#   Good luck!
#       And remember to have some fun :D
################################################################################


#####################################################
#   Database Connect
#   (No need to touch
#       (unless the exception is potatoing))
#####################################################

def database_connect():
    """
    Connects to the database using the connection string.
    If 'None' was returned it means there was an issue connecting to
    the database. It would be wise to handle this ;)
    """
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    if 'database' not in config['DATABASE']:
        config['DATABASE']['database'] = config['DATABASE']['user']

    # Create a connection to the database
    connection = None
    try:
        # Parses the config file and connects using the connect string
        connection = pg8000.connect(database=config['DATABASE']['database'],
                                    user=config['DATABASE']['user'],
                                    password=config['DATABASE']['password'],
                                    host=config['DATABASE']['host'])
    except pg8000.OperationalError as operation_error:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(operation_error)
        return None

    # return the connection to use
    return connection


#####################################################
#   Query (a + a[i])
#   Login
#####################################################

def check_login(employee_id, password: str) -> Optional[dict]:
    """
    Check that the users information exists in the database.
        - True => return the user data
        - False => return None
    """

    # Note: this example system is not well-designed for security.
    # There are several serious problems. One is that the database
    # stores passwords directly; a better design would "salt" each password
    # and then hash the result, and store only the hash.
    # This is ok for a toy assignment, but do not use this code as a model when you are
    # writing a real system for a client or yourself.


    # Check if the user details are correct!
    # Return the relevant information (watch the order!)


    # NOTE: Make sure you take care of ORDER!!!

    #employee_info = [
        #1337,                       # empid
        #'Porter Tato Head',         # name
        #'123 Fake Street',          # homeAddress
        #datetime.date(1970, 1, 1),  # dateOfBirth
    #]

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT empID, name, homeAddress, dateOfBirth FROM Employee
            WHERE empID = %s AND password = %s"""
        cursor.execute(sql, (employee_id, password))
        ret = cursor.fetchone()
        cursor.close()
        connection.close()
    except:
        print("Error logging in")
        cursor.close()
        connection.close()
        return None

    if (ret is None):
        return None

    employee_info = [int(ret[0]),ret[1],ret[2],ret[3]]

    user = {
        'empid': employee_info[0],
        'name': employee_info[1],
        'homeAddress': employee_info[2],
        'dateOfBirth': employee_info[3],
    }

    return user


#####################################################
#   Query (f[i])
#   Is Manager?
#####################################################

def is_manager(employee_id: int) -> Optional[str]:
    """
    Get the department the employee is a manager of, if any.
    Returns None if the employee doesn't manage a department.
    """

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT name FROM Department WHERE manager = %s"""
        cursor.execute(sql, (employee_id,))
        ret = cursor.fetchone()
        cursor.close()
        connection.close()
    except:
        print("Could not identify employee is_manager")
        cursor.close()
        connection.close()
        return None


    if (ret is None):
        return None

    # manager_of = ['RND', 'Accounting']
    manager_of = ret[0]

    return manager_of


#####################################################
#   Query (a[ii])
#   Get My Used Devices
#####################################################

def get_devices_used_by(employee_id: int) -> list:
    """
    Get a list of all the devices used by the employee.
    """

    # Return a list of devices issued to the user!
    # Each "Row" contains [ deviceID, manufacturer, modelNumber]
    # If no devices = empty list []

    #devices = [
        #[7, 'Zava', '1146805551'],
        #[13, 'Skyndu', '5296853075'],
        #[24, 'Yakitri', '8406089423'],
    #]
    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT deviceID,manufacturer,modelNumber FROM Device WHERE issuedTo = %s"""
        cursor.execute(sql, (employee_id,))
        ret = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("Failed to get deviced of employee")
        cursor.close()
        connection.close()
        return None

    devices = []
    for x in ret:
        tmp = [int(x[0]), x[1], x[2]]
        devices.append(tmp)

    return devices


#####################################################
#   Query (a[iii])
#   Get departments employee works in
#####################################################

def employee_works_in(employee_id: int) -> List[str]:
    """
    Return the departments that the employee works in.
    """

    # Return a list of departments

    #departments = ['IT', 'Marketing']
    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    departments = []
    try:
        sql = """SELECT department FROM EmployeeDepartments WHERE empID = %s"""
        cursor.execute(sql, (employee_id,))
        ret = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("Could not retrieve employee departments")
        cursor.close()
        connection.close()
        return None


    for x in ret:
        departments.append(x[0])

    return departments


#####################################################
#   Query (c)
#   Get My Issued Devices
#####################################################

def get_issued_devices_for_user(employee_id: int) -> list:
    """
    Get all devices issued to the user.
        - Return a list of all devices to the user.
    """

    # Return a list of devices issued to the user!
    # Each "Row" contains [ deviceID, purchaseDate, manufacturer, modelNumber ]
    # If no devices = empty list []

    #devices = [
        #[7, datetime.date(2017, 8, 28), 'Zava', '1146805551'],
        #[8, datetime.date(2017, 9, 22), 'Topicware', '5798231046'],
        #[6123, datetime.date(2017, 9, 5), 'Dabshots', '6481799600'],
        #[1373, datetime.date(2018, 4, 19), 'Cogibox', '6700815444'],
        #[8, datetime.date(2018, 2, 10), 'Feednation', '2050267274'],
        #[36, datetime.date(2017, 11, 5), 'Muxo', '8768929463'],
        #[17, datetime.date(2018, 1, 14), 'Izio', '5886976558'],
        #[13, datetime.date(2017, 9, 8), 'Skyndu', '5296853075'],
        #[24, datetime.date(2017, 10, 22), 'Yakitri', '8406089423'],
    #]

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT deviceID,purchaseDate,manufacturer, modelNumber FROM Device WHERE issuedTo = %s"""
        cursor.execute(sql, (employee_id,))
        ret = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("Failed to get deviced of employee")
        cursor.close()
        connection.close()
        return None

    devices = []
    for x in ret:
        tmp = [int(x[0]), x[1], x[2], x[3]]
        devices.append(tmp)

    return devices


#####################################################
#   Query (b)
#   Get All Models
#####################################################

def get_all_models() -> list:
    """
    Get all models available.
    """

    # Return the list of models with information from the model table.
    # Each "Row" contains: [manufacturer, description, modelnumber, weight]
    # If No Models = EMPTY LIST []

    #models = [
        #['Feednation', 'Expanded didactic instruction set', '2050267274', 31],
        #['Zoombox', 'Profit-focused global extranet', '8860068207', 57],
        #['Shufflebeat', 'Robust clear-thinking functionalities', '0288809602', 23],
        #['Voonyx', 'Vision-oriented bandwidth-monitored instruction set', '5275001460', 82],
        #['Tagpad', 'Fundamental human-resource migration', '3772470904', 89],
        #['Wordpedia', 'Business-focused tertiary orchestration', '0211912271', 17],
        #['Skyndu', 'Quality-focused web-enabled parallelism', '5296853075', 93],
        #['Tazz', 'Re-engineered well-modulated contingency', '8479884797', 95],
        #['Dabshots', 'Centralized empowering protocol', '6481799600', 68],
        #['Rhybox', 'Re-contextualized bifurcated orchestration', '7107712551', 25],
        #['Cogibox', 'Networked disintermediate application', '6700815444', 27],
        #['Meedoo', 'Progressive 24-7 orchestration', '3998544224', 43],
        #['Zoomzone', 'Reverse-engineered systemic monitoring', '9854941272', 50],
        #['Meejo', 'Secured static implementation', '3488947459', 75],
        #['Topicware', 'Extended system-worthy forecast', '5798231046', 100],
        #['Izio', 'Open-source static productivity', '5886976558', 53],
        #['Zava', 'Polarised incremental paradigm', '1146805551', 82],
        #['Demizz', 'Reduced hybrid website', '9510770736', 63],
        #['Muxo', 'Switchable contextually-based throughput', '8768929463', 40],
        #['Wordify', 'Front-line fault-tolerant middleware', '8465785368', 84],
        #'Twinder', 'Intuitive contextually-based local area network', '5709369365', 78],
        #['Jatri', 'Horizontal disintermediate workforce', '8271780565', 31],
        #['Chatterbridge', 'Phased zero tolerance architecture', '8429506128', 39],
    #]

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    try:
        sql = """SELECT manufacturer, description, modelnumber, weight FROM Model"""
        cursor.execute(sql)
        ret = cursor.fetchall()
    except:
        print("could not get all models")
        cursor.close()
        connection.close()
        return None

    models = []
    for x in ret:
        tmp = [x[0],x[1],x[2],int(x[3])]
        models.append(tmp)

    return models


#####################################################
#   Query (d[ii])
#   Get Device Repairs
#####################################################

def get_device_repairs(device_id: int) -> list:
    """
    Get all repairs made to a device.
    """

    # Return the repairs done to a certain device
    # Each "Row" contains:
    #       - repairid
    #       - faultreport
    #       - startdate
    #       - enddate
    #       - cost
    # If no repairs = empty list

    #repairs = [
        #[17, 'Never, The', datetime.date(2018, 7, 16), datetime.date(2018, 9, 22), '$837.13'],
        #[18, 'Gonna', datetime.date(2018, 8, 3), datetime.date(2018, 9, 22), '$1726.99'],
        #[19, 'Give', datetime.date(2018, 9, 4), datetime.date(2018, 9, 17), '$1751.01'],
        #[20, 'You', datetime.date(2018, 7, 21), datetime.date(2018, 9, 23), '$1496.36'],
        #[21, 'Up', datetime.date(2018, 8, 17), datetime.date(2018, 9, 18), '$1133.88'],
        #[22, 'Never', datetime.date(2018, 8, 8), datetime.date(2018, 9, 24), '$1520.95'],
        #[23, 'Gonna', datetime.date(2018, 9, 1), datetime.date(2018, 9, 29), '$611.09'],
        #[24, 'Let', datetime.date(2018, 7, 5), datetime.date(2018, 9, 15), '$1736.03'],
    #]
    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    try:
        sql = """SELECT repairID, faultreport, startDate, endDate, cost FROM Repair WHERE doneTo = %s"""
        cursor.execute(sql, (device_id,))
        ret = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("could not get device repairs")
        cursor.close()
        connection.close()
        return None

    repairs = []
    for x in ret:
        tmp = [int(x[0]),x[1],x[2],x[3],x[4]]
        repairs.append(tmp)

    return repairs


#####################################################
#   Query (d[i])
#   Get Device Info
#####################################################

def get_device_information(device_id: int) -> Optional[dict]:
    """
    Get related device information in detail.
    """

    # Return all the relevant device information for the device

    #device_info = [
        #1,                      # DeviceID
        #'2721153188',           # SerialNumber
        #datetime.date(2017, 12, 19),  # PurchaseDate
        #'$1009.10',             # PurchaseCost
        #'Zoomzone',             # Manufacturer
        #'9854941272',           # ModelNumber
        #1337,                   # IssuedTo
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    device_info = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT deviceID, serialNumber, purchaseDate, purchaseCost, manufacturer, modelnumber, issuedTo
                        FROM Device WHERE deviceID = %s"""
        cur.execute(sql, [device_id])

        device_info = cur.fetchone()

    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)
    print(device_info)

    device = {
        'device_id': device_info[0],
        'serial_number': device_info[1],
        'purchase_date': device_info[2],
        'purchase_cost': device_info[3],
        'manufacturer': device_info[4],
        'model_number': device_info[5],
        'issued_to': device_info[6],
    }

    cur.close()
    conn.close()
    return device


#####################################################
#   Query (d[iii/iv])
#   Get Model Info by Device
#####################################################

def get_device_model(device_id: int) -> Optional[dict]:
    """
    Get model information about a device.
    """

    #model_info = [
        #'Zoomzone',              # manufacturer
        #'9854941272',            # modelNumber
        #'brick--I mean laptop',  # description
        #2000,                    # weight
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    model_info = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT Model.manufacturer, modelNumber, description, weight
        FROM Device JOIN Model USING (modelNumber)
        WHERE deviceID = %s"""
        cur.execute(sql, [device_id])
        model_info = cur.fetchone()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)

    model = {
        'manufacturer': model_info[0],
        'model_number': model_info[1],
        'description': model_info[2],
        'weight': model_info[3],
    }
    cur.close()
    conn.close()
    return model


#####################################################
#   Query (e)
#   Get Repair Details
#####################################################

def get_repair_details(repair_id: int) -> Optional[dict]:
    """
    Get information about a repair in detail, including service information.
    """

    #repair_info = [
        #17,                    # repair ID
        #'Never, The',          # fault report
        #datetime.date(2018, 7, 16),  # start date
        #datetime.date(2018, 9, 22),  # end date
        #'$837.13',             # cost
        #'12345678901',         # service ABN
        #'TopDrive',            # service name
        #'repair@example.com',  # service email
        #1,                     # done to device
    #]

    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    repair_info = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT repairID,faultReport,startDate,endDate,cost, abn, serviceName, email, doneTo
        FROM Repair, Service WHERE repairID = %s AND doneBy = abn"""
        cur.execute(sql, [repair_id])
        repair_info = cur.fetchone()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)

    repair = {
        'repair_id': repair_info[0],
        'fault_report': repair_info[1],
        'start_date': repair_info[2],
        'end_date': repair_info[3],
        'cost': repair_info[4],
        'done_by': {
            'abn': repair_info[5],
            'service_name': repair_info[6],
            'email': repair_info[7],
        },
        'done_to': repair_info[8],
    }
    cur.close()
    conn.close()
    return repair


#####################################################
#   Query (f[ii])
#   Get Models assigned to Department
#####################################################

def get_department_models(department_name: str) -> list:
    """
    Return all models assigned to a department.
    """


    # Return the models allocated to the department.
    # Each "row" has: [ manufacturer, modelnumber, maxnumber ]

    #model_allocations = [
        #['Devpulse', '4030141218', 153],
        #['Gabcube', '1666158895', 186],
        #['Feednation', '2050267274', 275],
        #['Zoombox', '8860068207', 199],
        #['Shufflebeat', '0288809602', 208],
        #['Voonyx', '5275001460', 264],
        #['Tagpad', '3772470904', 227],
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    model_allocations = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT manufacturer,modelNumber,maxNumber FROM ModelAllocations  JOIN Department ON (department = name)
        WHERE name = %s"""
        cur.execute(sql, [department_name])
        model_allocations = cur.fetchall()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)
    cur.close()
    conn.close()

    return model_allocations


#####################################################
#   Query (f[iii])
#   Get Number of Devices of Model owned
#   by Employee in Department
#####################################################

def get_employee_department_model_device(department_name: str, manufacturer: str, model_number: str) -> list:
    """
    Get the number of devices owned per employee in a department
    matching the model.

    E.g. Model = iPhone, Manufacturer = Apple, Department = "Accounting"
        - [ 1337, Misty, 20 ]
        - [ 351, Pikachu, 10 ]
    """

    # Return the number of devices owned by each employee matching department,
    #   manufacturer and model.
    # Each "row" has: [ empid, name, number of devices issued that match ]

    #employee_counts = [
        #[1337, 'Misty', 20],
        #[351, 'Pikachu', 1],
        #[919, 'Hermione', 8],
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    employee_counts = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT empID, name, COUNT(deviceId)
        FROM (Employee JOIN EmployeeDepartments USING(empID)) JOIN Device ON (issuedTo = empID)
        WHERE department = %s AND modelNumber = %s AND manufacturer = %s
        GROUP BY (empID, name);"""
        cur.execute(sql , (department_name,model_number,manufacturer))
        ret = cur.fetchall()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)
        cur.close()
        conn.close()
        return None

    for x in ret:
        tmp = []
        tmp.append(x[0])
        tmp.append(x[1])
        tmp.append(x[2])
        employee_counts.append(tmp)

    cur.close()
    conn.close()

    return employee_counts


#####################################################
#   Query (f[iv])
#   Get a list of devices for a certain model and
#       have a boolean showing if the employee has
#       it issued.
#####################################################

def get_model_device_assigned(model_number: str, manufacturer: str, employee_id: int) -> list:
    """
    Get all devices matching the model and manufacturer and show True/False
    if the employee has the device assigned.

    E.g. Model = Pixel 2, Manufacturer = Google, employee_id = 1337
        - [123656, False]
        - [123132, True]
        - [51413, True]
        - [8765, False]
    """


    # Return each device of this model and whether the employee has it
    # issued.
    # Each "row" has: [ device_id, True if issued, else False.]

    #device_assigned = [
        #[123656, False],
        #[123132, True],
        #[51413, True],
        #[8765, False],
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    device_assigned = []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT deviceID,
        CASE WHEN issuedTo = %s THEN 'True'
        ELSE 'False'
        END AS Issued
        FROM Employee CROSS JOIN Device
        WHERE empID = %s AND modelNumber = %s AND manufacturer = %s"""

        cur.execute(sql ,(employee_id,employee_id,model_number,manufacturer) )

        ret = cur.fetchall()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)
        cur.close()
        conn.close()
        return None

    for x in ret:
        tmp = []
        tmp.append(x[0])
        tmp.append(x[1])
        device_assigned.append(tmp)

    cur.close()
    conn.close()

    return device_assigned


#####################################################
#   Get a list of devices for this model and
#       manufacturer that have not been assigned.
#####################################################

def get_unassigned_devices_for_model(model_number: str, manufacturer: str) -> list:
    """
    Get all unassigned devices for the model.
    """

    # Return each device of this model that has not been issued
    # Each "row" has: [ device_id ]
    #device_unissued = [123656, 123132, 51413, 8765]
    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    #print(model_number)
    #print(manufacturer)
    try:
        sql = """SELECT deviceID from Device
            WHERE modelNumber = %s AND manufacturer = %s AND issuedTo IS NULL """
        cursor.execute(sql,(model_number,manufacturer))
        #print('execute fine')
        ret = cursor.fetchall()
        cursor.close()
        connection.close()
    except:
        print("get_unassigned_devices_for_model failed")
        cursor.close()
        connection.close()
        return []

    #print(ret)
    if (ret is None):
        return []

    device_unissued = []
    for x in ret:
        device_unissued.append(int(x[0]))

    return device_unissued


#####################################################
#   Get Employees in Department
#####################################################

def get_employees_in_department(department_name: str) -> list:
    """
    Return all the employees' IDs and names in a given department.
    """


    # Return the employees in the department.
    # Each "row" has: [ empid, name ]

    #employees = [
        #[15905, 'Rea Fibbings'],
        #[9438, 'Julia Norville'],
        #[36020, 'Adora Lansdowne'],
        #[98809, 'Nathanial Farfoot'],
        #[58407, 'Lynne Smorthit'],
    #]
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    employees= []
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        sql = """SELECT empID, name FROM Employee NATURAL JOIN EmployeeDepartments
            WHERE department = %s"""
        cur.execute(sql, [department_name])
        ret = cur.fetchall()
    except Exception as e:
        # This happens if there is an error executing the query
        print("Error executing function")
        print(e)
        cur.close()
        conn.close()
        return []

    cur.close()
    conn.close()

    for x in ret:
        tmp = []
        tmp.append(x[0])
        tmp.append(x[1])
        employees.append(tmp)

    return employees


#####################################################
#   Query (f[v])
#   Issue Device
#####################################################

def issue_device_to_employee(employee_id: int, device_id: int):
    """
    Issue the device to the chosen employee.
    """

    # Return (True, None) if all good
    # Else return (False, ErrorMsg)
    # Error messages:
    #       - Device already issued?
    #       - Employee not in department?

    # return (False, "Device already issued")
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()

    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT
        check = """SELECT CASE WHEN issuedTo IS NULL THEN FALSE
                    ELSE TRUE
               END AS issued
               FROM Device
               WHERE deviceID = %s"""
        cur.execute(check,[device_id])
        ret = cur.fetchone()
        #print(ret)
        if(ret[0] == True):
            cur.close()
            conn.close()
            return (False, "Device already issued")

        sql = """UPDATE Device
                        SET issuedTo = %s
                        WHERE deviceID = %s AND issuedTo IS NULL;"""
        cur.execute(sql, (employee_id,device_id))
        conn.commit()
    except:
        cur.close()
        conn.close()
        return (False, "Device already issued")

    cur.close()
    conn.close()
    return (True, None)



#####################################################
#   Query (f[vi])
#   Revoke Device Issued to User
#####################################################

def revoke_device_from_employee(employee_id: int, device_id: int):
    """
    Revoke the device from the employee.
    """


    # Return (True, None) if all good
    # Else return (False, ErrorMsg)
    # Error messages:
    #       - Device already revoked?
    #       - employee not assigned to device?

    # return (False, "Device already unassigned")
    conn = database_connect()
    if(conn is None):
        return None
    cur = conn.cursor()
    try:
        # Try getting all the information returned from the query
        # NOTE: column ordering is IMPORTANT

        #first check
        check = """SELECT CASE WHEN issuedTo IS NULL THEN FALSE
                    ELSE TRUE
               END AS issued
               FROM Device
               WHERE deviceID = %s"""
        cur.execute(check,[device_id])
        ret = cur.fetchone()
        #print(ret)
        if(ret[0] == False):
            cur.close()
            conn.close()
            return (False, "Device already revoked")

        #second check
        check2 = """SELECT CASE WHEN issuedTo = %s THEN TRUE
                    ELSE FALSE
                    END AS assignedToThis
                    FROM Device
                    WHERE deviceID = %s"""
        cur.execute(check2,(employee_id,device_id))
        ret = cur.fetchone()
        #print(ret)
        if(ret[0] == False):
            cur.close()
            conn.close()
            return (False, "employee not assigned to device?")

        sql = """UPDATE Device
                    SET issuedTo = NULL
                    WHERE  issuedTo = %s AND deviceID = %s AND issuedTo IS NOT NULL;"""
        cur.execute(sql,(employee_id,device_id))
        conn.commit()

    except:
        cur.close()
        conn.close()
        return (False, "Device already unassigned")

    cur.close()
    conn.close()
    return (True, None)



#####################################################
#   Extension (1)
#   Get each department ranked by productivity, show average worked fraction and the max/least worked by an employee
#####################################################

def productivity_rank() -> list:
    """
    Get rank for departments based on productivity and show the max/least fraction worked by an employee
    """

    # Return the ranking of departments and some fraction data
    # Each "Row" contains: [rank, department, avg fraction, max fraction, min fraction]
    # If No departments = EMPTY LIST []

    #data = [
        #[12, 'Training', 26.19, 62.53, 2.58 ],

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    data = []
    try:
        sql = """SELECT rank() over (ORDER BY (SUM(fraction)/count(fraction))desc), department, round (( SUM(fraction)/count(fraction) ),2 )as average_worked_fraction, max(fraction) as most_worked_fraction , min(fraction) as least_worked_fraction
                    from EmployeeDepartments
                    group by department
                    order by rank, most_worked_fraction, department"""
        cursor.execute(sql)
        ret = cursor.fetchall()
    except:
        print("could not get department rankings")
        cursor.close()
        connection.close()
        return None

    for x in ret:
        tmp = [x[0],x[1],x[2],x[3],x[4]]
        data.append(tmp)

    cursor.close()
    connection.close()
    return data

#####################################################
#   Extension (2)
#   find the most spent on one service in each month, + total spending on that company for that month ,
#   max transaction with that company and repairIDS associated
#####################################################

def most_spent_per_month() -> list:

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    data = []
    try:
        sql = """SELECT extract(month FROM k.startdate), extract(year FROM k.startdate) , sum(k.cost), string_agg( cast(k.repairid as varchar(5)) , ',' ORDER BY k.repairid ) as repair_ids
        ,abn, servicename, max(cost)

        FROM repair k JOIN  service ON (doneby = abn)
        WHERE --extract(year FROM k.startdate) >= 2018 AND  extract(month FROM k.startdate) >= 6

        abn =  (SELECT abn
                      FROM service, repair
                      where repair.doneby = abn
                      AND extract(year FROM startdate) = extract(year FROM k.startdate) AND  extract(month FROM startdate) = extract(month FROM k.startdate)
                      GROUP BY abn
                      ORDER BY SUM(cost)  DESC
                      LIMIT 1)

        AND servicename =  (SELECT servicename
                      FROM service, repair
                      where repair.doneby = abn
                      AND extract(year FROM startdate) = extract(year FROM k.startdate) AND  extract(month FROM startdate) = extract(month FROM k.startdate)
                      GROUP BY servicename
                      ORDER BY SUM(cost)  DESC
                      LIMIT 1)

        GROUP BY extract(month FROM startdate), extract(year FROM startdate), abn
        ORDER BY extract(month FROM startdate), extract(year FROM startdate)"""

        cursor.execute(sql)
        ret = cursor.fetchall()
    except:
        print("could not get spending per month")
        cursor.close()
        connection.close()
        return None

    for x in ret:
        tmp = [int(x[0]),int(x[1]),x[2],x[3],x[4],x[5],x[6]]
        data.append(tmp)
    cursor.close()
    connection.close()
    return data

#####################################################
#   Extension (3)
#   Find manager of each deparment and their phonenumber(s)
#####################################################

def manager_department_phone() -> list:

    connection = database_connect()
    if (connection is None):
        return None
    cursor = connection.cursor()
    data = []
    try:
        sql = """SELECT d.name, d.manager, emp.name, e.phonenumber
                FROM Department d JOIN  EmployeePhoneNumbers e ON (d.manager = e.empid)
                JOIN employee emp ON (d.manager = emp.empid)
                ORDER BY d.name, emp.name"""

        cursor.execute(sql)
        ret = cursor.fetchall()
    except:
        print("could not get phone numbers")
        cursor.close()
        connection.close()
        return None

    for x in ret:
        tmp = [x[0],x[1],x[2],x[3]]
        data.append(tmp)
    cursor.close()
    connection.close()
    return data
