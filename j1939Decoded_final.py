# ---------------------------------
# DECODING J1939
# Thesis: Decoding J1939 to Enable Conditions-based Maintenance
# Whitaker, Michael Capt, USMC
#
# Python 3.6
#
# ---------------------------------
import sqlite3  # import the sqlite 3 extensions for Python
import csv
import glob
import os
import datetime


file_list = glob.glob(os.path.join(os.getcwd() + '/LOG FILES/', "20 marc mtvr 592420", "*.TXT"))
datalines = []

for file_path in file_list:
    with open(file_path) as f_input:
        datalines.extend(f_input.readlines())
print(datalines[0:10])  # shows the first few entries in the list. These entries should match the log file

# remove header information, start from end of list as not to skip index from values shifting
for line in reversed(datalines):
    if line.startswith("#"):
        datalines.remove(line)
    if line.startswith('T'):
        datalines.remove(line)
# print(datalines[0:10])  # shows the first few entries in the list after cleaning

# ####################################################################################################

# P A R S I N  G   T H E   L O G   F I L E    A N D   C R E A T IN G   D A T A   S T R U C T U R E S
# T O   H O L D    D A T A   F O R   F U R   H E R   P R O  C E S S I N G

# ####################################################################################################
DLCs = {}  # Data length Code Dictionary that will be used to find all IDs.
timeStamp_list = []  # store the timestamp from the log file
IDList = []  # store the IDs in sequence
rawData = []  # Store the data fields in sequence

for line in datalines:  # iterate through the entire file starting at line 0
    elements = line.split(';')  # Separate the entries by commas
    # convert Time to iso and parse
    t = elements[0] # 20T155058861
    dt_obj = datetime.datetime.strptime('201903' + t, '%Y%m%dT%H%M%S%f')
    t = int(dt_obj.strftime('%Y%m%d%H%M%S%f'))
    timeStamp_list.append(t)  # add timestamp to list
    # ## this took almost 3 minutes
    #  '20170828T160536247'
    ID = elements[2]  # extract the PGN associated with the CAN identifier
    # cf00400 or 18febf0b
    IDList.append(ID)  # add the ID to a list

    # DLCText = 8  # The DLC text field has a colon, so separate the label from the text
    # Dlc:8
    DLC = 8  # Get the data length code as an integer
    #  8
    # Store the DLC in a dictionary with the ID as a key. This will ensure only unique IDs are used as keys.
    DLCs[ID] = DLC
    # {'14fef031': 8}
    # this splits up the HEX value into SPNs
    templist = []  # hold the 32 bit hex value as I build it
    for i in range(len(elements[3][:-1])):
        if i % 2 == 0:
            templist.append(elements[3][:-1][i] + elements[3][:-1][i + 1])  # store the data field as a list of text strings
    rawData.append(templist)
    # ['ff', 'ff', 'ff', 'ff', 'ff', 'fc', '00', 'ff']
IDs = DLCs.keys()  # the keys in the DLCs dictionary are all the unique IDs that have not been duplicated.

# print('ID\tDLC')  # Print the heading of a table
# for ID in sorted(DLCs.keys()):  # iterate through all the sorted dictionary keys
#     print('%s\t%i' % (ID, DLCs[ID]))  # display the dictionary contents
#
# print('Number of Unique IDs: %i' % len(IDs))  # display the total number of messages
# ####################################################################################################

# C R E A T I N G     A   D A T A B A S E
'''Create a database to decode the raw data in accordance with SAE Technical Standards.  The data inputed into
the database id copyrighted by SAE'''

# ####################################################################################################
# ### Digital Annex ###
pathtoDA        = '/Users/whitakermichael/NPS/_THESIS/DigitialAnnex/SAEDigitalSAEcsvs'
SPNsandPGNs     = os.path.join(pathtoDA, "SPNsandPGNs.csv")
Slots           = os.path.join(pathtoDA, "slots.csv")
src_address_hwy = os.path.join(pathtoDA, "SourceAddressesOnHighway.csv")
man             = os.path.join(pathtoDA, "Manfuacturers.csv")
glb_addr        = os.path.join(pathtoDA, "SourceAddresses.csv")


# #SQL Statements to create tables
nameofdatabse = "SAEJ1939_db"
filename = '/Users/whitakermichael/NPS/_THESIS/Decoding J1939/' + nameofdatabse
if os.path.isfile(filename):
    os.remove(filename)  # delete the database if it already exists. This prevents errors for duplicate tables

conn = sqlite3.connect(filename)
conn.text_factory = str  # This command enables the strings to be decoded by the sqlite commands
cursor = conn.cursor()
cursor.execute('CREATE TABLE SourceAddressesOnHighWay (ID INT, Name STRING, Notes STRING, DateModified STRING)')
cursor.execute('CREATE TABLE Manufacturers (ID INT,MANUFACTURER,LOCATION,DateLastModified)')
cursor.execute('CREATE TABLE SLOTS (SLOTIdentifier INT,SLOTName STRING,SLOTType STRING,Scaling STRING,'
               'Range STRING,Offset STRING, Length STRING ,DateModified STRING)')
cursor.execute('CREATE TABLE SPNandPGN (PGN INT, ParameterGroupLabel, PGNLength, TransmissionRate, Acronym, pos,'
               'SPNlength INT, SPN INT, Name, Description, DataRange, OperationalRange, Resolution, Offset, Units,'
               'DateSPNAddedToPGN, StatusOfSPNAdditionToPGN, DateSPNModified, SPNDoc, PGNDoc, BitField)')
cursor.execute('CREATE TABLE SourceAddresses (ID INT, Name STRING, Notes STRING, DateModified STRING)')

with open(src_address_hwy, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cursor.execute('INSERT INTO SourceAddressesOnHighWay VALUES (?,?,?,?)', row)
with open(man, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cursor.execute('INSERT INTO Manufacturers VALUES (?,?,?,?)', row)
with open(SPNsandPGNs, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cursor.execute('INSERT INTO SPNandPGN VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row)
with open(Slots, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cursor.execute('INSERT INTO SLOTS VALUES (?,?,?,?,?,?,?,?)', row)
with open(glb_addr, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        cursor.execute('INSERT INTO SourceAddresses VALUES (?,?,?,?)', row)

conn.commit()  # save changes

# Test to make sure tables are uploaded in database
cursor.execute('SELECT * from SourceAddresses').fetchone()
# goto terminal log into database and fix unicode error
# ('\ufeff0', <- THIS IS THE UNICODE ERROR
#  'Engine #1',
#  'The #1 on the Engine CA is to identify that this is the first PA being used for the particular function, Engine.
#  It may only be used for the NAME Function of 0, Function Instance 0, and an ecu instance of 0, which is commonly
#  know as the “first engine”.',
#  '')

# ########################################
# RUN THIS IF DATABASE IS ALREADY CREATED
# ########################################
nameofdatabse = "SAEJ1939_db"
filename = '/Users/whitakermichael/NPS/_THESIS/Decoding J1939/' + nameofdatabse
conn = sqlite3.connect(filename)
conn.text_factory = str  # This command enables the strings to be decoded by the sqlite commands
cursor = conn.cursor()
# TEST
cursor.execute('SELECT * from SourceAddresses').fetchone()

# ####################################################################################################

# D E C O D I N G   J 1 9 3 9

'''Use database and SAE technical specificaitions to parse 64 bit message and 18 bit PGN header.
This allows us to pull out functions from ECU communication in vehicles.'''
# ####################################################################################################
SA_Name = {}  # Source Address
SA_Note = {}  # Source notes
SPNs = {}
decodingDictionary = {}
for IDText in sorted(IDs):
    SAText = IDText[-2:]
    try:
        SA = int(SAText, 16)  # Convert the hex string into an integer
    except ValueError:
        print("SAText:", SAText)
        print("IDtext:", IDText)
        continue
    PFText = IDText[-6:-4]
    PF = int(PFText, 16)
    if PF < 240:  # PDU1 format
        DA = int(IDText[-4:-2], 16)
        PSText = '00'
        PS = DA
    else:
        PSText = IDText[-4:-2]
        PS = int(PSText, 16)
        DA = 255  # Broadcast
    PriorityText = IDText[-8:-6]
    Priority = int(PriorityText, 16) >> 2  # bit shift the priority integer by 2 to account for hte Dp and EDP fields.
    print('\nMessage ID: %s' % IDText)
    decodingDictionary.setdefault(IDText, [])
    print('Priority (Hex): %s' % PriorityText)
    print('J1939 Priority: %i' % Priority)
    decodingDictionary[IDText].append(Priority)
    print('PDU Format (PF) in hex: %s and in decimal: %i' % (PFText, PF))
    decodingDictionary[IDText].append(PF)
    print('PDU Specific (PS) in hex: %s and in decimal: %i' % (PSText, PS))
    decodingDictionary[IDText].append(PS)
    print('Source Address in hex: %s and in decimal: %i' % (SAText, SA))
    decodingDictionary[IDText].append(SA)
    PGNText = PFText + PSText
    PGN = int(PGNText, 16)
    print('Parameter Group Number (PGN): %i' % PGN)
    decodingDictionary[IDText].append(PGN)
    PGNData = cursor.execute('SELECT ParameterGroupLabel,TransmissionRate,Acronym FROM SPNandPGN WHERE PGN=?',
                             [PGN]).fetchone()
    try:
        print('Parameter Group Label: %s' % PGNData[0])
        decodingDictionary[IDText].append(PGNData[0])
    except TypeError:
        break
    print('Transmission Rate: %s' % PGNData[1])
    decodingDictionary[IDText].append(PGNData[1])
    acronym = str(PGNData[2])
    print('Acronym: %s' % acronym)
    decodingDictionary[IDText].append(acronym)
    # Source Addresses
    if SA < 94 or SA > 247:
        sourceAddressData = cursor.execute('SELECT Name,Notes FROM SourceAddresses WHERE ID=?', [SA]).fetchone()
        SA_Name[SA] = sourceAddressData[0]
        SA_Note[SA] = sourceAddressData[1]
    elif SA > 159 and SA < 248:
        sourceAddressData = cursor.execute('SELECT Name,Notes FROM SourceAddressesOnHighWay WHERE ID=?', [SA]).fetchone()
        SA_Name[SA] = sourceAddressData[0]
        SA_Note[SA] = sourceAddressData[1]
    else:
        SA_Name[SA] = 'SAE Future Use'
        SA_Note[SA] = 'Used SAE future Use or for dynamic address assignment'
    print('Source Controller: %s' % SA_Name[SA])
    decodingDictionary[IDText].append(SA_Name[SA])
    print('The Following SPNs are available in the message:')
    TempDict = {}
    for SPNData in cursor.execute('SELECT SPN,Name,Units,Offset,Resolution,pos, SPNlength, DataRange FROM SPNandPGN WHERE PGN=?', [PGN]):
        try:
            SPN = int(SPNData[0])
        except ValueError:
            SPN = -1
        Name  = str(SPNData[1])
        Units = str(SPNData[2])
        # PARSE OFFSET VALUE
        offset_val = SPNData[3].split()  # ['-40', '°C']
        if len(offset_val) > 0:
            try:
                Offset = float(offset_val[0].replace(',', ''))
            except ValueError:
                Offset = 0
        else:
            Offset = 0

        # PARSE RESOLUTION
        res = SPNData[4].split(' ')
        res = res[0].split('/bit')
        res = res[0].split('/')
        try:
            if len(res) == 2:
                Resolution = float(res[0]) / float(res[1])
            else:
                Resolution = float(res[0])
        except ValueError:
            Resolution = 0
        #  PARSE BIT START POSITION FILL WITH -GRAND VAL
        try:
            pos = int(SPNData[5][0])
        except IndexError:
            pos = int(-88888)
        length_measure = SPNData[6]
        try:
            if length_measure.split()[1].startswith('bit'):
                continue
        except IndexError:
            continue
        #  PARSE DATA RANGE
        temp_datarange_list = []
        data_range_str = SPNData[7]
        for val in data_range_str.split():
            try:
                min_or_max_val = float(val.replace(',', ''))
            except ValueError:
                min_or_max_val = ()
                continue
            temp_datarange_list.append(min_or_max_val)
        date_Range = tuple(temp_datarange_list)
        if len(date_Range) < 1:
            # date_Range = (-8888, -88888)
            continue
        SPNs[SPN] = [Name, Units, Offset, Resolution, (IDText, PGN), acronym, pos, length_measure, date_Range]
        TempDict[SPN] = [Name, Units, ('Offset', Offset), Resolution, (IDText, PGN), acronym, ('pos', pos, length_measure), date_Range]
        # print(SPNData[4])
        print('SPN: %i, Name: %s, Unit: %s, Offset: %g, Resolution: %g, BitStartPosition: %i, length: %s '
              'DataRange: min= %d, max= %d' % (SPN, Name, Units, Offset, Resolution, pos, length_measure, date_Range[0], date_Range[1]))
    decodingDictionary[IDText].append(TempDict)

# close connection to Database
# conn.close()

# ####################################################################################################

#                C R E A T E  T A B L  E   T O    H O L D   O U T P U T   O F
#                        D E C O D E D   J 1 9 3 9   M E S S A G E S

# ####################################################################################################

conn = sqlite3.connect(filename)
conn.text_factory = str  # This command enables the strings to be decoded by the sqlite commands
cursor = conn.cursor()

cursor.execute('DROP TABLE decodedValues')  # drop table if it exist
cursor.execute('CREATE TABLE decodedValues (time FLOAT,ID STRING, PF INT, PS INT, DA INT, PGN INT, SA INT, Priority INT,'
               'Acronym STRING, Meaning STRING, Unitss STRING) ')

# ADD SPN COLUMN TO TABLE FOR EVERY UNIQUE SPN VALUE
for SPN in sorted(SPNs.keys()):
    print('{}\t{}\t{}' .format(SPNs[SPN][4], SPN, SPNs[SPN][0]))
    cursor.execute('ALTER TABLE decodedValues ADD COLUMN SPN%i' % SPN)  # This adds only the SPNs that are in the data stream.

# #####################################################################################################

#               T H I S    A D D S   D E C O D E D   V A L U E S   T O   D A T A B A S E

# #####################################################################################################
spn_list = [k for k, v in SPNs.items()] # added to test specific spns
for timestamp, PGNID, payload in zip(timeStamp_list, IDList, rawData):
    # "20190320173110134000", "18f00010", ['c0', '7d', 'ff', 'ff', 'ff', 'ff', 'ff', 'ff']
    for key, value in decodingDictionary.items():
        if PGNID[-6:] == key[-6:]:
            for spn, spnDataValue in value[9].items():  # this is the spn dictionary
                # if spn in spn_list: #this was added to test specific SPNs
                position = spnDataValue[6][1]
                SP_length = spnDataValue[6][2].split()[0]
                SPN = spn
                Unit = spnDataValue[1]
                Resolution = spnDataValue[3]
                Offset = spnDataValue[2][1]
                Max_dataRange = spnDataValue[7][1]
                if position < 8:
                    if int(SP_length) > 1:
                        # intel byte order (the right byte switched with left byte)
                        # this is why 0-based index is working out
                        binary = int(payload[position] + payload[position - 1], 16)
                        Value = Resolution * binary + Offset
                        if Value > Max_dataRange:
                            if Unit == 'count':
                                Value = 0
                            else:
                                Value = 9999  # THIS MEANS NO DATA (NA)
                        # print(f'{timestamp} -  {PGNID} - {SPN} - {Value}')
                        cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
                                       VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
                                       % (spn, timestamp, PGNID, decodingDictionary[key][1],
                                          decodingDictionary[key][2], decodingDictionary[key][4],
                                          decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))
                    else:
                        # do this
                        binary = int(payload[position-1], 16)
                        Value = Resolution * binary + Offset
                        if Value > Max_dataRange:
                            if Unit == 'count':
                                Value = 0
                            else:
                                Value = 9999  # THIS MEANS NO DATA (NA) OR FALSE VALUES
                        # print(f'{timestamp} -  {PGNID} - {SPN} - {Value}')
                        cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
                                       VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
                                       % (spn, timestamp, PGNID, decodingDictionary[key][1],
                                          decodingDictionary[key][2], decodingDictionary[key][4],
                                          decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))

                else:
                    length, signal_range = spnDataValue[6][2].split()
                    if signal_range.startswith('bit'):
                        continue  # there should be no SPNs with bit specified for length
                    else:
                        binary = int(payload[position - int(length)], 16)  # makes it index 7 vs 8
                        Value = Resolution * binary + Offset
                        if Value > Max_dataRange:
                            if Unit == 'count':
                                Value = 0
                            else:
                                Value = 9999  # THIS MEANS NO DATA (NA)
                        cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
                                       VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
                                       % (spn, timestamp, PGNID, decodingDictionary[key][1],
                                          decodingDictionary[key][2], decodingDictionary[key][4],
                                          decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))
                # print(f'{timestamp} - {PGNID} - {SPN} - {Value}')
conn.commit()
conn.close()

# #####################################################################################################
#                                          E N D
# #####################################################################################################


# #####################################################################################################
#                TEST  RUN WITH DATABASE ALREADY CREATED ADD DATAVALUES AGAIN
# #####################################################################################################


# import sqlite3  # import the sqlite 3 extensions for Python
# import csv
# import glob
# import os
# import datetime
#
#
# file_list = glob.glob(os.path.join(os.getcwd() + '/LOG FILES/', "20 marc mtvr 592420", "*.TXT"))
# datalines = []
#
# for file_path in file_list:
#     with open(file_path) as f_input:
#         datalines.extend(f_input.readlines())
# print(datalines[0:10])  # shows the first few entries in the list. These entries should match the log file
#
# # remove header information, start from end of list as not to skip index from values shifting
# for line in reversed(datalines):
#     if line.startswith("#"):
#         datalines.remove(line)
#     if line.startswith('T'):
#         datalines.remove(line)
# # print(datalines[0:10])  # shows the first few entries in the list after cleaning
#
# # ####################################################################################################
#
# # P A R S I N  G   T H E   L O G   F I L E    A N D   C R E A T IN G   D A T A   S T R U C T U R E S
# # T O   H O L D    D A T A   F O R   F U R   H E R   P R O  C E S S I N G
#
# # ####################################################################################################
# DLCs = {}  # Data length Code Dictionary that will be used to find all IDs.
# timeStamp_list = []  # store the timestamp from the log file
# IDList = []  # store the IDs in sequence
# rawData = []  # Store the data fields in sequence
#
# for line in datalines:  # iterate through the entire file starting at line 0
#     elements = line.split(';')  # Separate the entries by commas
#     # convert Time to iso and parse
#     t = elements[0] # 20T155058861
#     dt_obj = datetime.datetime.strptime('201903' + t, '%Y%m%dT%H%M%S%f')
#     t = int(dt_obj.strftime('%Y%m%d%H%M%S%f'))
#     timeStamp_list.append(t)  # add timestamp to list
#     # ## this took almost 3 minutes
#     #  '20170828T160536247'
#     ID = elements[2]  # extract the PGN associated with the CAN identifier
#     # cf00400 or 18febf0b
#     IDList.append(ID)  # add the ID to a list
#
#     # DLCText = 8  # The DLC text field has a colon, so separate the label from the text
#     # Dlc:8
#     DLC = 8  # Get the data length code as an integer
#     #  8
#     # Store the DLC in a dictionary with the ID as a key. This will ensure only unique IDs are used as keys.
#     DLCs[ID] = DLC
#     # {'14fef031': 8}
#     # this splits up the HEX value into SPNs
#     templist = []  # hold the 32 bit hex value as I build it
#     for i in range(len(elements[3][:-1])):
#         if i % 2 == 0:
#             templist.append(elements[3][:-1][i] + elements[3][:-1][i + 1])  # store the data field as a list of text strings
#     rawData.append(templist)
#     # ['ff', 'ff', 'ff', 'ff', 'ff', 'fc', '00', 'ff']
# IDs = DLCs.keys()  # the keys in the DLCs dictionary are all the unique IDs that have not been duplicated.
#
# # print('ID\tDLC')  # Print the heading of a table
# # for ID in sorted(DLCs.keys()):  # iterate through all the sorted dictionary keys
# #     print('%s\t%i' % (ID, DLCs[ID]))  # display the dictionary contents
# #
# # print('Number of Unique IDs: %i' % len(IDs))  # display the total number of messages
# nameofdatabse = "SAEJ1939_db"
# filename = '/Users/whitakermichael/NPS/_THESIS/Decoding J1939/' + nameofdatabse
# conn = sqlite3.connect(filename)
# conn.text_factory = str  # This command enables the strings to be decoded by the sqlite commands
# cursor = conn.cursor()
# # TEST
# cursor.execute('SELECT * from SourceAddresses').fetchone()
#
# SA_Name = {}  # Source Address
# SA_Note = {}  # Source notes
# SPNs = {}
# decodingDictionary = {}
# for IDText in sorted(IDs):
#     SAText = IDText[-2:]
#     try:
#         SA = int(SAText, 16)  # Convert the hex string into an integer
#     except ValueError:
#         print("SAText:", SAText)
#         print("IDtext:", IDText)
#         continue
#     PFText = IDText[-6:-4]
#     PF = int(PFText, 16)
#     if PF < 240:  # PDU1 format
#         DA = int(IDText[-4:-2], 16)
#         PSText = '00'
#         PS = DA
#     else:
#         PSText = IDText[-4:-2]
#         PS = int(PSText, 16)
#         DA = 255  # Broadcast
#     PriorityText = IDText[-8:-6]
#     Priority = int(PriorityText, 16) >> 2  # bit shift the priority integer by 2 to account for hte Dp and EDP fields.
#     print('\nMessage ID: %s' % IDText)
#     decodingDictionary.setdefault(IDText, [])
#     print('Priority (Hex): %s' % PriorityText)
#     print('J1939 Priority: %i' % Priority)
#     decodingDictionary[IDText].append(Priority)
#     print('PDU Format (PF) in hex: %s and in decimal: %i' % (PFText, PF))
#     decodingDictionary[IDText].append(PF)
#     print('PDU Specific (PS) in hex: %s and in decimal: %i' % (PSText, PS))
#     decodingDictionary[IDText].append(PS)
#     print('Source Address in hex: %s and in decimal: %i' % (SAText, SA))
#     decodingDictionary[IDText].append(SA)
#     PGNText = PFText + PSText
#     PGN = int(PGNText, 16)
#     print('Parameter Group Number (PGN): %i' % PGN)
#     decodingDictionary[IDText].append(PGN)
#     PGNData = cursor.execute('SELECT ParameterGroupLabel,TransmissionRate,Acronym FROM SPNandPGN WHERE PGN=?',
#                              [PGN]).fetchone()
#     try:
#         print('Parameter Group Label: %s' % PGNData[0])
#         decodingDictionary[IDText].append(PGNData[0])
#     except TypeError:
#         break
#     print('Transmission Rate: %s' % PGNData[1])
#     decodingDictionary[IDText].append(PGNData[1])
#     acronym = str(PGNData[2])
#     print('Acronym: %s' % acronym)
#     decodingDictionary[IDText].append(acronym)
#     # Source Addresses
#     if SA < 94 or SA > 247:
#         sourceAddressData = cursor.execute('SELECT Name,Notes FROM SourceAddresses WHERE ID=?', [SA]).fetchone()
#         SA_Name[SA] = sourceAddressData[0]
#         SA_Note[SA] = sourceAddressData[1]
#     elif SA > 159 and SA < 248:
#         sourceAddressData = cursor.execute('SELECT Name,Notes FROM SourceAddressesOnHighWay WHERE ID=?', [SA]).fetchone()
#         SA_Name[SA] = sourceAddressData[0]
#         SA_Note[SA] = sourceAddressData[1]
#     else:
#         SA_Name[SA] = 'SAE Future Use'
#         SA_Note[SA] = 'Used SAE future Use or for dynamic address assignment'
#     print('Source Controller: %s' % SA_Name[SA])
#     decodingDictionary[IDText].append(SA_Name[SA])
#     print('The Following SPNs are available in the message:')
#     TempDict = {}
#     for SPNData in cursor.execute('SELECT SPN,Name,Units,Offset,Resolution,pos, SPNlength, DataRange FROM SPNandPGN WHERE PGN=?', [PGN]):
#         try:
#             SPN = int(SPNData[0])
#         except ValueError:
#             SPN = -1
#         Name  = str(SPNData[1])
#         Units = str(SPNData[2])
#         # PARSE OFFSET VALUE
#         offset_val = SPNData[3].split()  # ['-40', '°C']
#         if len(offset_val) > 0:
#             try:
#                 Offset = float(offset_val[0].replace(',', ''))
#             except ValueError:
#                 Offset = 0
#         else:
#             Offset = 0
#
#         # PARSE RESOLUTION
#         res = SPNData[4].split(' ')
#         res = res[0].split('/bit')
#         res = res[0].split('/')
#         try:
#             if len(res) == 2:
#                 Resolution = float(res[0]) / float(res[1])
#             else:
#                 Resolution = float(res[0])
#         except ValueError:
#             Resolution = 0
#         #  PARSE BIT START POSITION FILL WITH -GRAND VAL
#         try:
#             pos = int(SPNData[5][0])
#         except IndexError:
#             pos = int(-88888)
#         length_measure = SPNData[6]
#         try:
#             if length_measure.split()[1].startswith('bit'):
#                 continue
#         except IndexError:
#             continue
#         #  PARSE DATA RANGE
#         temp_datarange_list = []
#         data_range_str = SPNData[7]
#         for val in data_range_str.split():
#             try:
#                 min_or_max_val = float(val.replace(',', ''))
#             except ValueError:
#                 min_or_max_val = ()
#                 continue
#             temp_datarange_list.append(min_or_max_val)
#         date_Range = tuple(temp_datarange_list)
#         if len(date_Range) < 1:
#             # date_Range = (-8888, -88888)
#             continue
#         SPNs[SPN] = [Name, Units, Offset, Resolution, (IDText, PGN), acronym, pos, length_measure, date_Range]
#         TempDict[SPN] = [Name, Units, ('Offset', Offset), Resolution, (IDText, PGN), acronym, ('pos', pos, length_measure), date_Range]
#         # print(SPNData[4])
#         print('SPN: %i, Name: %s, Unit: %s, Offset: %g, Resolution: %g, BitStartPosition: %i, length: %s '
#               'DataRange: min= %d, max= %d' % (SPN, Name, Units, Offset, Resolution, pos, length_measure, date_Range[0], date_Range[1]))
# #     decodingDictionary[IDText].append(TempDict)
#
# cursor.execute('DROP TABLE decodedValues')  # drop table if it exist
# cursor.execute('CREATE TABLE decodedValues (time FLOAT,ID STRING, PF INT, PS INT, DA INT, PGN INT, SA INT, Priority INT,'
#                'Acronym STRING, Meaning STRING, Unitss STRING) ')
#
# #
# # ADD SPN COLUMN TO TABLE FOR EVERY UNIQUE SPN VALUE
# for SPN in sorted(SPNs.keys()):
#     print('{}\t{}\t{}' .format(SPNs[SPN][4], SPN, SPNs[SPN][0]))
#     cursor.execute('ALTER TABLE decodedValues ADD COLUMN SPN%i' % SPN)  # This adds only the SPNs that are in the data stream.
# spn_list = [k for k, v in SPNs.items()] # added to test specific spns
# for timestamp, PGNID, payload in zip(timeStamp_list, IDList, rawData):
#     # "20190320173110134000", "18f00010", ['c0', '7d', 'ff', 'ff', 'ff', 'ff', 'ff', 'ff']
#     for key, value in decodingDictionary.items():
#         if PGNID[-6:] == key[-6:]:
#             for spn, spnDataValue in value[9].items():  # this is the spn dictionary
#                 # if spn in spn_list: #this was added to test specific SPNs
#                 position = spnDataValue[6][1]
#                 SP_length = spnDataValue[6][2].split()[0]
#                 SPN = spn
#                 Unit = spnDataValue[1]
#                 Resolution = spnDataValue[3]
#                 Offset = spnDataValue[2][1]
#                 Max_dataRange = spnDataValue[7][1]
#                 if position < 8:
#                     if int(SP_length) > 1:
#                         # intel byte order (the right byte switched with left byte)
#                         # this is why 0-based index is working out
#                         binary = int(payload[position] + payload[position - 1], 16)
#                         Value = Resolution * binary + Offset
#                         if Value > Max_dataRange:
#                             if Unit == 'count':
#                                 Value = 0
#                             else:
#                                 Value = 9999  # THIS MEANS NO DATA (NA)
#                         # print(f'{timestamp} -  {PGNID} - {SPN} - {Value}')
#                         cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
#                                        VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
#                                        % (spn, timestamp, PGNID, decodingDictionary[key][1],
#                                           decodingDictionary[key][2], decodingDictionary[key][4],
#                                           decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))
#                     else:
#                         # do this
#                         binary = int(payload[position-1], 16)
#                         Value = Resolution * binary + Offset
#                         if Value > Max_dataRange:
#                             if Unit == 'count':
#                                 Value = 0
#                             else:
#                                 Value = 9999  # THIS MEANS NO DATA (NA) OR FALSE VALUES
#                         # print(f'{timestamp} -  {PGNID} - {SPN} - {Value}')
#                         cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
#                                        VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
#                                        % (spn, timestamp, PGNID, decodingDictionary[key][1],
#                                           decodingDictionary[key][2], decodingDictionary[key][4],
#                                           decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))
#
#                 else:
#                     length, signal_range = spnDataValue[6][2].split()
#                     if signal_range.startswith('bit'):
#                         continue  # there should be no SPNs with bit specified for length
#                     else:
#                         binary = int(payload[position - int(length)], 16)  # makes it index 7 vs 8
#                         Value = Resolution * binary + Offset
#                         if Value > Max_dataRange:
#                             if Unit == 'count':
#                                 Value = 0
#                             else:
#                                 Value = 9999  # THIS MEANS NO DATA (NA)
#                         cursor.execute('''INSERT INTO decodedValues(time, ID, PF, PS, PGN, SA, Priority, Unitss, SPN%i)
#                                        VALUES(%i, \"%s\", %i, %i, %i, %i, %i, \"%s\", %f)'''
#                                        % (spn, timestamp, PGNID, decodingDictionary[key][1],
#                                           decodingDictionary[key][2], decodingDictionary[key][4],
#                                           decodingDictionary[key][3], decodingDictionary[key][0], Unit, Value))
#                 # print(f'{timestamp} - {PGNID} - {SPN} - {Value}')
# conn.commit()
# conn.close()