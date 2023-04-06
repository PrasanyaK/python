import asyncio
from collections import namedtuple
import pyodbc
import pandas as pd
from xml.etree import ElementTree as ET
import urllib.parse
import requests
import socket
import traceback2 as trace
import pathlib
from datetime import datetime
import os
import numpy as np
from openpyxl import load_workbook
import shutil
import re
import uuid
import warnings
warnings.filterwarnings('ignore')
import public_ip as ip
import time
from multiprocessing import Pool, cpu_count, freeze_support
import dill


class ReRateDetails:
    def __init__(self):
        self.ref_id_array = []
        self.szip_array = []
        self.czip_array = []
        self.shipdate_array = []
        self.weight_array = []
        self.class_array = []
        self.pallet_array = []
        self.filename_array = []
        self.request_xml_array = []
        self.response_xml_array = []
        self.status_array = []

        self.rerate_data = {
            "ReferenceID": self.ref_id_array,
            "sZip": self.szip_array,
            "cZip": self.czip_array,
            "Shipdate": self.shipdate_array,
            "Weight": self.weight_array,
            "Class": self.class_array,
            "Pallet": self.pallet_array,
            "FileName": self.filename_array,
            "RequestXML": self.request_xml_array,
            "ResponseXML": self.response_xml_array,
            "Status": self.status_array
        }


class DataAccessLayer:
    def __init__(self):
        self.sql_query = ""

    async def insert_ip_address(self, db_connection, flag, mac_address, ip_address, host):
        db_cursor = db_connection.cursor()
        params = (flag, mac_address, ip_address, host)
        db_cursor.execute(
            "EXEC stp_automatedReRate2o @flag=?,@MacAddress=?,@IPAddress=?,@Desc=?", params)
        row = db_cursor.fetchone()
        db_cursor.commit()
        db_cursor.close()
        return str(row[0])

    async def get_zip_cache_details(self, db_connection, flag, sZip, cZip, host):
        self.sql_query = '''EXEC stp_automatedReRate2o @flag=?,@sZipCode=?,@cZipCode=?,@Desc=?'''
        df = pd.read_sql_query(self.sql_query, db_connection, params=[
                               flag, sZip, cZip, host])
        return df

    async def get_qb_client_code(self, db_connection, flag, host):
        self.sql_query = '''EXEC stp_automatedReRate2o @flag=?,@Desc=?'''
        df = pd.read_sql_query(
            self.sql_query, db_connection, params=[flag, host])
        return df

    async def insert_rerate_results(self, db_connection, flag, host, rerate_df):
        db_cursor = db_connection.cursor()
        params = (flag, host, rerate_df)
        db_cursor.execute(
            "EXEC stp_automatedReRate2o @flag=?,@Desc=?,@tblTypeReRateDetails=?", params)
        row = db_cursor.fetchone()
        db_cursor.commit()
        db_cursor.close()
        print("Completed inserting data ....")
        return str(row[0])

    async def get_rerate_results(self, db_connection, flag, host, fileName):
        self.sql_query = '''EXEC stp_automatedReRate2o @flag=?,@Desc=?,@fileName=?'''
        df = pd.read_sql_query(self.sql_query, db_connection, params=[
                               flag, host, fileName])
        return df

    async def get_contract_name(self, db_connection, flag, host, fileName, referenceID):
        self.sql_query = '''EXEC stp_automatedReRate2o @flag=?,@Desc=?,@fileName=?,@referenceID=?'''
        df = pd.read_sql_query(self.sql_query, db_connection, params=[
                               flag, host, fileName, referenceID])
        return df

    async def send_error_email(self, db_connection, flag, err_subject, err_body):
        db_cursor = db_connection.cursor()
        params = (flag, err_subject, err_body)
        db_cursor.execute(
            "EXEC stp_automatedReRate2o @flag=?,@errorSubject=?,@errorBody=?", params)
        db_cursor.commit()
        db_cursor.close()


class ReRate:
    def __init__(self):
        self.date_time = str(datetime.now().strftime("%Y%m%d%H%M%S%f"))
        self.hostname = socket.gethostname()
        self.current_path = pathlib.Path().resolve()
        self.directory = '{0}\\'.format(self.current_path)
        self.rerate_det = ReRateDetails()      
    async def rerate_api(self, send_paramter):
        api_start_time = datetime.now()
        XML_response = await requests.post('https://ltlrating.mytlx.com/LTLRateProcessorPost.aspx',
                                            data=send_paramter,
                                            headers={"Content-Type": "application/x-www-form-urlencoded"})
        api_end_time = datetime.now() - api_start_time
        print("Log 4 API response : ", str(api_end_time).split(".")[0] + f".{api_end_time.microseconds:03}")
        return XML_response
        
    async def frame_request(self,row,Input_data,strQBClientCode,no_ref_list,contract_id,client_id):
        
        pallet = 1
        

        if 'Pallet' in Input_data.columns:
            pallet = row['Pallet']


        
        if(row["sCity"] is not None and row["sState"] is not None and row["sCountry"] is not None and row["cCity"] is not None and row["cState"] is not None and row["cCountry"] is not None ):
            if contract_id == 0:
                XML_request = f"""<?xml version=\"1.0\"?><RateRequest><Constraints><Carrier/><Mode/><ServiceFlags/></Constraints>
                    <Items>
                    <Item sequence="2" freightClass="{row['Class']}">
                    <Weight units="lb">{row['Weight']}</Weight>
                    <Quantity units="pallets">{pallet}</Quantity>
                    </Item>
                    </Items>
                    <Events>
                    <Event sequence="1" type="Pickup" date="{row['ShipDate']}">
                    <Location>
                    <City>{row['sCity']}</City><State>{row['sState']}</State><Zip>{row['sZip']}</Zip><Country>{row['sCountry']}</Country></Location></Event>
                    <Event sequence="2" type="Drop" date="{row["ShipDate"]}">
                    <Location><City>{row['cCity']}</City><State>{row['cState']}</State><Zip>{row['cZip']}</Zip><Country>{row['cCountry']}</Country></Location></Event>
                    </Events>
                    <Accessorial>
                    <Code>LFT</Code><Code>REP</Code><Code>IDL</Code>
                    <Code>NFY</Code><Code>APO</Code><Code>HLS</Code>
                    <Code>HAZ</Code><Code>REW</Code></Accessorial>
                    <Request>
                    <UserName>Palldemo</UserName>
                    <Password>demo</Password>
                    <ClientID>{client_id}</ClientID>                                
                    </Request>
                    </RateRequest>"""
            else:
                XML_request = f"""<?xml version=\"1.0\"?><RateRequest><Constraints><Carrier/><Mode/><ServiceFlags/></Constraints>
                                                <Items>
                                                <Item sequence="2" freightClass="{row['Class']}">
                                                <Weight units="lb">{row['Weight']}</Weight>
                                                <Quantity units="pallets">{pallet}</Quantity>
                                                </Item>
                                                </Items>
                                                <Events>
                                                <Event sequence="1" type="Pickup" date="{row['ShipDate']}">
                                                <Location>
                                                <City>{row['sCity']}</City><State>{row['sState']}</State><Zip>{row['sZip']}</Zip><Country>{row['sCountry']}</Country></Location></Event>
                                                <Event sequence="2" type="Drop" date="{row["ShipDate"]}">
                                                <Location><City>{row['cCity']}</City><State>{row['cState']}</State><Zip>{row['cZip']}</Zip><Country>{row['cCountry']}</Country></Location></Event>
                                                </Events>
                                                <Accessorial>
                                                <Code>LFT</Code><Code>REP</Code><Code>IDL</Code>
                                                <Code>NFY</Code><Code>APO</Code><Code>HLS</Code>
                                                <Code>HAZ</Code><Code>REW</Code></Accessorial>
                                                <Request>
                                                <UserName>Palldemo</UserName>
                                                <Password>demo</Password>
                                                <ClientID>{client_id}</ClientID>  
                                                <Contarct>{contract_id}</Contarct>                                                     
                                                </Request>
                                                </RateRequest>"""
            
            XML = XML_request
            print(XML_request)
            XML_request = urllib.parse.quote(XML_request)
            send_paramter = f"userid={strQBClientCode}_webrate&password={strQBClientCode}_password&request={XML_request}"
            
            XML_response = await self.rerate_api(send_paramter)
            XML_response = XML_response.text
            print(XML_response)
            self.rerate_det.ref_id_array.append(row['ReferenceID'])
            self.rerate_det.szip_array.append(row['sZip'])
            self.rerate_det.czip_array.append(row['cZip'])
            self.rerate_det.shipdate_array.append(row['ShipDate'])
            self.rerate_det.weight_array.append(row['Weight'])
            self.rerate_det.class_array.append(row['Class'])
            self.rerate_det.pallet_array.append(pallet)
            self.rerate_det.filename_array.append(
                self.insert_file_name)
            self.rerate_det.request_xml_array.append(XML)
            self.rerate_det.response_xml_array.append(XML_response)
            self.rerate_det.status_array.append('S')
            # print("Completed processing ",
            #         str(row['ReferenceID']))
        else:
            no_ref_list.append("Skipped ReferenceID " + str(
                row['ReferenceID'])+". Please validate sZip : "+ str(row['sZip']) + " and cZip : "+ str(row['cZip'])+ " in Gadget and if valid insert in tbl_MasZipCache")
        return self.rerate_det

    async def rerate(self):
        num_cores = cpu_count()
        pool = Pool(num_cores)
        output_file_name = 'Rerate_Output'+self.date_time

        output_file_path = str(self.current_path) + \
            "\\output\\"+output_file_name+".xlsx"

        path = str(self.current_path)+"\\"+"input"+"\\"
        dir_list = os.listdir(path)
        file = []
        file_extension = ""
        for i in dir_list:
            pattern_xlsx = ("\w+.xlsx\Z")
            pattern_xls = ("\w+.xls\Z")
            if (re.match(pattern_xlsx, i)) or (re.match(pattern_xls, i)):
                split = (i.split("."))
                file.append(split[0])
                file_extension = split[1]
        if file != []:
            self.input_file_name = file[0]
            self.input_file_sheet_name = 'rerate_input'
            self.input_contract_sheet_name = 'contract_input'
            self.insert_file_name = self.input_file_name+self.date_time
            try:
                server_name = 'database\SQL2015'
                database = 'LTLRating'
                connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                            f'Server={server_name};'
                                            f'Database={database};'
                                            f'Trusted_Connection=yes;')
                
                data_access = DataAccessLayer()

                mac_address = hex(uuid.getnode())
                ip_address = ip.get()
                ip_start_time = datetime.now()
                is_ip_exists_response = await data_access.insert_ip_address(
                    connection, "I", mac_address, ip_address, self.hostname)
                ip_end_time = datetime.now() - ip_start_time
                print("Log 1 Mac address and IP address : ", str(ip_end_time).split(".")[0] + f".{ip_end_time.microseconds:03}")
                print("Mac Address Response : ", is_ip_exists_response)
                if is_ip_exists_response == "Exists":
                    writer = pd.ExcelWriter(r'{0}\{1}.xlsx'.format(
                        pathlib.Path().resolve(), output_file_name))
                    Input_data = pd.read_excel(r'{0}\{1}.{2}'.format(
                        str(self.current_path)+"\\"+"input"+"\\", self.input_file_name, file_extension), sheet_name=self.input_file_sheet_name)
                    Input_data = Input_data.assign(sCity=pd.Series(dtype='object'), sState=pd.Series(dtype='object'), sCountry=pd.Series(dtype='object'),
                                                   cCity=pd.Series(dtype='object'), cState=pd.Series(dtype='object'), cCountry=pd.Series(dtype='object'))
                    
                    Input_contract_data = pd.read_excel(r'{0}\{1}.{2}'.format(
                        str(self.current_path)+"\\"+"input"+"\\", self.input_file_name, file_extension), sheet_name=self.input_contract_sheet_name)
                    no_ref_list = []
                    strQBClientCode = ""
                    qbclient_start_time = datetime.now()
                    sql_qbclient_df = await data_access.get_qb_client_code(
                        connection, "Q", self.hostname)
                    qbclient_end_time = datetime.now() - qbclient_start_time
                    print("Log 2 QBClientCode : ", str(qbclient_end_time).split(".")[0] + f".{qbclient_end_time.microseconds:03}")
                    if sql_qbclient_df.empty:
                        strQBClientCode = "DG"
                    else:
                        strQBClientCode = sql_qbclient_df['QBClientCode'][0]
                    
                    contract_id = 0
                    client_id = -1
                           
                    if ((('ClientID' in Input_contract_data.columns and Input_contract_data['ClientID'].replace(r'^\s*$', np.nan, regex=True).isna().all()) and (
                                'ContractID' in Input_contract_data.columns and Input_contract_data['ContractID'].replace(r'^\s*$', np.nan, regex=True).isna().all())) or ('ClientID' not in Input_contract_data.columns and 'ContractID' not in Input_contract_data.columns)):
                        print(
                            "Either ClientID or ContractID should be filled to proceed with rerate")
                        writer.save()
                        writer.close()
                        try:
                            writer.handles = None
                        except Exception as error:
                            pass 
                        output_path = os.path.join(
                            self.directory + output_file_name + ".xlsx")
                        os.remove(output_path)
                        return

                    elif (('ClientID' in Input_contract_data.columns and pd.isnull(Input_contract_data["ClientID"].values) == False) and ('ContractID' in Input_contract_data.columns and pd.isnull(Input_contract_data["ContractID"].values) == False)) or\
                            (('ContractID' in Input_contract_data.columns and pd.isnull(Input_contract_data["ContractID"].values) == False) and ('ClientID' not in Input_contract_data.columns or pd.isnull(Input_contract_data["ClientID"].values) == True)):

                        client_id = -1
                        contract_id = Input_contract_data["ContractID"].iloc[0]

                    elif ('ClientID' in Input_contract_data.columns and pd.isnull(Input_contract_data["ClientID"].values) == False) and ('ContractID' not in Input_contract_data.columns or pd.isnull(Input_contract_data["ContractID"].values) == True):

                        contract_id = 0
                        client_id = Input_contract_data["ClientID"].iloc[0]
                    Input_data['sZip'] = Input_data['sZip'].apply(str)
                    Input_data['cZip'] = Input_data['cZip'].apply(str)
                    Input_data['sZip'] = Input_data['sZip'].apply(lambda x: x.zfill(5))
                    Input_data['cZip'] = Input_data['cZip'].apply(lambda x: x.zfill(5))
                    for index,row in Input_data.iterrows(): 
                        loc_start_time = datetime.now()
                        loc_df = await data_access.get_zip_cache_details(connection, "Z", row['sZip'], row['cZip'], self.hostname)
                        
                        loc_end_time = datetime.now() - loc_start_time
                        print("Log 3 City,State,Country : ", str(loc_end_time).split(".")[0] + f".{loc_end_time.microseconds:03}")
                        Input_data.loc[index, 'sCity']=loc_df['City'][0]
                        Input_data.loc[index, 'sState']=loc_df['State'][0]
                        Input_data.loc[index, 'sCountry']=loc_df['Country'][0]
                        Input_data.loc[index, 'cCity']=loc_df['City'][1]
                        Input_data.loc[index, 'cState']=loc_df['State'][1]
                        Input_data.loc[index, 'cCountry']=loc_df['Country'][1]
                        
                    args = [(row, Input_data,strQBClientCode,no_ref_list,contract_id,client_id) for index,row in Input_data.iterrows()]
                    with Pool(processes=cpu_count()) as pool:
                        results = pool.starmap_async(dill.loads(dill.dumps(self.frame_request)), args) 
                        results.get()
                        # await self.frame_request(Input_data,data_access,connection,strQBClientCode,rerate_det,no_ref_list,contract_id,client_id)
                    print(self.rerate_det.ref_id_array)
                    self.rerate_det.rerate_data.ReferenceID = self.rerate_det.ref_id_array
                    self.rerate_det.rerate_data.sZip = self.rerate_det.szip_array
                    self.rerate_det.rerate_data.cZip = self.rerate_det.czip_array
                    self.rerate_det.rerate_data.Shipdate = self.rerate_det.shipdate_array
                    self.rerate_det.rerate_data.Weight = self.rerate_det.weight_array
                    self.rerate_det.rerate_data.Class = self.rerate_det.class_array
                    self.rerate_det.rerate_data.Pallet = self.rerate_det.pallet_array
                    self.rerate_det.rerate_data.FileName = self.rerate_det.filename_array
                    self.rerate_det.rerate_data.RequestXML = self.rerate_det.request_xml_array
                    self.rerate_det.rerate_data.ResponseXML = self.rerate_det.response_xml_array
                    self.rerate_det.rerate_data.Status = self.rerate_det.status_array
                    
                    
                    rerate_df = pd.DataFrame(self.rerate_det.rerate_data)
                    print(rerate_df)
                    insert_rerate_start_time = datetime.now()
                    insert_rate_results_response = await data_access.insert_rerate_results(connection, "IR", self.hostname,
                                                                                    rerate_df.values.tolist())
                    insert_rerate_end_time = datetime.now() - insert_rerate_start_time
                    print("Log 5 Insert rerate response : ", str(insert_rerate_end_time).split(".")[0] + f".{insert_rerate_end_time.microseconds:03}")
                    # print("ReRate Insert Response : ",
                    #       insert_rate_results_response)
                    description_Amt = 0
                    referenceID_list, sZip_list, cZip_list, Date_list, Weight_list, Class_list, Pallet_list = [
                    ], [], [], [], [], [], []
                    Freight_list, FAK_list, Fuel_list, Minimum_Adjustment_list, RAD_list, PSS_list, HAZ_list, LEN_list, REW_List = [
                    ], [], [], [], [], [], [], [], []
                    Discount_list, IDL_list, APO_list, HLS_list, BOR_list, CAL_list, HCR_list, DEL_list, LFT_list, REP_list, NFY_list = [
                    ], [], [], [], [], [], [], [], [], [], []
                    Contractname_list, finalTotal_list = [], []
                    contract_Name, Final_total = "", 0
                    rerate_results_start_time = datetime.now()
                    rating_df = await data_access.get_rerate_results(
                        connection, "SR", self.hostname, self.insert_file_name)
                    rerate_results_end_time = datetime.now() - rerate_results_start_time
                    print("Log 6 Get rerate results : ", str(rerate_results_end_time).split(".")[0] + f".{rerate_results_end_time.microseconds:03}")
                    table_df = pd.DataFrame()
                    Rerate_df = pd.DataFrame()
                    for index, row in rating_df.iterrows():
                        count = 0
                        ref_ID = row['ReferenceID']
                        sZip = row['sZip']
                        cZip = row['cZip']
                        Shipdate = row['Shipdate']
                        Weight = row['Weight']
                        pallet = row['Pallet']
                        Rate_dict = {'Freight': [], 'FAK': [], 'Fuel Surcharge': [], 'Minimum Adjustment': [], 'RAD': [], 'DEL': [],
                                    'HCR': [],
                                    'Discount': [],
                                    'CAL': [], 'BOR': [], 'PSS': [], 'LEN': [], 'NFY': [], 'LFT': [], 'REP': [], 'IDL': [],
                                    'APO': [], 'HLS': [],
                                    'HAZ': [], 'REW': []}
                        chargeset_Desc = []
                        xml_Response = row["ResponseXML"]
                        xml_Tree = ET.ElementTree(ET.fromstring(xml_Response))
                        freight_count = 0
                        root = xml_Tree.getroot()
                        for elt in root.iter():
                            if elt.tag == 'Description':
                                if 'Freight' in elt.text and freight_count == 0:
                                    description_text = 'Freight'
                                    freight_count = 1
                                elif 'Freight' in elt.text and freight_count != 0:
                                    description_text = 'FAK'
                                else:
                                    description_text = elt.text
                                chargeset_Desc.append(description_text)
                            elif elt.tag == 'Amount':
                                description_Amt = elt.text
                                if description_text in Rate_dict.keys():
                                    Rate_dict[description_text].append(
                                        elt.text)
                                else:
                                    Rate_dict[description_text] = [elt.text]
                            elif elt.tag == 'ContractName':
                                contract_Name = elt.text

                            elif elt.tag == 'Total':
                                Final_total = elt.text
                            elif elt.tag == 'PriceSheetID':
                                referenceID_list.append(row['ReferenceID'])
                                sZip_list.append(row['sZip'])
                                cZip_list.append(row['cZip'])
                                Date_list.append(row['Shipdate'])
                                Weight_list.append(row['Weight'])
                                Class_list.append(row['Class'])
                                Pallet_list.append(row['Pallet'])
                            elif elt.tag == 'MaxDeficitWeight':
                                count += 1
                                Contractname_list.append(contract_Name.strip())

                                finalTotal_list.append(Final_total)
                                for key in list(Rate_dict.keys()):
                                    if key not in chargeset_Desc:
                                        Rate_dict[key].append(0)
                                chargeset_Desc = []
                            elif elt.tag == 'StatusMessage' and elt.text != 'Rating Process completed with no errors':
                                count += 1
                                referenceID_list.append(row['ReferenceID'])
                                sZip_list.append(row['sZip'])
                                cZip_list.append(row['cZip'])
                                Date_list.append(row['Shipdate'])
                                Weight_list.append(row['Weight'])
                                Class_list.append(row['Class'])
                                Pallet_list.append(row['Pallet'])
                                contract_start_time = datetime.now()
                                contract_name_df = data_access.get_contract_name(
                                    connection, "EC", self.hostname, self.insert_file_name, row['ReferenceID'])
                                contract_end_time = datetime.now() - contract_start_time
                                print("Log 7 Get contract name : ", str(contract_end_time).split(".")[0] + f".{contract_end_time.microseconds:03}")
                                if(len(contract_name_df["ContractName"]) == 1):
                                    contrct_name = contract_name_df["ContractName"][0].strip(
                                    )

                                else:
                                    contrct_name = " "
                                Contractname_list.append(contrct_name)

                                finalTotal_list.append(0.00)
                                for key in list(Rate_dict.keys()):
                                    Rate_dict[key].append(0.00)
                        Freight_list += Rate_dict['Freight'][:count]
                        FAK_list += Rate_dict['FAK'][:count]
                        Fuel_list += Rate_dict['Fuel Surcharge'][:count]
                        Minimum_Adjustment_list += Rate_dict['Minimum Adjustment'][:count]
                        Discount_list += Rate_dict['Discount'][:count]
                        REP_list += Rate_dict['REP'][:count]
                        DEL_list += Rate_dict['DEL'][:count]
                        BOR_list += Rate_dict['BOR'][:count]
                        RAD_list += Rate_dict['RAD'][:count]
                        HCR_list += Rate_dict['HCR'][:count]
                        APO_list += Rate_dict['APO'][:count]
                        HLS_list += Rate_dict['HLS'][:count]
                        PSS_list += Rate_dict['PSS'][:count]
                        CAL_list += Rate_dict['CAL'][:count]
                        IDL_list += Rate_dict['IDL'][:count]
                        HAZ_list += Rate_dict['HAZ'][:count]
                        NFY_list += Rate_dict['NFY'][:count]
                        REW_List += Rate_dict['REW'][:count]
                        LFT_list += Rate_dict['LFT'][:count]
                        LEN_list += Rate_dict['LEN'][:count]
                        # print("Completed extracting ", str(row['ReferenceID']))
                    table_df['ReferenceID'] = referenceID_list
                    table_df['Szip'] = sZip_list
                    table_df['Czip'] = cZip_list
                    table_df['ShipDate'] = Date_list
                    table_df['Weight'] = Weight_list
                    table_df['Class'] = Class_list
                    table_df['Pallet'] = Pallet_list
                    Rerate_df['ContractName'] = Contractname_list
                    Rerate_df['Total'] = [float(i) for i in finalTotal_list]

                    Rerate_df['ReferenceID'] = referenceID_list
                    Rerate_df['Freight'] = [float(i) for i in Freight_list]
                    Rerate_df['FAK'] = [float(i) for i in FAK_list]
                    Rerate_df['Fuel'] = [float(i) for i in Fuel_list]
                    Rerate_df['Discount'] = [float(i) for i in Discount_list]
                    Rerate_df['Minimum Adjustment'] = [
                        float(i) for i in Minimum_Adjustment_list]
                    Rerate_df['REP'] = [float(i) for i in REP_list]
                    Rerate_df['DEL'] = [float(i) for i in DEL_list]
                    Rerate_df['APO'] = [float(i) for i in APO_list]
                    Rerate_df['HLS'] = [float(i) for i in HLS_list]
                    Rerate_df['CAL'] = [float(i) for i in CAL_list]
                    Rerate_df['HCR'] = [float(i) for i in HCR_list]
                    Rerate_df['NFY'] = [float(i) for i in NFY_list]
                    Rerate_df['REW'] = [float(i) for i in REW_List]
                    Rerate_df['LFT'] = [float(i) for i in LFT_list]
                    Rerate_df['BOR'] = [float(i) for i in BOR_list]
                    Rerate_df['HAZ'] = [float(i) for i in HAZ_list]
                    Rerate_df['PSS'] = [float(i) for i in PSS_list]
                    Rerate_df['IDL'] = [float(i) for i in IDL_list]
                    Rerate_df['RAD'] = [float(i) for i in RAD_list]
                    Rerate_df['LEN'] = [float(i) for i in LEN_list]

                    Result_df = table_df.merge(
                        Rerate_df, left_on='ReferenceID', right_on='ReferenceID')
                    Result_df.reset_index(drop=True)
                    Result_df['Freight'] = Result_df['Freight'] + \
                        Result_df['FAK'] + Result_df['Discount']
                    Result_df.drop('FAK', axis=1, inplace=True)
                    Result_df.drop('Discount', axis=1, inplace=True)

                    if('ContractID' not in Input_data.columns and ('ContractID' in Input_contract_data.columns or 'ClientID' in Input_contract_data.columns)):
                        Result_df = Result_df.drop_duplicates()

                        df2 = pd.DataFrame(Result_df)
                        df2 = df2.reset_index().pivot(index=["ReferenceID", "Szip", "Czip", "ShipDate", "Weight", "Class", "Pallet"],
                                                    columns=["ContractName"],
                                                    values=["ContractName", "Total", "Freight", "Fuel",
                                                            "Minimum Adjustment", "REP", "DEL", "APO", "HLS", "CAL", "HCR",
                                                            "NFY", "REW", "LFT", "BOR", "HAZ", "PSS", "IDL", "RAD", "LEN"])
                        df2 = df2.droplevel(
                            ["ContractName"], axis=1)
                        df2.reset_index(inplace=True)
                        df2.columns = pd.io.parsers.base_parser.ParserBase(
                            {'usecols': None})._maybe_dedup_names(df2.columns)
                        cols = []
                        oldcols = list(Result_df.columns.values)
                        cols.extend(oldcols)
                        for num in range(1, len(pd.unique(Result_df["ContractName"]))):
                            for col in Result_df.columns:
                                if col not in ["ReferenceID", "Szip", "Czip", "ShipDate", "Weight", "Class", "Pallet"]:
                                    cols.append(col+'.'+str(num))
                        df2 = df2[cols]
                        Result_df = df2
                        Result_df.to_excel(writer, sheet_name='ReRate Output',
                                        index=False, float_format="%.2f")
                    else:
                        Result_df.to_excel(writer, sheet_name='ReRate Output',
                                        index=False, float_format="%.2f")
                    workbook = writer.book
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'fg_color': '#0070C0',
                        'color': 'white',
                        'border': 1})
                    border_format = workbook.add_format(
                        {'border': 1, 'align': 'left'})
                    worksheet = writer.sheets['ReRate Output']
                    currency_format = workbook.add_format({'border': 1,
                                                        'num_format': u'_($* #,##0.00_);_($* (#,##0.00);_($* -_0_0_);_(@'})
                    pref_list = ["Total", "Freight", "Fuel",
                                "Minimum Adjustment", "REP", "DEL", "APO", "HLS", "CAL", "HCR",
                                "NFY", "REW", "LFT", "BOR", "HAZ", "PSS", "IDL", "RAD", "LEN"]
                    for col_num, col_name in enumerate(Result_df.columns.values):
                        column_width = max(Result_df[col_name].astype(
                            str).map(len).max(), len(col_name))
                        col_idx = Result_df.columns.get_loc(col_name)
                        if(list(filter(col_name.startswith, pref_list)) != []):
                            worksheet.set_column(
                                col_idx, col_idx, column_width + 4, currency_format)
                        else:
                            worksheet.set_column(
                                col_idx, col_idx, column_width + 4, border_format)
                        worksheet.write(0, col_num, col_name, header_format)
                    worksheet.ignore_errors(
                        {'number_stored_as_text': 'A2:XFD1048576'})
                    worksheet.conditional_format('A'+str(Result_df.shape[0]+1)+':XFD1048576',
                                                {'type': 'no_blanks', 'format': workbook.add_format({'border': 1})})
                    worksheet.conditional_format('A'+str(Result_df.shape[0]+2)+':XFD'+str(Result_df.shape[0]+2),
                                                {'type': 'blanks', 'format': workbook.add_format({'top': 1})})
                    worksheet.conditional_format('A'+str(Result_df.shape[0]+3)+':XFD1048576',
                                                {'type': 'blanks', 'format': workbook.add_format({'border': None})})
                    worksheet.hide_gridlines(2)
                    worksheet.freeze_panes(1, 1)
                    worksheet.autofilter(0, 0, 0, len(Result_df.columns) - 1)

                    writer.save()
                    print(
                        "Succeeded in creating ReRate Output Sheet inside "+output_file_path+" ...")
                    writer.close()
                    try:
                        writer.handles = None
                    except Exception as error:
                        pass 
                    archivefolder_path = self.directory+'\\archive'
                    outboxfolder_path = self.directory+'\\output'
                    if not os.path.exists(archivefolder_path):
                        os.makedirs(archivefolder_path)
                    if not os.path.exists(outboxfolder_path):
                        os.makedirs(outboxfolder_path)

                    input_file = str(self.directory) + "\\" + "input" + "\\" + "\\" + self.input_file_name + "." + file_extension
                    output_file = self.directory+"\\"+output_file_name+".xlsx"
                    archive_file_path = archivefolder_path+"\\" + \
                        self.input_file_name+"."+file_extension
                    rename_archive_file_path = archivefolder_path+"\\" + \
                        self.insert_file_name+"."+file_extension
                    shutil.copy(input_file, archivefolder_path)
                    os.rename(archive_file_path, rename_archive_file_path)
                    shutil.copy(output_file, outboxfolder_path)
                    input_path = os.path.join(
                        str(self.directory)+"\\"+"input"+"\\"+ self.input_file_name + "."+file_extension)
                    os.remove(input_path)
                    output_path = os.path.join(
                        self.directory + output_file_name + ".xlsx")
                    os.remove(output_path)

                    for no_ref in no_ref_list:
                        print(no_ref)

                elif is_ip_exists_response == "Not Exists - Mac":
                    print("Your are not authorized to access ReRate")
                    print("Please contact support@totalogistix.com with your mac address : " +
                        mac_address + "  and host : ", self.hostname)
                elif is_ip_exists_response == "Not Exists - IP":
                    print("Your are not authorized to access ReRate")
                    print("Please contact support@totalogistix.com with your ip address : " +
                        ip_address + "  and host : ", self.hostname)

            except Exception as err:
                err_subject = "ReRate2.o Error :: Error occurred during ReRate process :: Host Name :: {0} Input File - {1}".format(
                    self.hostname, self.insert_file_name)
                err_body = ''.join(trace.format_exception(
                    None, err, err.__traceback__))

                writer.close()
                try:
                    writer.handles = None
                except Exception as error:
                    pass                
                error_path = self.directory + '\\error'
                if not os.path.exists(error_path):
                    os.makedirs(error_path)

                error_file_name = str(self.directory) + "\\" + "input" + "\\" + "\\" + \
                                  self.input_file_name + "." + file_extension
                error_file_path = error_path+"\\" + \
                    self.input_file_name+"."+file_extension
                rename_error_file_path = error_path+"\\" + \
                    self.insert_file_name+"."+file_extension

                shutil.copy(error_file_name, error_path)
                os.rename(error_file_path, rename_error_file_path)
                output_path = os.path.join(
                    self.directory + output_file_name + ".xlsx")
                input_path = os.path.join(
                    str(self.directory) + "\\" + "input" + "\\" + self.input_file_name + "." + file_extension)
                os.remove(output_path)
                os.remove(input_path)
                print(
                    "\n\nRerate resulted with an error. Please check your input and try again.\n\n")
                error_start_time = datetime.now()
                # await data_access.send_error_email(
                #     connection, 'E', err_subject, err_body)
                error_end_time = datetime.now() - error_start_time
                print("Log 8 Insert Error : ", str(error_end_time).split(".")[0] + f".{error_end_time.microseconds:03}")
                print(err_body)

        else:
            print("The file does not exist. Please check the file and try again.")

async def main():
    start_time = datetime.now()
    print("Re-rate process started...", datetime.now())
    re_rate = ReRate()
    task1 = asyncio.create_task(re_rate.rerate())
    await task1
    print("Re-rate process successfully completed...", datetime.now())
    end_time = datetime.now() - start_time
    print("Log 9 Time HH:MM:SS took to complete : ", str(end_time).split(".")[0] + f".{end_time.microseconds:03}")

if __name__ == '__main__':
     freeze_support()   
     asyncio.run(main())