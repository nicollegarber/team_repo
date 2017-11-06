#inspired by http://nadbordrozd.github.io/blog/2016/05/22/one-weird-trick-that-will-fix-your-pyspark-schemas/ and by Talha the creator
# coding=utf-8
import pyspark.sql.types as pst
from pyspark.sql import Row
import xml.etree.ElementTree as ET
import os
from json import JSONEncoder
from timeit import default_timer as timer
import progressbar
from collections import OrderedDict

from pyspark.sql.functions import *

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

#from confluent_kafka import Consumer, KafkaError

conf = SparkConf().setAppName("kfPocConsumer").setMaster("local")
conf.set('spark.executor.instances', '15')
# conf.set('configuration', 'value')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

#c = Consumer({'bootstrap.servers': 'pthdmaster1', 'group.id': 'newgroup', 'default.topic.config': {'auto.offset.reset': 'smallest'}})
#c.subscribe(['ilangaXmlMsg'])

df_mssql = sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://TSQLGIANT3;database=DDS;user=svc_hadoop;password=Hado0p").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "(SELECT top 10 * FROM Message_Inbox where Req_System_Cd = 'COMPASS' and Caller_User_Id = 'COMPASS_ADMIN') as message_inbox").load()
df_mssql.registerTempTable("msg_inbox")
#print(sqlContext.sql("select Msg_Content from msg_inbox").limit(1).collect())

df_lkp_title = sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://TSQLGIANT3;database=DSS_EKF;user=svc_hadoop;password=Hado0p").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "(select * from lkp_title) as lkp_title").load()
sqlContext.registerDataFrameAsTable(df_lkp_title, 'lkp_title')

df_lkp_marital_status = sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://TSQLGIANT3;database=DSS_EKF;user=svc_hadoop;password=Hado0p").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "(select * from lkp_marital_status) as lkp_marital_status").load()
sqlContext.registerDataFrameAsTable(df_lkp_marital_status, 'df_lkp_marital_status')

to_ignore = []

def infer_schema(rec):
    """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
    if isinstance(rec, dict):
        return pst.StructType([pst.StructField(key, infer_schema(value), True)
                              for key, value in sorted(rec.items())])
    elif isinstance(rec, list):
        if len(rec) == 0:
            raise ValueError("can't infer type of an empty list")
        elem_type = infer_schema(rec[0])
        for elem in rec:
            this_type = infer_schema(elem)
            if elem_type != this_type:
                raise ValueError("can't infer type of a list with inconsistent elem types")
        return pst.ArrayType(elem_type)
    else:
        return pst._infer_type(rec)

def _rowify(x, prototype):
    """creates a Row object conforming to a schema as specified by a dict"""
    def _equivalent_types(x, y):
        if type(x) in [str] and type(y) in [str]:
            return True
        return isinstance(x, type(y)) or isinstance(y, type(x))
    if x is None:
        return None
    elif isinstance(prototype, dict):
        if type(x) != dict:
            raise ValueError("expected dict, got %s instead" % type(x))
        rowified_dict = {}
        for key, val in x.items():
            if key not in prototype:
                raise ValueError("got unexpected field %s" % key)
            rowified_dict[key] = _rowify(val, prototype[key])
            for key in prototype:
                if key not in x:
                    raise ValueError(
                        "expected %s field but didn't find it" % key)
        return Row(**rowified_dict)
    elif isinstance(prototype, list):
        if type(x) != list:
            raise ValueError("expected list, got %s instead" % type(x))
        return [_rowify(e, prototype[0]) for e in x]
    else:
        if not _equivalent_types(x, prototype):
            raise ValueError("expected %s (%s), got %s instead (%s)" %
                             (type(prototype), str(prototype), type(x), str(x)))
        return x

def df_from_rdd(rdd, prototype, sql):
    """creates a dataframe out of an rdd of dicts, with schema inferred from a prototype record"""
    schema = infer_schema(prototype)
    row_rdd = rdd.map(lambda x: _rowify(x, prototype))
    return sql.createDataFrame(row_rdd, schema)

def extract_section(parent):
        child_dict = OrderedDict()
        for attrib, val in parent.attrib.items():
                child_dict[attrib] = val
        try:
                for child in parent:
                        if len(child) > 0:
                                section, section_dict = extract_section(child)
                        else:
                                section = child.tag.replace("-", "_")
                                section_dict = child.text.strip() if child.text is not None else ''
                        if len(child.attrib) > 0: # If child has attributes, add them as well
                                section_dict = {section: section_dict}
                                for attrib, val in child.attrib.items():
                                        section_dict[attrib] = val
                        if section in child_dict.keys():
                                if isinstance(child_dict[section], list):
                                        child_dict[section].append(section_dict)
                                else:
                                        child_dict[section] = [child_dict[section], section_dict]
                        else:
                                child_dict[section] = section_dict
        except:
                e = sys.exc_info()[0]
                print("Tag: {0}, error: {0}".format(child.tag, e))
        section = parent.tag.replace("-", "_")
        return section, child_dict

def main(xml_strings, out_prefix=''):
        encoder = JSONEncoder()
        out_file = os.path.join(out_prefix, '{0}kfMultiMessage'.format(out_prefix))
        failed_files = []
        xpath_dict = {"Id": "./Party/Person/Id",
                      "Action_Cd": "./Party/Person/Action_Cd",
                      "Source_Ref": "./Party/Person/Source_Reference/Source_Ref",
                      "Source_Ref_Type": "./Party/Person/Source_Reference/Source_Ref_Type",
                      "Source_Creation_Dt": "./Party/Person/Source_Creation_Dt",
                      "Bus_Eff_Start_Dt": "./Party/Person/Bus_Eff_Start_Dt",
                      "Bus_Eff_End_Dt": "./Party/Person/Bus_Eff_End_Dt",
                      "Rec_Status_Cd": "./Party/Person/Rec_Status_Cd",
                      "Action_By": "./Party/Person/Action_By",
                      "Action_By_Type": "./Party/Person/Action_By_Type",
                      "Type_Cd": "./Party/Person/Type_Cd",
                      "Surname": "./Party/Person/Surname",
                      "First_Nm": "./Party/Person/First_Nm",
                      "Second_Nm": "./Party/Person/Second_Nm",
                      "Third_Nm": "./Party/Person/Third_Nm",
                      "Fourth_Nm": "./Party/Person/Fourth_Nm",
                      "Fifth_Nm": "./Party/Person/Fifth_Nm",
                      "Title_Cd": "./Party/Person/Title_Cd",
                      "Initials": "./Party/Person/Initials",
                      "Maiden_Name": "./Party/Person/Maiden_Name",
                      "Known_As_Nm": "./Party/Person/Known_As_Nm",
                      "Gender_Cd": "./Party/Person/Gender_Cd",
                      "MaritalStatus_Cd": "./Party/Person/MaritalStatus/Maritalstatus_Cd",
                      "Marital_Bus_Eff_Start_Dt": "./Party/Person/MaritalStatus/Bus_Eff_Start_Dt",
                      "Marital_Bus_Eff_End_Dt": "./Party/Person/MaritalStatus/Bus_Eff_End_Dt",
                      "Preferred_Language_No": "./Party/Person/Preferred_Language_No",
                      "Birth_Dt": "./Party/Person/Birth_Dt",
                      "Birth_Cntry_Cd": "./Party/Person/Birth_Cntry_Cd",
                      "Registration_Type_Cd": "./Party/Person/Registration/Type_Cd",
                      "Registration_Reg_No": "./Party/Person/Registration/Reg_No",
                      "Registration_Reg_Cntry_Cd": "./Party/Person/Registration/Reg_Cntry_Cd",
                      "Registration_Reg_Issues_Dt": "./Party/Person/Registration/Reg_Issues_Dt",
                      "Registration_Reg_Exp_Dt": "./Party/Person/Registration/Reg_Exp_Dt",
                      "Dual_Citizenship_Ind": "./Party/Person/Dual_Citizenship_Ind",
                      "PDB_Id": "./Party/Person/PDB_Id",
                      "Occupation_Cd": "./Party/Person/Occupation_Cd",
                      "Orig_Occupation_Cd": "./Party/Person/Orig_Occupation_Cd",
                      "Occupation_Duties": "./Party/Person/Occupation_Duties",
                      "Smoker_Cd": "./Party/Person/Smoker_Cd",
                      "Qualification_Cd": "./Party/Person/Qualification_Cd",
                      "Annual_Income_Gross": "./Party/Person/Annual_Income_Gross",
                      "Insolvency_Ind": "./Party/Person/Insolvency_Ind",
                      "Rating_Cd": "./Party/Person/Rating_Cd",
                      "Occupation_Risk_Cat_Cd": "./Party/Person/Occupation_Risk_Cat_Cd",
                      "Death_Dt": "./Party/Person/Death_Dt",
                      "Mortality_Reason_Cd": "./Party/Person/Mortality_Reason_Cd",
                      "Health_Status_Cd": "./Party/Person/Health_Status_Cd",
                      "Health_Event_Dt": "./Party/Person/Health_Event_Dt",
                      "Irate_Ind": "./Party/Person/Irate_Ind",
                      "Race_Cd": "./Party/Person/Race_Cd",
                      "Religion_Cd": "./Party/Person/Religion_Cd",
                      "Suffix_Cd": "./Party/Person/Suffix_Cd",
                      "Immigration_Status_Cd": "./Party/Person/Immigration_Status_Cd",
                      "Immigration_Entry_Dt": "./Party/Person/Immigration_Entry_Dt",
                      "SA_Resident_Ind": "./Party/Person/SA_Resident_Ind",
                      "Retirement_Status": "./Party/Person/Retirement_Status",
                      "Retirement_Deferred_Ind": "./Party/Person/Retirement_Deferred_Ind",
                      "Retirement_Dt": "./Party/Person/Retirement_Dt",
                      "Origin_Cd": "./Party/Person/Origin_Cd",
                      "Special_Condition_Cd": "./Party/Person/Special_Condition_Cd",
                      "Customer_Segment_Cd": "./Party/Person/Customer_Segment_Cd",
                      "Occupation_Change_Dt": "./Party/Person/Occupation_Change_Dt",
                      "Income_Kind_Cd": "./Party/Person/Income_Kind_Cd",
                      "Income_Eff_Dt": "./Party/Person/Income_Eff_Dt",
                      "Phone_Id": "./Party/Person/ContactDet/Phone/Id",
                      "Phone_Action_Cd": "./Party/Person/ContactDet/Phone/Action_Cd",
                      "Phone_Source_Ref": "./Party/Person/ContactDet/Phone/Source_Reference/Source_Ref",
                      "Phone_Source_Ref_Type": "./Party/Person/ContactDet/Phone/Source_Reference/Source_Ref_Type",
                       "Address_Id": "./Party/Person/ContactDet/Address/Id",
                       "Address_Action_Cd": "./Party/Person/ContactDet/Address/Action_Cd",
                       "Address_Source_Ref": "./Party/Person/ContactDet/Address/Source_Reference/Source_Ref",
                       "Address_Source_Ref_Type": "./Party/Person/ContactDet/Address/Source_Reference/Address/Source_Ref_Type",
                       "Address_Source_Creation_Dt": "./Party/Person/ContactDet/Address/Source_Creation_Dt",
                       "Address_Bus_Eff_Start_Dt": "./Party/Person/ContactDet/Address/Bus_Eff_Start_Dt",
                       "Address_Bus_Eff_End_Dt": "./Party/Person/ContactDet/Address/Bus_Eff_End_Dt",
                       "Address_Rec_Status_Cd": "./Party/Person/ContactDet/Address/Rec_Status_Cd",
                       "Address_Actioned_By": "./Party/Person/ContactDet/Address/Actioned_By",
                       "Address_Actioned_By_Type": "./Party/Person/ContactDet/Address/Actioned_By_Type",
                       "Address_Type_Cd": "./Party/Person/ContactDet/Address/Type_Cd",
                       "Address_Usage_Cd": "./Party/Person/ContactDet/Address/Usage_Cd",
                       "Address_Facility_Cd": "./Party/Person/ContactDet/Address/Facility_Cd",
                       "Address_Care_Of": "./Party/Person/ContactDet/Address/Care_Of",
                       "Address_Addr_Line_1": "./Party/Person/ContactDet/Address/Addr_Line_1",
                       "Address_Addr_Line_2": "./Party/Person/ContactDet/Address/Addr_Line_2",
                       "Address_Addr_Line_3": "./Party/Person/ContactDet/Address/Addr_Line_3",
                       "Address_Addr_Line_4": "./Party/Person/ContactDet/Address/Addr_Line_4",
                       "Address_City": "./Party/Person/ContactDet/Address/City",
                       "Address_Region": "./Party/Person/ContactDet/Address/Region",
                       "Address_Cntry_Cd": "./Party/Person/ContactDet/Address/Cntry_Cd",
                       "Address_Postal_Cd": "./Party/Person/ContactDet/Address/Postal_Cd",
                       "Email_Id": "./Party/Person/ContactDet/Email/Id",
                       "Email_Action_Cd": "./Party/Person/ContactDet/Email/Action_Cd",
                       "Email_Source_Ref": "./Party/Person/ContactDet/Email/Source_Reference/Source_Ref",
                       "Email_Source_Ref_Type": "./Party/Person/ContactDet/Email/Source_Reference/Source_Ref_Type",
                       "Email_Source_Reference": "./Party/Person/ContactDet/Email//Source_Reference",
                       "Email_Source_Creation_Dt": "./Party/Person/ContactDet/Email/Source_Creation_Dt",
                       "Email_Bus_Eff_Start_Dt": "./Party/Person/ContactDet/Email/Bus_Eff_Start_Dt",
                       "Email_Bus_Eff_End_Dt": "./Party/Person/ContactDet/Email/Bus_Eff_End_Dt",
                       "Email_Rec_Status_Cd": "./Party/Person/ContactDet/Email/Rec_Status_Cd",
                       "Email_Actioned_By": "./Party/Person/ContactDet/Email/Actioned_By",
                       "Email_Actioned_By_Type": "./Party/Person/ContactDet/Email/Actioned_By_Type",
                       "Email_Type_Cd": "./Party/Person/ContactDet/Email/Type_Cd",
                       "Email_Usage_Cd": "./Party/Person/ContactDet/Email/Usage_Cd",
                       "Email_Email_Addr": "./Party/Person/ContactDet/Email/Email_Addr",
                       "Phone_Id": "./Party/Person/ContactDet/Phone/Id",
                       "Phone_Action_Cd": "./Party/Person/ContactDet/Phone/Action_Cd",
                       "Phone_Source_Ref": "./Party/Person/ContactDet/Phone/Source_Reference/Source_Ref",
                       "Phone_Source_Ref_Type": "./Party/Person/ContactDet/Phone/Source_Reference/Source_Ref_Type",
                       "Phone_Source_Creation_Dt": "./Party/Person/ContactDet/Phone/Source_Creation_Dt",
                       "Phone_Bus_Eff_Start_Dt": "./Party/Person/ContactDet/Phone/Bus_Eff_Start_Dt",
                       "Phone_Bus_Eff_End_Dt": "./Party/Person/ContactDet/Phone/Bus_Eff_End_Dt",
                       "Phone_Rec_Status_Cd": "./Party/Person/ContactDet/Phone/Rec_Status_Cd",
                       "Phone_Actioned_By": "./Party/Person/ContactDet/Phone/Actioned_By",
                       "Phone_Actioned_By_Type": "./Party/Person/ContactDet/Phone/Actioned_By_Type",
                       "Phone_Type_Cd": "./Party/Person/ContactDet/Phone/Type_Cd",
                       "Phone_Facility_Cd": "./Party/Person/ContactDet/Phone/Facility_Cd",
                       "Phone_Usage_Cd": "./Party/Person/ContactDet/Phone/Usage_Cd",
                       "Phone_Tel_Cntry_CD": "./Party/Person/ContactDet/Phone/Tel_Cntry_CD",
                       "Phone_Tel_Area_CD": "./Party/Person/ContactDet/Phone/Tel_Area_CD",
                       "Phone_Tel_No": "./Party/Person/ContactDet/Phone/Tel_No",
                       "Account_Id": "./Party/Person/Account/Id",
                       "Account_Action_Cd": "./Party/Person/Account/Action_Cd",
                       "Account_Source_Ref": "./Party/Person/Account/Source_Reference/Source_Ref",
                       "Account_Source_Ref_Type": "./Party/Person/Account/Source_Reference/Source_Ref_Type",
                       "Account_Source_Creation_Dt": "./Party/Person/Account/Source_Creation_Dt",
                       "Account_Bus_Eff_Start_Dt": "./Party/Person/Account/Bus_Eff_Start_Dt",
                       "Account_Bus_Eff_End_Dt": "./Party/Person/Account/Bus_Eff_End_Dt",
                       "Account_Rec_Status_Cd": "./Party/Person/Account/Rec_Status_Cd",
                       "Account_Actioned_By": "./Party/Person/Account/Actioned_By",
                       "Account_Actioned_By_Type": "./Party/Person/Account/Actioned_By_Type",
                       "Account_Type_Cd": "./Party/Person/Account/Type_Cd",
                       "Account_Acc_No": "./Party/Person/Account/Acc_No",
                       "Account_Bank_Cd": "./Party/Person/Account/Bank_Cd",
                       "Account_Bank_Brn_Cd": "./Party/Person/Account/Bank_Brn_Cd",
                       "Account_Acc_Holder_Nm": "./Party/Person/Account/Acc_Holder_Nm",
                       "Account_Acc_Kind_Cd": "./Party/Person/Account/Acc_Kind_Cd",
                       "Account_Fin_Purpose_Cd": "./Party/Person/Account/Fin_Purpose_Cd",
                       "Account_Verified_Status_Cd": "./Party/Person/Account/Verified_Status_Cd",
                       "Account_Verified_Status_Dt": "./Party/Person/Account/Verified_Status_Dt",
                       "Account_Acc_Status_Cd": "./Party/Person/Account/Acc_Status_Cd",
                       "Account_Open_Dt": "./Party/Person/Account/Open_Dt",
                       "Account_Closed_Dt": "./Party/Person/Account/Closed_Dt",
                       "Compliance_Id": "./Party/Person/Compliance/Id",
                       "Compliance_Source_Creation_Dt": "./Party/Person/Compliance/Source_Creation_Dt",
                       "Compliance_Bus_Eff_From_Dt": "./Party/Person/Compliance/Bus_Eff_From_Dt",
                       "Compliance_Bus_Eff_To_Dt": "./Party/Person/Compliance/Bus_Eff_To_Dt",
                       "Compliance_Rec_Status_Cd": "./Party/Person/Compliance/Rec_Status_Cd",
                       "Compliance_Actioned_By": "./Party/Person/Compliance/Actioned_By",
                       "Compliance_Actioned_By_Type": "./Party/Person/Compliance/Actioned_By_Type",
                       "Compliance_Type_Cd": "./Party/Person/Compliance/Type_Cd",
                       "Compliance_Company_Cd": "./Party/Person/Compliance/Company_Cd",
                       "Compliance_BusinessUnit_Cd": "./Party/Person/Compliance/BusinessUnit_Cd",
                       "Compliance_Application_Cd": "./Party/Person/Compliance/Application_Cd",
                       "Compliance_Function_Cd": "./Party/Person/Compliance/Function_Cd",
                       "Compliance_Event_Result_Ind": "./Party/Person/Compliance/Event_Result_Ind",
                       "Compliance_Role_Cd": "./Party/Person/Compliance/Applicable_Roles/Role_Cd",
                       "Compliance_Full_Compliance_Info_Request_Ind": "./Party/Person/Compliance/Full_Compliance_Info_Request_Ind",
                       "Compliance_Declaration_Text": "./Party/Person/Compliance/Declaration_Text",
                       "Compliance_Declaration_Applicable_Ind": "./Party/Person/Compliance/Declaration_Applicable_Ind",
                       "Compliance_Show_Previous_Response_Ind": "./Party/Person/Compliance/Show_Previous_Response_Ind",
                       "Compliance_Request_Mandatory_Ind": "./Party/Person/Compliance/Request_Mandatory_Ind",
                       "Compliance_Response_Mandatory_Ind": "./Party/Person/Compliance/Response_Mandatory_Ind",
                       "Compliance_Request_Id": "./Party/Person/Compliance/Request_Response/Request_Id",
                       "Compliance_Request_Type": "./Party/Person/Compliance/Request_Response/Request_Type",
                       "Compliance_Request_Text": "./Party/Person/Compliance/Request_Response/Request_Text",
                       "Compliance_Response_Type": "./Party/Person/Compliance/Request_Response/Response_Type",
                       "Compliance_Response_Text": "./Party/Person/Compliance/Request_Response/Response_Text",
                       "Compliance_Response_Result_Ind": "./Party/Person/Compliance/Request_Response/Response_Result_Ind",
                       "AgmtRole_Source_Ref" : "./Party/Person/AgmtRole/Source_Reference/Source_Ref",
                       "AgmtRole_Source_Ref_Type" : "./Party/Person/AgmtRole/Source_Reference/Source_Ref_Type"}
        bar = progressbar.ProgressBar()
        lists = []
        for f in bar(xml_strings):
                try:
                        row_obj = OrderedDict()
                        # tree = ET.parse(f)
                        # root = tree.getroot()
                        root = ET.fromstring(f)
                        root = root[0]

                        val_dict = {"Id": None,
                                    "Action_Cd": None,
                                    "Source_Ref": None,
                                    "Source_Ref_Type": None,
                                    "Source_Creation_Dt": None,
                                    "Bus_Eff_Start_Dt": None,
                                    "Bus_Eff_End_Dt": None,
                                    "Rec_Status_Cd": None,
                                    "Action_By": None,
                                    "Action_By_Type": None,
                                    "Type_Cd": None,
                                    "Surname": None,
                                    "First_Nm": None,
                                    "Second_Nm": None,
                                    "Third_Nm": None,
                                    "Fourth_Nm": None,
                                    "Fifth_Nm": None,
                                    "Title_Cd": None,
                                    "Initials": None,
                                    "Maiden_Name": None,
                                    "Known_As_Nm": None,
                                    "Gender_Cd": None,
                                    "MaritalStatus_Cd": None,
                                    "Marital_Bus_Eff_Start_Dt": None,
                                    "Marital_Bus_Eff_End_Dt": None,
                                    "Preferred_Language_No": None,
                                    "Birth_Dt": None,
                                    "Birth_Cntry_Cd": None,
                                    "Registration_Type_Cd": None,
                                    "Registration_Reg_No": None,
                                    "Registration_Reg_Cntry_Cd": None,
                                   "Registration_Reg_Issues_Dt": None,
                                    "Registration_Reg_Exp_Dt": None,
                                    "Dual_Citizenship_Ind": None,
                                    "PDB_Id": None,
                                    "Occupation_Cd": None,
                                    "Orig_Occupation_Cd": None,
                                    "Occupation_Duties": None,
                                    "Smoker_Cd": None,
                                    "Qualification_Cd": None,
                                    "Annual_Income_Gross": None,
                                    "Insolvency_Ind": None,
                                    "Rating_Cd": None,
                                    "Occupation_Risk_Cat_Cd": None,
                                    "Death_Dt": None,
                                    "Mortality_Reason_Cd": None,
                                    "Health_Status_Cd": None,
                                    "Health_Event_Dt": None,
                                    "Irate_Ind": None,
                                    "Race_Cd": None,
                                    "Religion_Cd": None,
                                    "Suffix_Cd": None,
                                    "Immigration_Status_Cd": None,
                                    "Immigration_Entry_Dt": None,
                                    "SA_Resident_Ind": None,
                                    "Retirement_Status": None,
                                    "Retirement_Deferred_Ind": None,
                                    "Retirement_Dt": None,
                                    "Origin_Cd": None,
                                    "Special_Condition_Cd": None,
                                    "Customer_Segment_Cd": None,
                                    "Occupation_Change_Dt": None,
                                    "Income_Kind_Cd": None,
                                    "Income_Eff_Dt": None,
                                    "Address_Id": None,
                                    "Address_Action_Cd": None,
                                    "Address_Source_Ref": None,
                                    "Address_Source_Ref_Type": None,
                                    "Address_Source_Creation_Dt": None,
                                    "Address_Bus_Eff_Start_Dt": None,
                                    "Address_Bus_Eff_End_Dt": None,
                                    "Address_Rec_Status_Cd": None,
                                    "Address_Actioned_By": None,
                                    "Address_Actioned_By_Type": None,
                                    "Address_Type_Cd": None,
                                    "Address_Usage_Cd": None,
                                    "Address_Facility_Cd": None,
                                    "Address_Care_Of": None,
                                    "Address_Addr_Line_1": None,
                                    "Address_Addr_Line_2": None,
                                    "Address_Addr_Line_3": None,
                                    "Address_Addr_Line_4": None,
                                    "Address_City": None,
                                    "Address_Region": None,
                                    "Address_Cntry_Cd": None,
                                    "Address_Postal_Cd": None,
                                    "Email_Id": None,
                                    "Email_Action_Cd": None,
                                    "Email_Source_Ref": None,
                                    "Email_Source_Ref_Type": None,
                                    "Email_Source_Reference": None,
                                    "Email_Source_Creation_Dt": None,
                                    "Email_Bus_Eff_Start_Dt": None,
                                    "Email_Bus_Eff_End_Dt": None,
                                    "Email_Rec_Status_Cd": None,
                                    "Email_Actioned_By": None,
                                    "Email_Actioned_By_Type": None,
                                    "Email_Type_Cd": None,
                                    "Email_Usage_Cd": None,
                                    "Email_Email_Addr": None,
                                    "Phone_Id": None,
                                    "Phone_Action_Cd": None,
                                    "Phone_Source_Ref": None,
                                    "Phone_Source_Ref_Type": None,
                                    "Phone_Source_Creation_Dt": None,
                                    "Phone_Bus_Eff_Start_Dt": None,
                                    "Phone_Bus_Eff_End_Dt": None,
                                    "Phone_Rec_Status_Cd": None,
                                    "Phone_Actioned_By": None,
                                    "Phone_Actioned_By_Type": None,
                                    "Phone_Type_Cd": None,
                                    "Phone_Facility_Cd": None,
                                    "Phone_Usage_Cd": None,
                                    "Phone_Tel_Cntry_CD": None,
                                    "Phone_Tel_Area_CD": None,
                                    "Phone_Tel_No": None,
                                    "Account_Id": None,
                                    "Account_Action_Cd": None,
                                    "Account_Source_Ref": None,
                                    "Account_Source_Ref_Type": None,
                                    "Account_Source_Creation_Dt": None,
                                    "Account_Bus_Eff_Start_Dt": None,
                                    "Account_Bus_Eff_End_Dt": None,
                                    "Account_Rec_Status_Cd": None,
                                    "Account_Actioned_By": None,
                                    "Account_Actioned_By_Type": None,
                                    "Account_Type_Cd": None,
                                    "Account_Acc_No": None,
                                    "Account_Bank_Cd": None,
                                    "Account_Bank_Brn_Cd": None,
                                    "Account_Acc_Holder_Nm": None,
                                    "Account_Acc_Kind_Cd": None,
                                    "Account_Fin_Purpose_Cd": None,
                                    "Account_Verified_Status_Cd": None,
                                    "Account_Verified_Status_Dt": None,
                                    "Account_Acc_Status_Cd": None,
                                    "Account_Open_Dt": None,
                                    "Account_Closed_Dt": None,
                                    "Compliance_Id": None,
                                    "Compliance_Source_Creation_Dt": None,
                                    "Compliance_Bus_Eff_From_Dt": None,
                                    "Compliance_Bus_Eff_To_Dt": None,
                                    "Compliance_Rec_Status_Cd": None,
                                    "Compliance_Actioned_By": None,
                                    "Compliance_Actioned_By_Type": None,
                                    "Compliance_Type_Cd": None,
                                    "Compliance_Company_Cd": None,
                                    "Compliance_BusinessUnit_Cd": None,
                                    "Compliance_Application_Cd": None,
                                    "Compliance_Function_Cd": None,
                                    "Compliance_Event_Result_Ind": None,
                                    "Compliance_Role_Cd": None,
                                    "Compliance_Full_Compliance_Info_Request_Ind": None,
                                    "Compliance_Declaration_Text": None,
                                    "Compliance_Declaration_Applicable_Ind": None,
                                    "Compliance_Show_Previous_Response_Ind": None,
                                    "Compliance_Request_Mandatory_Ind": None,
                                    "Compliance_Response_Mandatory_Ind": None,
                                    "Compliance_Request_Id": None,
                                    "Compliance_Request_Type": None,
                                    "Compliance_Request_Text": None,
                                    "Compliance_Response_Type": None,
                                   "Compliance_Response_Text": None,
                                    "Compliance_Response_Result_Ind": None,
                                    "AgmtRole_Source_Ref" : None,
                                    "AgmtRole_Source_Ref_Type" : None}
                        # print(root.findall("./Party/Person/ContactDet/Phone/*"))
                        for key, val in val_dict.items():
                            val_dict[key]=''
                        for key, val in xpath_dict.items():
                            item = root.find(val)
                            val_dict[key]= item.text if (item is not None and item.text is not None) else ''
                        lists.append(val_dict)
                except KeyboardInterrupt:
                        raise
                except Exception as e:
                        print("\n error: {0}".format(e))
                        return 0
        with open('{0}.failed'.format(out_file), 'wt') as failed_file:
                for f in failed_files:
                        failed_file.write(f)
        print("{0} files failed to process".format(len(failed_files)))
        prototype = val_dict
        rdd_list = sc.parallelize(lists)
        print(rdd_list)
        df = df_from_rdd(rdd_list, prototype, sqlContext)

        print(type(rdd_list))
        print(type(prototype))
        print(type(sqlContext))
        df.printSchema()
        #sqlContext.sql("show tables").show()
        df1 = df.where(length(df.Surname) > 0).join(df_lkp_title, df.Title_Cd.cast('int').alias('Title_Code') == df_lkp_title.Title_Cd, 'inner').join(df_lkp_marital_status, df.MaritalStatus_Cd == df_lkp_marital_status.Marital_Status_Cd, 'left').select(df.Birth_Dt, df.First_Nm, df.Second_Nm, df.Third_Nm, df.Fourth_Nm, df.Fifth_Nm, df.Surname, df.Maiden_Name, df.Known_As_Nm, df.Initials, df.Id, df.SA_Resident_Ind, df.Immigration_Entry_Dt, df.Dual_Citizenship_Ind, df.Insolvency_Ind, df.Retirement_Status, df.Retirement_Deferred_Ind, df.Retirement_Dt, df.Death_Dt, df.PDB_Id, df.Source_Creation_Dt, df.Bus_Eff_Start_Dt, df.Bus_Eff_End_Dt)
#print("This is the count of the amount of records that will be processed: " + str(df1.count()))

if __name__ == '__main__':
        start = timer()
        print("Processing")
        #confluent
        xml_list = sqlContext.sql("select Msg_Content from msg_inbox").collect()
        xml = [a.Msg_Content for a in xml_list]
        print(type(xml))
        main(xml)
        end = timer()
        print("{0} rows took {0} seconds to process".format(len(xml), end-start))
