import os
import fdb # imports the Firebird connector python module in your program so you can use the functions of this module to communicate with the Firebird database. you need to pip install fdb.
import requests
import json
import unittest
import sys

# variables you need to pass in from command line when test different Firbird DB conversions
# fireBirdDBName = 'COBALT_ENGINEERING_OFFICE' # copy the firebird DB to your folder, outside the "Scripts" folder
def query_fireBirdDB(fireBirdDBName, relationTypeCount, nodeIdList):

    # if you encounter an error "The location of Firebird Client Library could not be determined", then google "firebird client" to install will fix it.
    dir_name = '../'
    suffix = '.GDB'
    databasePath = os.path.join(dir_name, fireBirdDBName + suffix) # ../XYZ.GDB
    fb = fdb.connect(database = databasePath, user='sysdba', password='masterkey')

    schema = fdb.schema.Schema()
    schemaBindFb = schema.bind(fb)
    cursor = fb.cursor()

    # Use "BIM Objects.xlsx" from the URL below (passwrod protected) to find which table in Firebird DB contains the relations
    # https://osramencelium.atlassian.net/wiki/spaces/EVOUI/pages/151748778/PostgreSQL+db
    # Remote desk in any SSU listed in the URL below (password protected) and use the "DB Editor" software to have a visualized view of the table columns
    # https://osramencelium.atlassian.net/wiki/spaces/AR/pages/9215005/Deployment
    # Use ObjectType file from the URL below (password protected) to understand which data is for which node type
    # http://bitbucket:7990/projects/ECU/repos/ecu_properties/raw/generatedcode/polaris/ecs_Type_ECUProperties.pas?at=refs%2Fheads%2Fdev
    for table in schema.tables:
        assert isinstance(table, fdb.schema.Table)
        #print table.name, [c.name for c in table.columns] #something like TBL_ADDRESS_OFFSET ['FREE_OFFSET', 'ISCOUNT']

        #  to fetch data use cursor.execute() to run a query.
        sql_select_Query  = "s" \
                            "elect * from {0}".format(table.name)
        cursor.execute(sql_select_Query)
        data = cursor.fetchall() # cursor.fetchall() to fetch all rows
        #print(type(data)) # <type 'list'>

        # Building CONTAINS Floor
        if table.name == 'TBL_BUILDING_ZONE':
            for row in data:
                if row[4] == 246:
                    nodeIdList['BuildingIdList'].append(row[0].lower())
                elif row[4] == 245: # can not use TBL_PLAN, otherwise the id you get is floor plan's id instead of floor's id
                    nodeIdList['FloorIdList'].append(row[0].lower())
        # for any Firebird DB with NO Building a new Building node will be inserted to the site when it is uploaded to the BIM
        if len(nodeIdList['BuildingIdList']) == 0 and len(nodeIdList['FloorIdList']) > 0 :
            relationTypeCount['BuildingCONTAINSFloorCount'] = len(nodeIdList['FloorIdList'])

        # OrganizationalAreaTemplate TEMPLATES OrganizationalArea
        if table.name == 'TBL_BUILDING_ZONE':
            for row in data:
                # objectType 254 is virtualZone, objectType 2 is Zone
                if row[4] == 254 or row[4] == 2:
                    relationTypeCount['OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCount'] += 1
                    nodeIdList['OrganizationalAreaIdList'].append(row[0].strip("{}").lower())
                    nodeIdList['OrganizationalAreaTemplateIdList'].append(row[21].strip("{}").lower())

    return relationTypeCount, nodeIdList

# variables you need to pass in from command line when test different Firbird DB conversions
# encSystemId = '9757a197-03e5-40a0-ad89-3d8350948f6a' # guid of encSystem node in PostgreSQL DB
def query_postgreSQLDB(encSystemId):
    # Firebird DB contains 64 tables, but PostgreSQL DB only contains 2 tables: node table and relation table
    # Instead of connect to PostgreSQL in the Azure Cloud, we use 2 API endpoints to return all the nodes and all the relations under EncSystem from PostgreSQL DB.
    http = 'https://'
    env = 'qa'
    urlConfig = '-bimapi.5ea7a1d2a99a4699b36b.eastus.aksapp.io/config'
    username = 'removed'
    password = 'removed'

    relations = requests.get(http + env + urlConfig + '/v1/relations?encSystemId=' + encSystemId + '&Pageindex=0&PageSize=999999', auth=(username, password))
    relationsStatusCode = relations.status_code
    assert relationsStatusCode == 200, "non 200 status code returned for {}{}{}/v1/relations?encSystemId={}&Pageindex=0&PageSize=999999".format(http, env, urlConfig, encSystem)
    relationsString = relations.content # all relations belong to the encSystem (i.e. a site)
    relationsList = relationsString.split('},') # convert the api return to a list that contains all relations, each relation is an element in the list.

    return relationsList

# compare relations from Firebird DB with API returns (API queries cloud PostgreSQL DB)
# implement non-fatal assertions (i.e., if failed in the middle next test will continue to run) by capturing the assertion exception and store the exceptions in a list
class relationCompare(unittest.TestCase):
    fireBirdDBName = '' # copy the firebird DB to your folder, outside the "Scripts" folder, will pass in from cmd.
    encSystemId = '' # guid of encSystem node in PostgreSQL DB, will pass in from cmd.

    def setUp(self):
        # The hierarchy and relations between nodes please refer to the architecture diagram (password protected):
        # https://osramencelium.atlassian.net/wiki/spaces/EVOUI/pages/108167181/Node+Types+Relationship+Types
        self.relationTypeCount = {
            'BuildingCONTAINSFloorCount': 0,
            'OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCount': 0
        }
        # need to define variables here so they can be accessed correctly from the API section below
        self.nodeIdList = {
            'BuildingIdList': [],
            'FloorIdList': [],
            'OrganizationalAreaIdList': [],
            'OrganizationalAreaTemplateIdList': []
        }
        self.relationTypeCountOutput, self.nodeIdListOutput = query_fireBirdDB(self.fireBirdDBName, self.relationTypeCount, self.nodeIdList)
        self.relationsListOutput = query_postgreSQLDB(self.encSystemId)
        self.verificationErrors = []

    def tearDown(self):
        self.assertEqual([], self.verificationErrors)

    def test_init(self):
        # Building CONTAINS Floor
        BuildingCONTAINSFloorList = []
        for relation in self.relationsListOutput:
            if "\"relationType\": \"Contains\"" and "\"inNodeType\": \"Floor\"" and "\"outNodeType\": \"Building\"" in relation:
                BuildingCONTAINSFloorList.append(relation)
        BuildingCONTAINSFloorCountAPI = len(BuildingCONTAINSFloorList)
        try: self.assertEqual(BuildingCONTAINSFloorCountAPI, self.relationTypeCountOutput['BuildingCONTAINSFloorCount'])
        except AssertionError: self.verificationErrors.append("Building CONTAINS Floor count is incorrect! Expected {0}, but API returned {1}".format(self.relationTypeCountOutput['BuildingCONTAINSFloorCount'], BuildingCONTAINSFloorCountAPI))

        for item in BuildingCONTAINSFloorList:
            keyValuePairList = item.split(',')
            for keyValuePair in keyValuePairList:
                if "inNodeId" in keyValuePair:
                    inNodeId = keyValuePair.split(':')[1].replace('"', '').lstrip(' ')
                    try: self.assertTrue(inNodeId in item for item in self.nodeIdListOutput['FloorIdList'])
                    except AssertionError: self.verificationErrors.append("inNodeId for Floor {0} does not exist in Firebird DB".format(inNodeId))
                if len(self.nodeIdListOutput['BuildingIdList']) != 0 and "outNodeId" in keyValuePair: # only check building id when originally the Firebird DB contains building
                    outNodeId = keyValuePair.split(':')[1].replace('"', '').lstrip(' ')
                    try: self.assertTrue(outNodeId in item for item in self.nodeIdListOutput['FloorIdList'])
                    except AssertionError: self.verificationErrors.append("outNodeId for Building {0} does not exist in Firebird DB".format(outNodeId))

        # OrganizationalAreaTemplate TEMPLATES OrganizationalArea
        OrganizationalAreaTemplateTEMPLATESOrganizationalAreaList = []
        for relation in self.relationsListOutput:
            if "\"relationType\": \"Templates\"" and "\"inNodeType\": \"OrganizationalArea\"" and "\"outNodeType\": \"OrganizationalAreaTemplate\"" in relation:
                OrganizationalAreaTemplateTEMPLATESOrganizationalAreaList.append(relation)
        OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCountAPI = len(OrganizationalAreaTemplateTEMPLATESOrganizationalAreaList)
        try: self.assertEqual(OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCountAPI, self.relationTypeCountOutput['OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCount'])
        except AssertionError: self.verificationErrors.append("OrganizationalAreaTemplate TEMPLATES OrganizationalArea count is incorrect! Expected {0}, but API returned {1}".format(self.relationTypeCountOutput['OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCount'], OrganizationalAreaTemplateTEMPLATESOrganizationalAreaCountAPI))

        for item in OrganizationalAreaTemplateTEMPLATESOrganizationalAreaList:
            keyValuePairList = item.split(',')
            for keyValuePair in keyValuePairList:
                if "inNodeId" in keyValuePair:
                    inNodeId = keyValuePair.split(':')[1].replace('"', '').lstrip(' ')
                    try: self.assertTrue(inNodeId in item for item in self.nodeIdListOutput['OrganizationalAreaIdList'])
                    except AssertionError: self.verificationErrors.append("inNodeId for OrganizationalArea {0} does not exist in Firebird DB".format(inNodeId))
                if "outNodeId" in keyValuePair:
                    outNodeId = keyValuePair.split(':')[1].replace('"', '').lstrip(' ')
                    try: self.assertTrue(outNodeId in item for item in self.nodeIdListOutput['OrganizationalAreaTemplateIdList'])
                    except AssertionError: self.verificationErrors.append("outNodeId for OrganizationalAreaTemplate {0} does not exist in Firebird DB".format(outNodeId))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Invalid number of parameters" \
              "Usage:   relationCompare.py <firebird database> <postgreSQL encSystemId>" \
              "Example: relationCompare.py COBALT_ENGINEERING_OFFICE 9757a197-03e5-40a0-ad89-3d8350948f6a"
        sys.exit(1)
    else: # pass the arguments in from cmd
        relationCompare.encSystemId = sys.argv.pop()
        relationCompare.fireBirdDBName = sys.argv.pop()
    unittest.main()


