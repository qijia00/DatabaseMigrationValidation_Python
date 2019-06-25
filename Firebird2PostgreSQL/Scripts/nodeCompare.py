import os
import fdb # imports the Firebird connector python module in your program so you can use the functions of this module to communicate with the Firebird database. you need to pip install fdb.
import requests
import json
import unittest
import sys

# variables you need to pass in from command line when test different Firbird DB conversions
# fireBirdDBName = 'COBALT_ENGINEERING_OFFICE' # copy the firebird DB to your folder, outside the "Scripts" folder
def query_fireBirdDB(fireBirdDBName, nodeTypeCount):

    # if you encounter an error "The location of Firebird Client Library could not be determined", then google "firebird client" to install will fix it.
    dir_name = '../'
    suffix = '.GDB'
    databasePath = os.path.join(dir_name, fireBirdDBName + suffix) # ../XYZ.GDB
    fb = fdb.connect(database = databasePath, user='sysdba', password='masterkey')

    schema = fdb.schema.Schema()
    schemaBindFb = schema.bind(fb)
    cursor = fb.cursor()

    # Use "BIM Objects.xlsx" from the URL below (passwrod protected, in comment area) to find which table in Firebird DB contains the node
    # https://osramencelium.atlassian.net/wiki/spaces/EVOUI/pages/151748778/PostgreSQL+db
    # Remote desk in any SSU listed in the URL below (password protected) and use the "DB Editor" software to have a visualized view of the table columns
    # https://osramencelium.atlassian.net/wiki/spaces/AR/pages/9215005/Deployment
    # Use ObjectType file from the URL below (password protected) to filter the node that you want to count from the table
    # http://bitbucket:7990/projects/ECU/repos/ecu_properties/raw/generatedcode/polaris/ecs_Type_ECUProperties.pas?at=refs%2Fheads%2Fdev
    for table in schema.tables:
        assert isinstance(table, fdb.schema.Table)
        #print table.name, [c.name for c in table.columns] #something like TBL_ADDRESS_OFFSET ['FREE_OFFSET', 'ISCOUNT']

        #  to fetch data use cursor.execute() to run a query.
        sql_select_Query  = "select * from {0}".format(table.name)
        cursor.execute(sql_select_Query)
        data = cursor.fetchall() # cursor.fetchall() to fetch all rows
        #print(type(data)) # <type 'list'>

        if table.name == 'TBL_PLAN':
            #print("TBL_PLAN:")
            #for column in table.columns:
                #print(column.name)
            for row in data:
                #print(row)
                nodeTypeCount['FloorCount'] += 1 # actually floor plan instead of floor, but according to BIM Objects.xlsx, this is the place to get floor.

        if table.name == 'TBL_ECU':
            for row in data:
                 nodeTypeCount['ManagerCount'] += 1  # SSU and ECU are both considered as Manager node.

        if table.name == 'TBL_ZONE_TEMPLATE':
            for row in data:
                # objectType 254 is virtualZone, objectType 2 is Zone
                if row[2] == 254 or row[2] == 2:
                    nodeTypeCount['OrganizationalAreaTemplateCount'] += 1
                # objectType 246 is Building.
                elif row[2] == 246:
                    nodeTypeCount['BuildingTemplateCount'] += 1
                # objectType 245 is Floor.
                elif row[2] == 245:
                    nodeTypeCount['PolarisVirtualFloorTemplateCount'] += 1

        if table.name == 'TBL_BUILDING_ZONE':
            for row in data:
                if row[4] == 254 or row[4] == 2:
                    nodeTypeCount['OrganizationalAreaCount'] += 1
                elif row[4] == 246:
                    nodeTypeCount['BuildingCount'] += 1
                elif row[4] == 245:
                    nodeTypeCount['PolarisVirtualFloorCount'] += 1
            # for any Firebird DB with NO Building a new Building node will be inserted to the site when it is uploaded to the BIM
            if nodeTypeCount['BuildingCount'] == 0:
                nodeTypeCount['BuildingCount'] = 1

    return nodeTypeCount

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

    nodes = requests.get(http + env + urlConfig + '/v1/nodes?encSystemId=' + encSystemId + '&Pageindex=0&PageSize=999999', auth=(username, password))
    nodesStatusCode = nodes.status_code
    assert nodesStatusCode == 200, "non 200 status code returned for {}{}{}/v1/nodes?encSystemId={}&Pageindex=0&PageSize=999999".format(http, env, urlConfig, encSystem)
    nodesString = nodes.content # all nodes belong to the encSystem (i.e. a site)

    return nodesString

# compare nodes from Firebird DB with API returns (API queries cloud PostgreSQL DB)
# implement non-fatal assertions (i.e., if failed in the middle next test will continue to run) by capturing the assertion exception and store the exceptions in a list
class nodeCompare(unittest.TestCase):
    fireBirdDBName = '' # copy the firebird DB to your folder, outside the "Scripts" folder, will pass in from cmd.
    encSystemId = '' # guid of encSystem node in PostgreSQL DB, will pass in from cmd.

    def setUp(self):
        # Below is a dictionary of supported node types and their count, you can get this from the URL below (password protected):
        # https://qa-bimapi.5ea7a1d2a99a4699b36b.eastus.aksapp.io/config//v1/schemas/nodetypes
        # The hierarchy and relations between nodes please refer to the architecture diagram (password protected):
        # https://osramencelium.atlassian.net/wiki/spaces/EVOUI/pages/108167181/Node+Types+Relationship+Types
        self.nodeTypeCount = {
            'AvInterfaceHandlerCount': 0,
            'BacnetConfigurationCount': 0,
            'BacnetGatewayCount': 0,
            'BallastCount': 0,
            'BuildingCount': 0,
            'BuildingTemplateCount': 0,
            'ButtonCount': 0,
            'CalendarCount': 0,
            'CentralBatteryCount': 0,
            'CentralBatteryTemplateCount': 0,
            'CommandOptimizerCount': 0,
            'ContactClosureTriggerCount': 0,
            'EmergencyGroupDispatcherCount': 0,
            'EmergencyInverterCount': 0,
            'EmergencyLuminaireCount': 0,
            'EmergencyLuminaireTemplateCount': 0,
            'EmergencyTestGroupCount': 0,
            'EmergencyTestManagerCount': 0,
            # 'EncSystemCount': 0, # Each Firebird DB is migrated to a single EncSystem in PostgreSQL as the parent node of all the other nodes, not included in this validation.
            'EventDispatcherCount': 0,
            'EventListCount': 0,
            'EventLoggerCount': 0,
            'FacadeCount': 0,
            'FireAlarmCount': 0,
            'FireAlarmTemplateCount': 0,
            'FloorCount': 0,
            'GenericDeviceEndpointCount': 0,
            'KeypadCount': 0,
            'KeypadTemplateCount': 0,
            'LoadSheddingDispatcherCount': 0,
            'LoadSheddingGroupCount': 0,
            'LoadSheddingPrioritizerCount': 0,
            'LoadSheddingRequestorCount': 0,
            'LuminaireCount': 0,
            'LuminaireTemplateCount': 0,
            'ManagerCount': 0,
            'OccupancySensorCount': 0,
            'OccupancySensorTemplateCount': 0,
            # 'OrganizationCount': 0, # Firebird DB does not have Organization node, which is the only node that is higher than EncSystem, not included in this validation.
            'OrganizationalAreaCount': 0,
            'OrganizationalAreaTemplateCount': 0,
            'PartitionWallCount': 0,
            'PartitionWallTemplateCount': 0,
            'PersonalControlDeviceCount': 0,
            'PhotoSensorCount': 0,
            'PhotoSensorTemplateCount': 0,
            'PhysicalLoadMeterCount': 0,
            'PlugLoadCount': 0,
            'PlugLoadTemplateCount': 0,
            'PolarisVirtualFloorCount': 0,
            'PolarisVirtualFloorTemplateCount': 0,
            'RepeaterCount': 0,
            'ScheduleCount': 0,
            'ScheduleDispatcherCount': 0,
            'ScheduleEventCount': 0,
            'ScheduleExceptionOccurenceCount': 0,
            'ScheduleRecurrenceRuleCount': 0,
            'ScheduleTriggerCount': 0,
            'ShadeCount': 0,
            'ShadeTemplateCount': 0,
            'SolarCalculatorCount': 0,
            'StatusTriggerCount': 0,
            'TunableWhiteLuminaireCount': 0,
            'TunableWhiteLuminaireTemplateCount': 0,
            'WalcHiddenTemplateCount': 0,
            'WalcLuminaireCount': 0,
            'WalcLuminaireTemplateCount': 0,
            'WslcLuminaireCount': 0,
            'WslcLuminaireTemplateCount': 0,
            'ZigbeeNetworkInfoCount': 0
        }
        self.nodeTypeCountOutput = query_fireBirdDB(self.fireBirdDBName, self.nodeTypeCount)
        self.nodesStringOutput = query_postgreSQLDB(self.encSystemId)
        self.verificationErrors = []

    def tearDown(self):
        self.assertEqual([], self.verificationErrors)

    def test_init(self):
        FloorCountAPI= self.nodesStringOutput.count("\"nodeType\": \"Floor\"")
        try: self.assertEqual(FloorCountAPI, self.nodeTypeCountOutput['FloorCount'])
        except AssertionError: self.verificationErrors.append("Floor count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['FloorCount'], FloorCountAPI))

        ManagerCountAPI= self.nodesStringOutput.count("\"nodeType\": \"Manager\"")
        try: self.assertEqual(ManagerCountAPI, self.nodeTypeCountOutput['ManagerCount'])
        except AssertionError: self.verificationErrors.append("Manager count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['ManagerCount'], ManagerCountAPI))

        OrganizationalAreaTemplateCountAPI= self.nodesStringOutput.count("\"nodeType\": \"OrganizationalAreaTemplate\"")
        try: self.assertEqual(OrganizationalAreaTemplateCountAPI, self.nodeTypeCountOutput['OrganizationalAreaTemplateCount'])
        except AssertionError: self.verificationErrors.append("OrganizationalAreaTemplate count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['OrganizationalAreaTemplateCount'], OrganizationalAreaTemplateCountAPI))

        BuildingTemplateCountAPI= self.nodesStringOutput.count("\"nodeType\": \"BuildingTemplate\"")
        try: self.assertEqual(BuildingTemplateCountAPI, self.nodeTypeCountOutput['BuildingTemplateCount'])
        except AssertionError: self.verificationErrors.append("BuildingTemplate count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['BuildingTemplateCount'], BuildingTemplateCountAPI))

        PolarisVirtualFloorTemplateCountAPI= self.nodesStringOutput.count("\"nodeType\": \"PolarisVirtualFloorTemplate\"")
        try: self.assertEqual(PolarisVirtualFloorTemplateCountAPI, self.nodeTypeCountOutput['PolarisVirtualFloorTemplateCount'])
        except AssertionError: self.verificationErrors.append("PolarisVirtualFloorTemplate count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['PolarisVirtualFloorTemplateCount'], PolarisVirtualFloorTemplateCountAPI))

        OrganizationalAreaCountAPI= self.nodesStringOutput.count("\"nodeType\": \"OrganizationalArea\"")
        try: self.assertEqual(OrganizationalAreaCountAPI, self.nodeTypeCountOutput['OrganizationalAreaCount'])
        except AssertionError: self.verificationErrors.append("OrganizationalArea count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['OrganizationalAreaCount'], OrganizationalAreaCountAPI))

        BuildingCountAPI= self.nodesStringOutput.count("\"nodeType\": \"Building\"")
        try: self.assertEqual(BuildingCountAPI, self.nodeTypeCountOutput['BuildingCount'])
        except AssertionError: self.verificationErrors.append("Building count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['BuildingCount'], BuildingCountAPI))

        PolarisVirtualFloorCountAPI= self.nodesStringOutput.count("\"nodeType\": \"PolarisVirtualFloor\"")
        try: self.assertEqual(PolarisVirtualFloorCountAPI, self.nodeTypeCountOutput['PolarisVirtualFloorCount'])
        except AssertionError: self.verificationErrors.append("PolarisVirtualFloor count is incorrect! Expected {0}, but API returned {1}".format(self.nodeTypeCountOutput['PolarisVirtualFloorCount'], PolarisVirtualFloorCountAPI))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Invalid number of parameters" \
              "Usage:   nodeCompare.py <firebird database> <postgreSQL encSystemId>" \
              "Example: nodeCompare.py COBALT_ENGINEERING_OFFICE 9757a197-03e5-40a0-ad89-3d8350948f6a"
        sys.exit(1)
    else: # pass the arguments in from cmd
        nodeCompare.encSystemId = sys.argv.pop()
        nodeCompare.fireBirdDBName = sys.argv.pop()
    unittest.main()


