using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

namespace AzureFunctionDb2IoTHub
{
    class Program
    {

        static DeviceClient deviceClient;
        static string iotHubUri = "iotHubTest023.azure-devices.net";
        static string deviceKey = "W1tTMbI1mbaOrBH2QQrCqv4wVhjLmdPLzkb0ErYduxw=";
        static string deviceId = "TestDevice01";
        static string connetionString = "Data Source=plastcoatpoc.database.windows.net; Initial Catalog=PlastcoatPoC-SQL;User ID=plascoat;Password=plastc0atPoC";
        static SqlConnection sqlConnection;

        static void Main(string[] args)
        {
            Console.WriteLine("Simulated device\n");
            sqlConnection = new SqlConnection(connetionString);

            deviceClient = DeviceClient.Create(
                iotHubUri, new DeviceAuthenticationWithRegistrySymmetricKey(deviceId, deviceKey), 
                TransportType.Mqtt);

            DataSet dsTelemetry = ConnectionSQLServer();

            SendDeviceToCloudMessagesAsync(dsTelemetry);
            UpdateStatusDB(dsTelemetry);
            Console.ReadKey();
            
        }
        private static DataSet ConnectionSQLServer()
        {
            
            try
            {
                //Open SQL Server connection
                sqlConnection.Open();
                //Retrive data
                SqlDataAdapter custAdapter = new SqlDataAdapter(
                //"SELECT TOP (2) * FROM Telemetry2 WHERE IoThubSync = 0", sqlConnection);
                "SELECT * FROM Telemetry2 WHERE IoThubSync = 0", sqlConnection);

                DataSet deviceTelemetryDS = new DataSet();
                custAdapter.Fill(deviceTelemetryDS);

                //Close SQL Server connectio
                sqlConnection.Close();
                return deviceTelemetryDS;
            }
            catch (Exception ex)
            {
                if (sqlConnection.State == ConnectionState.Open)
                    sqlConnection.Close();
                return null;
            }
        }
        private static async void SendDeviceToCloudMessagesAsync(DataSet dsTelemetry)
        {
            if (dsTelemetry != null)
            {

                foreach (DataTable thisTable in dsTelemetry.Tables)
                {
                    // For each row, print the values of each column.
                    foreach (DataRow row in thisTable.Rows)
                    {
                        
                            string MACHINE_ID = string.Empty;
                            string TOOL_NAME = string.Empty;
                            int MOULD_CYCLE_ID = 0;                            
                            int UniqueID = 0;

                            MACHINE_ID = row[0].ToString();
                            TOOL_NAME = row[1].ToString();
                            MOULD_CYCLE_ID = int.Parse( row[2].ToString() );
                            UniqueID = int.Parse(row[4].ToString());

                        Console.WriteLine(string.Format("Values MACHINE_ID:{0} TOOL_NAME:{1}  UniqueID:{2}", MACHINE_ID, TOOL_NAME, UniqueID));
                   
                        
                        var telemetryDataPoint = new
                        {
                            Machine_ID = MACHINE_ID,
                            Tool_NAME = TOOL_NAME,
                            Mould_CYCLE_ID = MOULD_CYCLE_ID
                        };

                        var messageString = JsonConvert.SerializeObject(telemetryDataPoint);
                        var message = new Message(Encoding.ASCII.GetBytes(messageString));
                        await deviceClient.SendEventAsync(message);

                        




                    }
                }
            }
        }

        private static void UpdateStatusDB(DataSet dsTelemetry)
        {
            
            try
            {

                if (dsTelemetry != null)
                {

                    foreach (DataTable thisTable in dsTelemetry.Tables)
                    {
                        // For each row, print the values of each column.
                        foreach (DataRow row in thisTable.Rows)
                        {

                            string MACHINE_ID = string.Empty;
                            string TOOL_NAME = string.Empty;
                            int MOULD_CYCLE_ID = 0;
                            int UniqueID = 0;

                            MACHINE_ID = row[0].ToString();
                            TOOL_NAME = row[1].ToString();
                            MOULD_CYCLE_ID = int.Parse(row[2].ToString());
                            UniqueID = int.Parse(row[4].ToString());

                            string sQuery = string.Format("UPDATE Telemetry2 SET IoThubSync=1 WHERE UniqueID={0} ", UniqueID);
                            using (SqlCommand cmd = new SqlCommand(sQuery, sqlConnection))
                            {
                                // There're three command types: StoredProcedure, Text, TableDirect. The TableDirect
                                // type is only for OLE DB.
                                sqlConnection.Open();
                                cmd.ExecuteNonQuery();
                                sqlConnection.Close();
                            }
                        }
                    }
                }

                                 
            }
            catch (Exception ex)
            {
                if (sqlConnection.State == ConnectionState.Open)
                    sqlConnection.Close();                
            }
        }
    }
}
