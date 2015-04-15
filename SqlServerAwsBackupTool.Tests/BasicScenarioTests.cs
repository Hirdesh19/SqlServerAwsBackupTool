using IniParser;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.IO;
using System.Linq;
using IniParser.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerAwsBackupTool.Tests
{
    [TestFixture]
    public class BasicScenarioTests
    {
        private string tmpFolder = @"C:\temp\";
        private string defaultConfigIni = @"C:\SqlServerAwsBackupTool\config.ini";

        [TestFixtureSetUp]
        public void SetupTests ()
        {
            if (!Directory.Exists(tmpFolder))
            {
                Directory.CreateDirectory(tmpFolder);
            }
        }

        [TestCase]
        public void BasicScenario1()
        {
            var dbName = "basic_scenario_one";
            var backupFile = "";
            var testConfigFileName = tmpFolder + "BasicScenario1.ini";

            var parser = new FileIniDataParser();
            var config = parser.ReadFile(defaultConfigIni);

            config["sqlserver"].SetKeyData(new KeyData("database") { Value = dbName });

            parser.WriteFile(testConfigFileName, config);

            var conn = new SqlConnection(string.Format("Server={0};Database=master;Trusted_Connection=True;", config["sqlserver"]["server"]));

            conn.Open();

            try
            {
                // Create the database and insert some data
                conn.Execute("create database " + dbName);
                conn.ChangeDatabase(dbName);
                conn.Execute("create table test1 (test1 varchar(50))");
                conn.Execute("insert into test1 values ('data')");

                // Backup the database
                var backupManager = new BackupManager();
                var backupResult = backupManager.Backup(new string[1] { testConfigFileName });

                Assert.AreEqual(0, backupResult.ReturnCode);
                Assert.IsFalse(File.Exists(backupFile));

                // Drop the database
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                // Restore the backup from S3
                backupFile = Path.Combine(tmpFolder, backupResult.BackupName);
                var awsProfile = "sql_server_backup";

                Amazon.Util.ProfileManager.RegisterProfile(awsProfile, config["aws"]["access_key"], config["aws"]["secret_key"]);

                var creds = Amazon.Util.ProfileManager.GetAWSCredentials(awsProfile);

                var awsClient = new AmazonS3Client(creds, Amazon.RegionEndpoint.USEast1);

                var getRequest = new GetObjectRequest()
                {
                    BucketName = config["aws"]["bucket"],
                    Key = backupResult.BackupName
                };

                using (var getResponse = awsClient.GetObject(getRequest))
                {
                    if (!File.Exists(backupFile))
                    {
                        getResponse.WriteResponseStreamToFile(backupFile);
                    }
                }

                ServerConnection smoConn = new ServerConnection(config["sqlserver"]["server"]);
                Server server = new Server(smoConn);
                Restore destination = new Restore();
                destination.Action = RestoreActionType.Database;
                destination.Database = config["sqlserver"]["database"];
                BackupDeviceItem source = new BackupDeviceItem(backupFile, DeviceType.File);
                destination.Devices.Add(source);
                destination.ReplaceDatabase = true;
                destination.SqlRestore(server);

                conn.ChangeDatabase(dbName);
                var result = conn.Query("select test1 from test1 where test1 = 'data'");

                Assert.IsTrue(result.Count() == 1);

                var listReq = new ListObjectsRequest()
                {
                    BucketName = config["aws"]["bucket"]
                };

                var objects = awsClient.ListObjects(listReq);

                foreach (var obj in objects.S3Objects)
                {
                    if (obj.Key.IndexOf(dbName) != -1)
                    {
                        var delReq = new DeleteObjectRequest() { BucketName = config["aws"]["bucket"], Key = obj.Key };

                        awsClient.DeleteObject(delReq);
                    }
                }
            }
            finally
            {
                conn.ChangeDatabase("master");
                conn.Execute("drop database " + dbName);

                if (File.Exists(backupFile))
                {
                    File.Delete(backupFile);
                }
            }
        }
    }
}
